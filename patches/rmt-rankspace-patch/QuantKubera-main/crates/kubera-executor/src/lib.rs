//! # Order Execution Engine Module
//!
//! Unified execution layer supporting both simulation and live trading.
//!
//! ## Description
//! Implements order execution through a unified trait:
//! - **SimulatedExchange**: For backtesting with configurable slippage/commissions
//! - **ZerodhaLiveExchange**: For live trading via Kite Connect API
//!
//! ## Order Flow
//! ```text
//! Strategy → OrderEvent → Exchange (Simulated/Live) → Fill → PositionUpdate
//! ```
//!
//! ## Modes
//! - **Backtest**: Deterministic fills, configurable slippage
//! - **Paper**: Simulated fills, real market data
//! - **Live**: Real order placement via Zerodha Kite API
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Kite Connect API: <https://kite.trade/docs/connect/v3/>

use kubera_models::{MarketEvent, MarketPayload, OrderEvent, OrderPayload, OrderStatus, Side};
use kubera_core::EventBus;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tracing::{info, warn, error};
use chrono::{Utc, DateTime, Duration};
use uuid::Uuid;
use std::collections::HashMap;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use async_trait::async_trait;

// ============================================================================
// UNIFIED EXCHANGE TRAIT
// ============================================================================

/// Unified execution interface for both simulated and live trading.
///
/// All exchange implementations must provide order handling and market data processing.
#[async_trait]
pub trait Exchange: Send + Sync {
    /// Handle an incoming order (new, cancel, modify)
    async fn handle_order(&mut self, event: OrderEvent) -> anyhow::Result<()>;

    /// Process market data for order matching (simulated) or position updates (live)
    async fn on_market_data(&mut self, event: MarketEvent) -> anyhow::Result<()>;

    /// Get the exchange name for logging
    fn name(&self) -> &'static str;
}

/// Execution mode selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Simulated exchange for backtesting
    Backtest,
    /// Simulated fills with real market data
    Paper,
    /// Live order placement via broker API
    Live,
}

// =============================================================================
// Phase 0: C++ execution simulator (L2 market-order costing)
// Enabled via: --features cpp_exec
// =============================================================================

#[cfg(feature = "cpp_exec")]
pub mod cpp_exec {
    use kubera_ffi::{KuberaL2Book, MarketOrder, ExecConfig, Side, Fill, DEPTH_MAX, simulate_market_fill};

    /// Convenience helper to create a book snapshot with `depth` levels.
    /// Prices and sizes are **integer** ticks/qty.
    pub fn make_l2_book(
        depth: i32,
        bid_px: &[i64],
        bid_sz: &[i64],
        ask_px: &[i64],
        ask_sz: &[i64],
    ) -> KuberaL2Book {
        let mut book = KuberaL2Book {
            depth,
            bid_px: [0; DEPTH_MAX],
            bid_sz: [0; DEPTH_MAX],
            ask_px: [0; DEPTH_MAX],
            ask_sz: [0; DEPTH_MAX],
        };
        for i in 0..(DEPTH_MAX.min(bid_px.len())) { book.bid_px[i] = bid_px[i]; }
        for i in 0..(DEPTH_MAX.min(bid_sz.len())) { book.bid_sz[i] = bid_sz[i]; }
        for i in 0..(DEPTH_MAX.min(ask_px.len())) { book.ask_px[i] = ask_px[i]; }
        for i in 0..(DEPTH_MAX.min(ask_sz.len())) { book.ask_sz[i] = ask_sz[i]; }
        book
    }

    /// Simulate a market order fill using the C++ engine.
    ///
    /// This is **causal**: it consumes only the current L2 snapshot.
    pub fn simulate_l2_market_order(
        book: &KuberaL2Book,
        ts_ns: i64,
        symbol_id: i32,
        side: Side,
        qty: i64,
        taker_fee_bps: i64,
        latency_ns: i64,
        allow_partial: bool,
    ) -> Fill {
        let ord = MarketOrder { ts_ns, symbol_id, side, qty, max_slippage_bps: None };
        let cfg = ExecConfig { taker_fee_bps, latency_ns, allow_partial };
        simulate_market_fill(book, &ord, &cfg)
    }
}

/// Internal position state for a specific asset.
struct PositionState {
    /// Current quantity (positive for long, negative for short).
    quantity: f64,
    /// Volume-weighted average entry price (VWAP).
    avg_price: f64,
}

/// Internal pending order state.
struct OrderState {
    /// Unique order identifier.
    id: Uuid,
    /// Link to the original strategy intent.
    intent_id: Option<Uuid>,
    /// Asset symbol (e.g., "BTCUSDT").
    symbol: String,
    /// Side of the transaction (Buy/Sell).
    side: Side,
    /// Optional limit price; market order if None.
    price: Option<f64>,
    /// Target quantity to execute.
    quantity: f64,
}

/// Supported commission models for different exchanges and asset classes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CommissionModel {
    /// No commissions (default).
    None,
    /// Fixed basis points of turnover.
    Linear(f64),
    /// Zerodha Indian F&O (Futures) model as of Dec 2025.
    ZerodhaFnO,
}

/// High-fidelity simulated execution venue.
///
/// # Architecture
/// - **Order Matching**: Compares pending orders against incoming L1 market data.
/// - **Slippage Simulation**: Adds probabilistic noise to execution prices.
/// - **Commission Modeling**: Calculates realistic fees as of late 2025.
/// - **State Management**: Tracks per-symbol positions and pending limit orders.
pub struct SimulatedExchange {
    /// Broadcast bus for publishing execution reports (Fills).
    bus: Arc<EventBus>,
    /// Queue of orders awaiting market triggers, partitioned by symbol.
    pending_orders: HashMap<String, Vec<OrderState>>,
    /// Portfolio state tracking current holdings.
    positions: HashMap<String, PositionState>,
    /// Constant slippage parameter in basis points.
    slippage_bps: f64,
    /// Commission model to use for fills.
    commission_model: CommissionModel,
    /// Seeded PRNG for deterministic slippage noise.
    rng: Pcg64,
    /// Internal simulation clock synchronized with market data.
    current_time: chrono::DateTime<chrono::Utc>,
}

impl SimulatedExchange {
    /// Initializes a new simulated exchange.
    ///
    /// # Parameters
    /// * `bus` - Shared event distribution system.
    /// * `slippage_bps` - Expected price impact/cost in basis points.
    /// * `commission_model` - Fees model to apply.
    /// * `seed` - Optional seed for the PRNG to enable reproducible runs.
    pub fn new(bus: Arc<EventBus>, slippage_bps: f64, commission_model: CommissionModel, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => Pcg64::seed_from_u64(s),
            None => Pcg64::from_entropy(),
        };

        Self {
            bus,
            pending_orders: HashMap::new(),
            positions: HashMap::new(),
            slippage_bps,
            commission_model,
            rng,
            current_time: Utc::now(),
        }
    }

    /// Calculates commission for a trade based on the configured model.
    fn calculate_commission(&self, side: Side, quantity: f64, price: f64) -> f64 {
        let turnover = quantity * price;
        match self.commission_model {
            CommissionModel::None => 0.0,
            CommissionModel::Linear(bps) => turnover * (bps / 10000.0),
            CommissionModel::ZerodhaFnO => {
                // Brokerage: Lower of 0.03% or ₹20 per executed order
                let brokerage = ( turnover * 0.0003).min(20.0);
                
                // STT (Sell side only for Futures): 0.02%
                let stt = if side == Side::Sell { turnover * 0.0002 } else { 0.0 };
                
                // Transaction Charge: 0.00173% (NSE)
                let trans_charge = turnover * 0.0000173;
                
                // SEBI: 0.00005%
                let sebi = turnover * 0.0000005;
                
                // Stamp Duty (Buy side only): 0.002%
                let stamp = if side == Side::Buy { turnover * 0.00002 } else { 0.0 };
                
                // GST: 18% on (Brokerage + Trans + SEBI)
                let gst = (brokerage + trans_charge + sebi) * 0.18;
                
                brokerage + stt + trans_charge + sebi + stamp + gst
            }
        }
    }

    /// Generates a UUID derived from the internal seeded RNG.
    pub fn next_deterministic_id(&mut self) -> Uuid {
        let bytes: [u8; 16] = self.rng.r#gen();
        Uuid::from_bytes(bytes)
    }

    /// Explicitly updates the internal simulation clock.
    pub fn set_time(&mut self, time: chrono::DateTime<chrono::Utc>) {
        self.current_time = time;
    }

    /// Ingests a new order command or cancellation request.
    ///
    /// # Parameters
    /// * `event` - The order event containing the intent (New, Cancel, Modify).
    pub async fn handle_order(&mut self, event: OrderEvent) -> anyhow::Result<()> {
        match event.payload {
            OrderPayload::New { symbol, side, price, quantity, .. } => {
                info!("Accepted New Order: {} {} @ {:?}", side, symbol, price);
                let order = OrderState { 
                    id: event.order_id, 
                    intent_id: event.intent_id,
                    symbol: symbol.clone(), 
                    side, 
                    price, 
                    quantity 
                };
                self.pending_orders.entry(symbol).or_default().push(order);
                
                self.bus.publish_order_update(OrderEvent {
                    order_id: event.order_id,
                    intent_id: event.intent_id,
                    timestamp: self.current_time,
                    symbol: event.symbol.clone(),
                    side: event.side,
                    payload: OrderPayload::Update {
                        status: OrderStatus::Accepted,
                        filled_quantity: 0.0,
                        avg_price: 0.0,
                        commission: 0.0,
                    },
                })?;
            },
            OrderPayload::Cancel => {
                let mut cancelled = false;
                for orders in self.pending_orders.values_mut() {
                    if let Some(pos) = orders.iter().position(|o| o.id == event.order_id) {
                        orders.remove(pos);
                        cancelled = true;
                        break;
                    }
                }
                
                if cancelled {
                    info!("Cancelled Order: {}", event.order_id);
                    self.bus.publish_order_update(OrderEvent {
                        order_id: event.order_id,
                        intent_id: event.intent_id,
                        timestamp: self.current_time,
                        symbol: event.symbol.clone(),
                        side: event.side,
                        payload: OrderPayload::Update {
                            status: OrderStatus::Cancelled,
                            filled_quantity: 0.0,
                            avg_price: 0.0,
                            commission: 0.0,
                        },
                    })?;
                } else {
                    warn!("Cancel failed - order {} not found", event.order_id);
                }
            },
            OrderPayload::Modify { new_price, new_quantity } => {
                let mut modified = false;
                for orders in self.pending_orders.values_mut() {
                    if let Some(order) = orders.iter_mut().find(|o| o.id == event.order_id) {
                        if let Some(p) = new_price {
                            order.price = Some(p);
                        }
                        if let Some(q) = new_quantity {
                            order.quantity = q;
                        }
                        modified = true;
                        break;
                    }
                }
                
                if modified {
                    info!("Modified Order: {}", event.order_id);
                    self.bus.publish_order_update(OrderEvent {
                        order_id: event.order_id,
                        intent_id: event.intent_id,
                        timestamp: self.current_time,
                        symbol: event.symbol.clone(),
                        side: event.side,
                        payload: OrderPayload::Update {
                            status: OrderStatus::Accepted,
                            filled_quantity: 0.0,
                            avg_price: 0.0,
                            commission: 0.0,
                        },
                    })?;
                } else {
                    warn!("Modify failed - order {} not found", event.order_id);
                }
            },
            _ => {}
        }
        Ok(())
    }

    /// Evaluates market data and triggers executions for pending orders.
    ///
    /// # Parameters
    /// * `event` - Incoming market tick or bar.
    pub async fn on_market_data(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        self.current_time = event.exchange_time;
        // Handle both Tick and Trade events for order matching
        let price = match &event.payload {
            MarketPayload::Tick { price, .. } => Some(*price),
            MarketPayload::Trade { price, .. } => Some(*price),
            _ => None,
        };
        if let Some(price) = price {
            let symbol = event.symbol.clone();
            
            // 1. Collect potential matches to avoid borrow checker conflicts
            let mut fills = Vec::new();
            
            if let Some(orders) = self.pending_orders.get_mut(&symbol) {
                let mut remaining_orders = Vec::new();
                for order in orders.drain(..) {
                    let can_fill = match order.price {
                        None => true,
                        Some(limit_price) => match order.side {
                            Side::Buy => price <= limit_price,
                            Side::Sell => price >= limit_price,
                        },
                    };

                    if can_fill {
                        let random_factor = self.rng.gen_range(0.9..1.1);
                        let effective_slippage = self.slippage_bps * random_factor;
                        
                        let slip = price * (effective_slippage / 10000.0);
                        let fill_price = match order.side {
                            Side::Buy => price + slip,
                            Side::Sell => price - slip,
                        };
                        
                        fills.push((order, fill_price));
                    } else {
                        remaining_orders.push(order);
                    }
                }
                *orders = remaining_orders;
            }
            
            // 2. Process fills outside the borrow
            for (order, fill_price) in fills {
                info!("Simulated Fill: {} {} @ {}", order.side, order.symbol, fill_price);
                
                let pos = self.positions.entry(order.symbol.clone()).or_insert(PositionState { quantity: 0.0, avg_price: 0.0 });
                let side_mult = if order.side == Side::Buy { 1.0 } else { -1.0 };
                
                if (pos.quantity > 0.0 && order.side == Side::Buy) || (pos.quantity < 0.0 && order.side == Side::Sell) {
                    pos.avg_price = ((pos.quantity * pos.avg_price) + (order.quantity * fill_price)) / (pos.quantity + order.quantity);
                } else if pos.quantity == 0.0 {
                    pos.avg_price = fill_price;
                }
                
                pos.quantity += side_mult * order.quantity;
                info!("Position Update: {} quantity: {}", order.symbol, pos.quantity);

                let commission = self.calculate_commission(order.side, order.quantity, fill_price);

                self.bus.publish_order_update(OrderEvent {
                    order_id: order.id,
                    intent_id: order.intent_id,
                    timestamp: self.current_time,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    payload: OrderPayload::Update {
                        status: OrderStatus::Filled,
                        filled_quantity: order.quantity,
                        avg_price: fill_price,
                        commission,
                    },
                })?;

                self.bus.publish_fill(kubera_models::FillEvent {
                    timestamp: self.current_time,
                    order_id: order.id,
                    intent_id: order.intent_id,
                    fill_id: Uuid::new_v4().to_string(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    price: fill_price,
                    quantity: order.quantity,
                    commission,
                    commission_asset: "INR".to_string(),
                    venue: "SimulatedExchange".to_string(),
                    is_final: true,
                }).await?;
            }
        }
        Ok(())
    }
}

// Implement Exchange trait for SimulatedExchange
#[async_trait]
impl Exchange for SimulatedExchange {
    async fn handle_order(&mut self, event: OrderEvent) -> anyhow::Result<()> {
        // Call the existing implementation
        SimulatedExchange::handle_order(self, event).await
    }

    async fn on_market_data(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        // Call the existing implementation
        SimulatedExchange::on_market_data(self, event).await
    }

    fn name(&self) -> &'static str {
        "SimulatedExchange"
    }
}

// ============================================================================
// ZERODHA LIVE EXCHANGE
// ============================================================================

/// Base URL for Kite Connect REST API
const KITE_API_URL: &str = "https://api.kite.trade";

/// Zerodha live execution client for production trading.
///
/// # Safety
/// This client places REAL orders with REAL money. Use with extreme caution.
/// Always test thoroughly in paper mode before going live.
///
/// # Features
/// - Market and limit order support
/// - Automatic commission calculation (ZerodhaFnO model)
/// - Position tracking for risk management
/// - Order status polling for fill confirmation
pub struct ZerodhaLiveExchange {
    /// Event bus for publishing execution reports
    bus: Arc<EventBus>,
    /// Kite API key
    api_key: String,
    /// Session access token
    access_token: String,
    /// HTTP client for API calls
    client: reqwest::Client,
    /// Pending orders awaiting fill confirmation
    pending_orders: HashMap<String, PendingOrder>,    /// Order-status polling interval (milliseconds)
    poll_interval_ms: u64,
    /// Maximum time to wait for a terminal status after submitting an order (milliseconds)
    poll_timeout_ms: u64,

    /// Commission model
    commission_model: CommissionModel,
}

/// Pending order tracking for live exchange
#[allow(dead_code)]
struct PendingOrder {
    /// Internal order ID
    order_id: Uuid,
    /// Intent ID for attribution
    intent_id: Option<Uuid>,
    /// Zerodha order ID
    zerodha_order_id: String,
    /// Symbol
    symbol: String,
    /// Side
    side: Side,
    /// Quantity requested
    quantity: f64,
    /// UTC time when we submitted the order
    submitted_utc: DateTime<Utc>,
    /// Last time we polled the broker for status (UTC)
    last_poll_utc: DateTime<Utc>,
}

/// Zerodha order response
#[derive(Debug, serde::Deserialize)]
struct ZerodhaOrderResponse {
    status: String,
    data: Option<ZerodhaOrderData>,
    message: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ZerodhaOrderData {
    order_id: String,
}

impl ZerodhaLiveExchange {
    /// Create a new Zerodha live exchange client.
    ///
    /// # Parameters
    /// * `bus` - Event bus for publishing fills
    /// * `api_key` - Kite API key from developer console
    /// * `access_token` - Session token from login
    ///
    /// # Safety
    /// This will place REAL orders. Ensure credentials are correct.
    pub fn new(bus: Arc<EventBus>, api_key: String, access_token: String) -> Self {
        info!("[ZERODHA LIVE] Initializing live execution client - REAL ORDERS ENABLED");
        Self {
            bus,
            api_key,
            access_token,
            client: reqwest::Client::new(),
            pending_orders: HashMap::new(),
            commission_model: CommissionModel::ZerodhaFnO,
        }
    }

    /// Place an order via Kite API.
    async fn place_order(
        &self,
        symbol: &str,
        side: Side,
        quantity: f64,
        price: Option<f64>,
    ) -> anyhow::Result<String> {
        // Determine exchange based on symbol type
        let exchange = if symbol.contains("FUT") || symbol.contains("CE") || symbol.contains("PE") {
            "NFO"
        } else {
            "NSE"
        };

        let transaction_type = match side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        };

        let order_type = if price.is_some() { "LIMIT" } else { "MARKET" };

        let url = format!("{}/orders/regular", KITE_API_URL);

        info!(
            "[ZERODHA LIVE] Placing {} {} {} {} @ {:?}",
            transaction_type, quantity as u32, symbol, order_type, price
        );

        let mut form = vec![
            ("tradingsymbol", symbol.to_string()),
            ("exchange", exchange.to_string()),
            ("transaction_type", transaction_type.to_string()),
            ("order_type", order_type.to_string()),
            ("quantity", (quantity as u32).to_string()),
            ("product", "NRML".to_string()),  // F&O normal
            ("validity", "DAY".to_string()),
        ];

        if let Some(p) = price {
            form.push(("price", format!("{:.2}", p)));
        }

        let response = self.client.post(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .form(&form)
            .send()
            .await?;

        let status = response.status();
        let resp: ZerodhaOrderResponse = response.json().await?;

        if resp.status != "success" {
            let msg = resp.message.unwrap_or_else(|| "Unknown error".to_string());
            error!("[ZERODHA LIVE] Order placement FAILED: {}", msg);
            return Err(anyhow::anyhow!("Order failed: {}", msg));
        }

        let order_id = resp.data
            .ok_or_else(|| anyhow::anyhow!("No order ID in response"))?
            .order_id;

        info!("[ZERODHA LIVE] Order placed successfully: {} (HTTP {})", order_id, status);
        Ok(order_id)
    }

    /// Cancel an order via Kite API.
    async fn cancel_order(&self, zerodha_order_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/orders/regular/{}", KITE_API_URL, zerodha_order_id);

        info!("[ZERODHA LIVE] Cancelling order: {}", zerodha_order_id);

        let response = self.client.delete(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?;

        let resp: ZerodhaOrderResponse = response.json().await?;

        if resp.status != "success" {
            let msg = resp.message.unwrap_or_else(|| "Unknown error".to_string());
            return Err(anyhow::anyhow!("Cancel failed: {}", msg));
        }

        info!("[ZERODHA LIVE] Order cancelled: {}", zerodha_order_id);
        Ok(())
    }

    
    /// Fetch the current orders list from Zerodha.
    async fn fetch_orders(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let url = "https://api.kite.trade/orders";
        let resp = self
            .client
            .get(url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?;

        let status = resp.status();
        let body: serde_json::Value = resp.json().await?;
        if !status.is_success() {
            return Err(anyhow::anyhow!("[ZERODHA LIVE] fetch_orders failed: HTTP {} body={}", status, body));
        }

        // Kite typically returns { status: "success", data: [...] }
        let data = body.get("data").cloned().unwrap_or(serde_json::Value::Null);
        let arr = data.as_array().cloned().unwrap_or_default();
        Ok(arr)
    }

    /// Find an order by ID in the current orders list.
    async fn fetch_order_by_id(&self, zerodha_order_id: &str) -> anyhow::Result<Option<serde_json::Value>> {
        let orders = self.fetch_orders().await?;
        for o in orders {
            if o.get("order_id").and_then(|v| v.as_str()) == Some(zerodha_order_id) {
                return Ok(Some(o));
            }
        }
        Ok(None)
    }

    /// Poll an order until it reaches a terminal status or timeout.
    ///
    /// Terminal statuses (best-effort): COMPLETE, REJECTED, CANCELLED.
    async fn poll_order_until_terminal(
        &self,
        zerodha_order_id: &str,
        timeout_ms: u64,
        poll_ms: u64,
    ) -> anyhow::Result<Option<(String, f64, f64, Option<DateTime<Utc>>)>> {
        let started = Utc::now();
        let timeout = Duration::milliseconds(timeout_ms as i64);

        loop {
            if Utc::now() - started > timeout {
                return Ok(None);
            }

            if let Some(o) = self.fetch_order_by_id(zerodha_order_id).await? {
                let status = o.get("status").and_then(|v| v.as_str()).unwrap_or("").to_string();

                // Filled quantity and average price are typically numbers or strings.
                let filled_qty = o.get("filled_quantity")
                    .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok())))
                    .unwrap_or(0.0);

                let avg_price = o.get("average_price")
                    .and_then(|v| v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok())))
                    .unwrap_or(0.0);

                let ts_utc = o.get("exchange_timestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc));

                let status_upper = status.to_uppercase();
                if status_upper == "COMPLETE" || status_upper == "REJECTED" || status_upper == "CANCELLED" {
                    return Ok(Some((status_upper, filled_qty, avg_price, ts_utc)));
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
        }
    }

/// Calculate commission using ZerodhaFnO model
    fn calculate_commission(&self, side: Side, quantity: f64, price: f64) -> f64 {
        let turnover = quantity * price;
        match self.commission_model {
            CommissionModel::ZerodhaFnO => {
                let brokerage = (turnover * 0.0003).min(20.0);
                let stt = if side == Side::Sell { turnover * 0.0002 } else { 0.0 };
                let trans_charge = turnover * 0.0000173;
                let sebi = turnover * 0.0000005;
                let stamp = if side == Side::Buy { turnover * 0.00002 } else { 0.0 };
                let gst = (brokerage + trans_charge + sebi) * 0.18;
                brokerage + stt + trans_charge + sebi + stamp + gst
            }
            CommissionModel::Linear(bps) => turnover * (bps / 10000.0),
            CommissionModel::None => 0.0,
        }
    }
}

#[async_trait]
impl Exchange for ZerodhaLiveExchange {
    async fn handle_order(&mut self, event: OrderEvent) -> anyhow::Result<()> {
        match event.payload {
            OrderPayload::New { symbol, side, price, quantity, .. } => {
                // Place REAL order
                match self.place_order(&symbol, side, quantity, price).await {
                    Ok(zerodha_order_id) => {
                        // Track pending order
                        self.pending_orders.insert(zerodha_order_id.clone(), PendingOrder {
                            order_id: event.order_id,
                            intent_id: event.intent_id,
                            zerodha_order_id: zerodha_order_id.clone(),
                            symbol: symbol.clone(),
                            side,
                            quantity,
                            submitted_utc: Utc::now(),
                            last_poll_utc: Utc::now(),
                        });

                        // Publish accepted status
                        self.bus.publish_order_update(OrderEvent {
                            order_id: event.order_id,
                            intent_id: event.intent_id,
                            timestamp: Utc::now(),
                            symbol: symbol.clone(),
                            side,
                            payload: OrderPayload::Update {
                                status: OrderStatus::Accepted,
                                filled_quantity: 0.0,
                                avg_price: 0.0,
                                commission: 0.0,
                            },
                        })?;

                        // For market orders, assume immediate fill and simulate fill event
                        // In production, you'd poll order status or use WebSocket for updates
                        if price.is_none() {
                            // Market order - assume filled at last price
                            // NOTE: In real production, poll /orders/{order_id} for actual fill price
                            
                            // The actual fill will be confirmed via order status polling
                            // For now, mark as working and let market data update position
                        }
                    }
                    Err(e) => {
                        error!("[ZERODHA LIVE] Order placement failed: {}", e);
                        self.bus.publish_order_update(OrderEvent {
                            order_id: event.order_id,
                            intent_id: event.intent_id,
                            timestamp: Utc::now(),
                            symbol: symbol.clone(),
                            side,
                            payload: OrderPayload::Update {
                                status: OrderStatus::Rejected,
                                filled_quantity: 0.0,
                                avg_price: 0.0,
                                commission: 0.0,
                            },
                        })?;
                    }
                }
            }
            OrderPayload::Cancel => {
                // Find and cancel the order
                let zerodha_id = self.pending_orders.iter()
                    .find(|(_, po)| po.order_id == event.order_id)
                    .map(|(id, _)| id.clone());

                if let Some(zerodha_order_id) = zerodha_id {
                    if let Err(e) = self.cancel_order(&zerodha_order_id).await {
                        warn!("[ZERODHA LIVE] Cancel failed: {}", e);
                    } else {
                        self.pending_orders.remove(&zerodha_order_id);
                        self.bus.publish_order_update(OrderEvent {
                            order_id: event.order_id,
                            intent_id: event.intent_id,
                            timestamp: Utc::now(),
                            symbol: event.symbol.clone(),
                            side: event.side,
                            payload: OrderPayload::Update {
                                status: OrderStatus::Cancelled,
                                filled_quantity: 0.0,
                                avg_price: 0.0,
                                commission: 0.0,
                            },
                        })?;
                    }
                } else {
                    warn!("[ZERODHA LIVE] Order {} not found for cancel", event.order_id);
                }
            }
            _ => {
                warn!("[ZERODHA LIVE] Unsupported order payload: {:?}", event.payload);
            }
        }
        Ok(())
    }

    async fn on_market_data(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        // Live fill confirmation: poll Zerodha order status (no local synthetic fills).
        let now = Utc::now();
        let mut fills: Vec<FillEvent> = Vec::new();
        let mut updates: Vec<OrderUpdate> = Vec::new();
        let mut done: Vec<Uuid> = Vec::new();

        // Iterate pending orders and poll at most once per interval.
        for (internal_id, pending) in self.pending_orders.iter_mut() {
            // Only poll occasionally (avoid hammering the API).
            let since = now - pending.last_poll_utc;
            if since < Duration::milliseconds(self.poll_interval_ms as i64) {
                continue;
            }
            pending.last_poll_utc = now;

            match self
                .poll_order_until_terminal(
                    &pending.zerodha_order_id,
                    self.poll_timeout_ms,
                    self.poll_interval_ms,
                )
                .await
            {
                Ok(Some((status, filled_qty, avg_price, exch_ts))) => {
                    if status == "COMPLETE" && filled_qty > 0.0 && avg_price > 0.0 {
                        let commission = self
                            .commission_model
                            .calculate_commission(&pending.symbol, pending.side, filled_qty, avg_price);

                        fills.push(FillEvent {
                            timestamp: exch_ts.unwrap_or_else(Utc::now),
                            symbol: pending.symbol.clone(),
                            side: pending.side,
                            quantity: filled_qty,
                            price: avg_price,
                            order_id: pending.order_id,
                            venue_order_id: Some(pending.zerodha_order_id.clone()),
                            intent_id: pending.intent_id,
                            commission,
                            commission_asset: "INR".to_string(),
                            slippage: 0.0,
                            venue: "ZERODHA".to_string(),
                        });

                        updates.push(OrderUpdate {
                            order_id: pending.order_id,
                            status: OrderStatus::Filled,
                            filled_quantity: filled_qty,
                            average_price: Some(avg_price),
                            timestamp: exch_ts.unwrap_or_else(Utc::now),
                            reason: Some("Fill confirmed via Zerodha polling".to_string()),
                            venue_order_id: Some(pending.zerodha_order_id.clone()),
                        });

                        done.push(*internal_id);
                    } else {
                        // Terminal but not filled.
                        updates.push(OrderUpdate {
                            order_id: pending.order_id,
                            status: if status == "REJECTED" { OrderStatus::Rejected } else { OrderStatus::Cancelled },
                            filled_quantity: filled_qty,
                            average_price: if avg_price > 0.0 { Some(avg_price) } else { None },
                            timestamp: exch_ts.unwrap_or_else(Utc::now),
                            reason: Some(format!("Order terminal status from broker: {}", status)),
                            venue_order_id: Some(pending.zerodha_order_id.clone()),
                        });
                        done.push(*internal_id);
                    }
                }
                Ok(None) => {
                    // Still pending; keep it.
                }
                Err(e) => {
                    warn!(
                        "[ZERODHA LIVE] order status polling error for {}: {:?}",
                        pending.zerodha_order_id, e
                    );
                }
            }
        }

        // Publish fills and updates.
        for u in updates {
            self.bus.publish_order_update(u).await?;
        }
        for f in fills {
            self.bus.publish_fill(f).await?;
        }

        // Remove completed orders.
        for id in done {
            self.pending_orders.remove(&id);
        }

        // Keep passing market data through (strategy may use it).
        Ok(())
    }

    fn name(&self) -> &'static str {
        "ZerodhaLiveExchange"
    }
}
