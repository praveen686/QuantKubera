//! # Order Execution Engine Module
//!
//! Simulated exchange for backtesting and paper trading.
//!
//! ## Description
//! Implements a realistic exchange simulation with:
//! - Order matching against market ticks
//! - Configurable slippage model
//! - Position tracking with FIFO accounting
//! - Deterministic execution for reproducible backtests
//!
//! ## Order Flow
//! ```text
//! Strategy → OrderEvent → SimulatedExchange → Fill → PositionUpdate
//! ```
//!
//! ## Slippage Model
//! - Base slippage: Configurable basis points
//! - Random component: ±10% of base (for realism)
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use kubera_models::{MarketEvent, MarketPayload, OrderEvent, OrderPayload, OrderStatus, Side};
use kubera_core::EventBus;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tracing::{info, warn};
use chrono::Utc;
use uuid::Uuid;
use std::collections::HashMap;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;

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
