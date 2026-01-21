//! # CoinDCX Market Connector Module
//!
//! Provides REST and WebSocket integration with CoinDCX cryptocurrency exchange.
//!
//! ## Description
//! This module implements a market data connector for CoinDCX (India's largest
//! cryptocurrency exchange). It supports both REST API for order management
//! and WebSocket for real-time market data streaming.
//!
//! ## Features
//! - REST API: Ticker, orderbook, balances, order placement/cancellation
//! - WebSocket: Real-time trades and orderbook updates
//! - HMAC-SHA256 request signing for authenticated endpoints
//!
//! ## API Endpoints
//! | Endpoint | Type | Authentication |
//! |----------|------|----------------|
//! | `/exchange/ticker` | REST | Public |
//! | `/market_data/orderbook` | REST | Public |
//! | `/exchange/v1/users/balances` | REST | Signed |
//! | `/exchange/v1/orders/create` | REST | Signed |
//!
//! ## References
//! - CoinDCX API Documentation: <https://docs.coindcx.com/>
//! - IEEE Std 1016-2009: Software Design Descriptions

use async_trait::async_trait;
use kubera_core::connector::MarketConnector;
use kubera_core::EventBus;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tracing::{info, warn, error};

/// Base URL for CoinDCX REST API endpoints.
const COINDCX_REST_BASE: &str = "https://api.coindcx.com";

/// Base URL for CoinDCX WebSocket streaming.
const COINDCX_WS_BASE: &str = "wss://stream.coindcx.com";

/// CoinDCX market data and order execution connector.
///
/// # Description
/// Provides unified access to CoinDCX exchange for market data retrieval
/// and order execution. Implements the [`MarketConnector`] trait for
/// integration with the QuantKubera event bus architecture.
///
/// # Fields
/// * `api_key` - CoinDCX API key for authentication
/// * `api_secret` - HMAC secret for request signing
/// * `symbols` - List of trading pairs to subscribe (e.g., ["BTC", "ETH"])
/// * `event_bus` - Shared event bus for publishing market events
///
/// # Examples
/// ```ignore
/// use kubera_connectors::coindcx::CoinDCXConnector;
/// use kubera_core::EventBus;
/// use std::sync::Arc;
///
/// let bus = EventBus::new(1024);
/// let connector = CoinDCXConnector::new(
///     "api_key".into(),
///     "secret".into(),
///     vec!["BTC".into()],
///     bus,
/// );
/// ```
pub struct CoinDCXConnector {
    /// API key for CoinDCX authentication.
    api_key: String,
    /// Secret key for HMAC-SHA256 request signing.
    api_secret: String,
    /// Trading symbols to monitor (base currency, e.g., "BTC").
    symbols: Vec<String>,
    /// Shared event bus for publishing market data events.
    event_bus: Arc<EventBus>,
    /// HTTP client for REST API requests.
    client: reqwest::Client,
    /// Local orderbook cache keyed by symbol.
    orderbooks: Arc<RwLock<HashMap<String, OrderBook>>>,
    /// Atomic flag for graceful shutdown.
    running: Arc<AtomicBool>,
}

/// Local orderbook cache structure.
///
/// # Description
/// Maintains a local copy of the order book for latency-sensitive operations.
/// Updated via WebSocket orderbook delta messages.
#[derive(Debug, Clone, Default)]
struct OrderBook {
    /// Bid levels as (price, quantity) tuples, sorted descending by price.
    bids: Vec<(f64, f64)>,
    /// Ask levels as (price, quantity) tuples, sorted ascending by price.
    asks: Vec<(f64, f64)>,
    /// Exchange timestamp of last update in milliseconds.
    last_update: u64,
}

/// CoinDCX ticker response from REST API.
///
/// # Description
/// Contains last traded price, best bid/ask, and 24h volume for a market.
#[derive(Debug, Deserialize)]
pub struct CoinDCXTicker {
    /// Market identifier (e.g., "BTCUSDT").
    pub market: String,
    /// Last traded price as string.
    pub last_price: String,
    /// Best bid price as string.
    pub bid: String,
    /// Best ask price as string.
    pub ask: String,
    /// 24-hour trading volume as string.
    pub volume: String,
    /// Exchange timestamp in milliseconds.
    pub timestamp: u64,
}

/// CoinDCX orderbook snapshot from REST API.
#[derive(Debug, Deserialize)]
pub struct CoinDCXOrderbook {
    /// Bid price levels.
    pub bids: Vec<CoinDCXLevel>,
    /// Ask price levels.
    pub asks: Vec<CoinDCXLevel>,
}

/// Single price level in orderbook.
#[derive(Debug, Deserialize)]
pub struct CoinDCXLevel {
    /// Price at this level as string.
    pub price: String,
    /// Quantity available at this level as string.
    pub quantity: String,
}

/// Account balance for a single currency.
#[derive(Debug, Deserialize)]
pub struct CoinDCXBalance {
    /// Currency symbol (e.g., "BTC", "INR").
    pub currency: String,
    /// Available balance as string.
    pub balance: String,
    /// Balance locked in open orders as string.
    pub locked_balance: String,
}

/// Order placement request structure.
#[derive(Debug, Serialize)]
pub struct CoinDCXOrderRequest {
    /// Market identifier (e.g., "BTCUSDT").
    pub market: String,
    /// Order side: "buy" or "sell".
    pub side: String,
    /// Order type: "market_order" or "limit_order".
    pub order_type: String,
    /// Limit price (None for market orders).
    pub price_per_unit: Option<String>,
    /// Order quantity as string.
    pub total_quantity: String,
    /// Request timestamp in milliseconds.
    pub timestamp: u64,
}

/// Order response from CoinDCX API.
#[derive(Debug, Deserialize)]
pub struct CoinDCXOrderResponse {
    /// Unique order identifier.
    pub id: String,
    /// Market identifier.
    pub market: String,
    /// Order status: "open", "partially_filled", "filled", "cancelled".
    pub status: String,
    /// Order side: "buy" or "sell".
    pub side: String,
    /// Limit price per unit.
    pub price_per_unit: String,
    /// Total order quantity.
    pub total_quantity: String,
    /// Remaining unfilled quantity.
    pub remaining_quantity: String,
}

impl CoinDCXConnector {
    /// Creates a new CoinDCX connector instance.
    ///
    /// # Parameters
    /// * `api_key` - CoinDCX API key from developer portal
    /// * `api_secret` - API secret for HMAC signing
    /// * `symbols` - Base currencies to monitor (e.g., vec!["BTC", "ETH"])
    /// * `event_bus` - Shared event bus for market event publishing
    ///
    /// # Returns
    /// Configured [`CoinDCXConnector`] ready to start.
    pub fn new(
        api_key: String,
        api_secret: String,
        symbols: Vec<String>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            api_key,
            api_secret,
            symbols,
            event_bus,
            client: reqwest::Client::new(),
            orderbooks: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Fetch ticker for a market
    pub async fn get_ticker(&self, market: &str) -> anyhow::Result<CoinDCXTicker> {
        let url = format!("{}/exchange/ticker", COINDCX_REST_BASE);
        let response: Vec<CoinDCXTicker> = self.client
            .get(&url)
            .send()
            .await?
            .json()
            .await?;
        
        response.into_iter()
            .find(|t| t.market == market)
            .ok_or_else(|| anyhow::anyhow!("Market {} not found", market))
    }

    /// Fetch orderbook for a market
    pub async fn get_orderbook(&self, market: &str) -> anyhow::Result<CoinDCXOrderbook> {
        let url = format!("{}/market_data/orderbook?pair={}", COINDCX_REST_BASE, market);
        let response = self.client
            .get(&url)
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }

    /// Get account balances (authenticated)
    pub async fn get_balances(&self) -> anyhow::Result<Vec<CoinDCXBalance>> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        
        let body = serde_json::json!({
            "timestamp": timestamp
        });
        
        let signature = self.sign_request(&body.to_string());
        
        let url = format!("{}/exchange/v1/users/balances", COINDCX_REST_BASE);
        let response = self.client
            .post(&url)
            .header("X-AUTH-APIKEY", &self.api_key)
            .header("X-AUTH-SIGNATURE", &signature)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        
        Ok(response)
    }

    /// Place a market order
    pub async fn place_market_order(
        &self,
        market: &str,
        side: &str,
        quantity: f64,
    ) -> anyhow::Result<CoinDCXOrderResponse> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        
        let body = serde_json::json!({
            "market": market,
            "side": side,
            "order_type": "market_order",
            "total_quantity": quantity.to_string(),
            "timestamp": timestamp
        });
        
        let signature = self.sign_request(&body.to_string());
        
        let url = format!("{}/exchange/v1/orders/create", COINDCX_REST_BASE);
        let response = self.client
            .post(&url)
            .header("X-AUTH-APIKEY", &self.api_key)
            .header("X-AUTH-SIGNATURE", &signature)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        
        Ok(response)
    }

    /// Place a limit order
    pub async fn place_limit_order(
        &self,
        market: &str,
        side: &str,
        price: f64,
        quantity: f64,
    ) -> anyhow::Result<CoinDCXOrderResponse> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        
        let body = serde_json::json!({
            "market": market,
            "side": side,
            "order_type": "limit_order",
            "price_per_unit": price.to_string(),
            "total_quantity": quantity.to_string(),
            "timestamp": timestamp
        });
        
        let signature = self.sign_request(&body.to_string());
        
        let url = format!("{}/exchange/v1/orders/create", COINDCX_REST_BASE);
        let response = self.client
            .post(&url)
            .header("X-AUTH-APIKEY", &self.api_key)
            .header("X-AUTH-SIGNATURE", &signature)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        
        Ok(response)
    }

    /// Cancel an order
    pub async fn cancel_order(&self, order_id: &str) -> anyhow::Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        
        let body = serde_json::json!({
            "id": order_id,
            "timestamp": timestamp
        });
        
        let signature = self.sign_request(&body.to_string());
        
        let url = format!("{}/exchange/v1/orders/cancel", COINDCX_REST_BASE);
        let _response = self.client
            .post(&url)
            .header("X-AUTH-APIKEY", &self.api_key)
            .header("X-AUTH-SIGNATURE", &signature)
            .json(&body)
            .send()
            .await?;
        
        Ok(())
    }

    fn sign_request(&self, body: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        
        type HmacSha256 = Hmac<Sha256>;
        
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(body.as_bytes());
        
        hex::encode(mac.finalize().into_bytes())
    }

    /// Start WebSocket stream for real-time data
    pub async fn start_ws_stream(&self) -> anyhow::Result<()> {
        use tokio_tungstenite::{connect_async, tungstenite::Message};
        use futures_util::{SinkExt, StreamExt};
        
        let url = format!("{}/socket.io/?EIO=3&transport=websocket", COINDCX_WS_BASE);
        
        info!("Connecting to CoinDCX WebSocket: {}", url);
        
        let (ws_stream, _) = connect_async(&url).await?;
        let (mut write, mut read) = ws_stream.split();
        
        // Subscribe to channels
        for symbol in &self.symbols {
            let sub_msg = serde_json::json!({
                "event": "join",
                "channel": format!("{}-USDT@trades", symbol)
            });
            write.send(Message::Text(sub_msg.to_string())).await?;
        }
        
        info!("Subscribed to {} CoinDCX channels", self.symbols.len());
        
        // Process messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = self.process_ws_message(&text).await {
                        warn!("Error processing CoinDCX message: {}", e);
                    }
                }
                Ok(Message::Ping(data)) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Ok(Message::Close(_)) => {
                    warn!("CoinDCX WebSocket closed, reconnecting...");
                    break;
                }
                Err(e) => {
                    error!("CoinDCX WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        
        Ok(())
    }

    async fn process_ws_message(&self, text: &str) -> anyhow::Result<()> {
        use chrono::Utc;
        use kubera_models::{MarketEvent, MarketPayload, Side, L2Update, L2Level};
        
        // Socket.IO protocol: messages start with packet type
        // 0 = open, 2 = ping, 3 = pong, 4 = message, 42 = event message
        
        // Skip Socket.IO control messages
        if text.starts_with("0") || text.starts_with("3") || text.len() < 2 {
            return Ok(());
        }
        
        // Handle Socket.IO event messages (start with "42")
        let json_start = if text.starts_with("42") {
            2
        } else if text.starts_with("4") {
            1
        } else {
            return Ok(());
        };
        
        let json_str = &text[json_start..];
        
        // Parse as JSON array: ["event_name", {data}]
        let parsed: serde_json::Value = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(_) => return Ok(()), // Skip malformed messages
        };
        
        let arr = match parsed.as_array() {
            Some(a) if a.len() >= 2 => a,
            _ => return Ok(()),
        };
        
        let event_name = arr[0].as_str().unwrap_or("");
        let data = &arr[1];
        
        match event_name {
            // Trade event: {"s": "BTCINR", "p": "5000000", "q": "0.01", "m": true, "T": 1234567890}
            "new-trade" | "trade" => {
                let symbol = data["s"].as_str()
                    .or_else(|| data["symbol"].as_str())
                    .unwrap_or("");
                    
                let price: f64 = data["p"].as_str()
                    .or_else(|| data["price"].as_str())
                    .and_then(|s| s.parse().ok())
                    .or_else(|| data["p"].as_f64())
                    .unwrap_or(0.0);
                    
                let quantity: f64 = data["q"].as_str()
                    .or_else(|| data["quantity"].as_str())
                    .and_then(|s| s.parse().ok())
                    .or_else(|| data["q"].as_f64())
                    .unwrap_or(0.0);
                    
                let is_buyer_maker = data["m"].as_bool().unwrap_or(false);
                let trade_id = data["T"].as_i64()
                    .or_else(|| data["t"].as_i64())
                    .unwrap_or(0);
                
                if price > 0.0 && quantity > 0.0 {
                    let event = MarketEvent {
                        exchange_time: Utc::now(),
                        local_time: Utc::now(),
                        symbol: symbol.to_string(),
                        payload: MarketPayload::Trade {
                            trade_id,
                            price,
                            quantity,
                            is_buyer_maker,
                        },
                    };
                    
                    if let Err(e) = self.event_bus.publish_market(event).await {
                        warn!("Failed to publish CoinDCX trade: {}", e);
                    }
                }
            }
            
            // Depth update: {"s": "BTCINR", "b": [["5000", "1.5"]], "a": [["5001", "2.0"]]}
            "depth-update" | "depthUpdate" => {
                let symbol = data["s"].as_str()
                    .or_else(|| data["symbol"].as_str())
                    .unwrap_or("");
                    
                let bids: Vec<L2Level> = data["b"].as_array()
                    .or_else(|| data["bids"].as_array())
                    .map(|arr| {
                        arr.iter().filter_map(|level| {
                            let price = level[0].as_str()?.parse().ok()?;
                            let size = level[1].as_str()?.parse().ok()?;
                            Some(L2Level { price, size })
                        }).collect()
                    })
                    .unwrap_or_default();
                    
                let asks: Vec<L2Level> = data["a"].as_array()
                    .or_else(|| data["asks"].as_array())
                    .map(|arr| {
                        arr.iter().filter_map(|level| {
                            let price = level[0].as_str()?.parse().ok()?;
                            let size = level[1].as_str()?.parse().ok()?;
                            Some(L2Level { price, size })
                        }).collect()
                    })
                    .unwrap_or_default();
                    
                let update_id = data["u"].as_u64()
                    .or_else(|| data["lastUpdateId"].as_u64())
                    .unwrap_or(0);
                
                if !bids.is_empty() || !asks.is_empty() {
                    let l2_update = L2Update {
                        bids,
                        asks,
                        first_update_id: update_id,
                        last_update_id: update_id,
                    };
                    
                    // Update local orderbook
                    {
                        let mut books = self.orderbooks.write().await;
                        let book = books.entry(symbol.to_string()).or_default();
                        for bid in &l2_update.bids {
                            if bid.size == 0.0 {
                                book.bids.retain(|(p, _)| (*p - bid.price).abs() > 1e-10);
                            } else {
                                book.bids.push((bid.price, bid.size));
                            }
                        }
                        for ask in &l2_update.asks {
                            if ask.size == 0.0 {
                                book.asks.retain(|(p, _)| (*p - ask.price).abs() > 1e-10);
                            } else {
                                book.asks.push((ask.price, ask.size));
                            }
                        }
                        book.last_update = update_id;
                    }
                    
                    let event = MarketEvent {
                        exchange_time: Utc::now(),
                        local_time: Utc::now(),
                        symbol: symbol.to_string(),
                        payload: MarketPayload::L2Update(l2_update),
                    };
                    
                    if let Err(e) = self.event_bus.publish_market(event).await {
                        warn!("Failed to publish CoinDCX depth: {}", e);
                    }
                }
            }
            
            // Ticker - convert to Tick event
            "ticker" => {
                let symbol = data["s"].as_str().unwrap_or("");
                let price: f64 = data["c"].as_str() // Last price
                    .and_then(|s| s.parse().ok())
                    .or_else(|| data["c"].as_f64())
                    .unwrap_or(0.0);
                let volume: f64 = data["v"].as_str()
                    .and_then(|s| s.parse().ok())
                    .or_else(|| data["v"].as_f64())
                    .unwrap_or(0.0);
                    
                if price > 0.0 {
                    let event = MarketEvent {
                        exchange_time: Utc::now(),
                        local_time: Utc::now(),
                        symbol: symbol.to_string(),
                        payload: MarketPayload::Tick {
                            price,
                            size: volume,
                            side: Side::Buy, // Default, ticker doesn't have side
                        },
                    };
                    
                    if let Err(e) = self.event_bus.publish_market(event).await {
                        warn!("Failed to publish CoinDCX ticker: {}", e);
                    }
                }
            }
            
            _ => {
                // Unknown event type, log at debug level
                tracing::debug!("CoinDCX unknown event: {}", event_name);
            }
        }
        
        Ok(())
    }
}

#[async_trait]
impl MarketConnector for CoinDCXConnector {
    async fn run(&self) -> anyhow::Result<()> {
        self.running.store(true, Ordering::SeqCst);
        info!("CoinDCX connector starting for symbols: {:?}", self.symbols);
        
        while self.running.load(Ordering::SeqCst) {
            if let Err(e) = self.start_ws_stream().await {
                error!("CoinDCX WebSocket error: {}, reconnecting in 5s...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
        
        Ok(())
    }

    fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        info!("CoinDCX connector stopped");
    }

    fn name(&self) -> &'static str {
        "CoinDCX"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use std::sync::Arc;

    #[test]
    fn test_coindcx_connector_creation() {
        let bus = EventBus::new(1024);
        let connector = CoinDCXConnector::new(
            "test_key".to_string(),
            "test_secret".to_string(),
            vec!["BTC".to_string()],
            bus,
        );
        assert_eq!(connector.name(), "CoinDCX");
    }
}
