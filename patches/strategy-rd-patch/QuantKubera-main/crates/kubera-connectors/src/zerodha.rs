//! # Zerodha Market Connector Module
//!
//! Provides REST integration with Zerodha Kite Connect API for Indian markets.
//!
//! ## Description
//! This module implements a market data connector and order execution client for
//! Zerodha, India's largest retail stockbroker. It supports quote fetching for
//! indices (NIFTY, BANKNIFTY) and F&O order placement.
//!
//! ## Features
//! - REST API polling for index quotes (1-second intervals)
//! - Order placement: Market, Limit, Stop-Loss orders
//! - Product types: NRML (overnight), MIS (intraday), CNC (delivery)
//! - Python sidecar for TOTP-based authentication
//!
//! ## API Endpoints
//! | Endpoint | Method | Description |
//! |----------|--------|-------------|
//! | `/quote` | GET | Fetch live quotes |
//! | `/orders/{variety}` | POST | Place orders |
//! | `/orders/{variety}/{order_id}` | DELETE | Cancel orders |
//! | `/portfolio/positions` | GET | Get positions |
//!
//! ## Authentication
//! Uses Python sidecar script (`zerodha_auth.py`) for TOTP-based login flow.
//! Returns `api_key` and `access_token` for subsequent API calls.
//!
//! ## References
//! - Kite Connect API: <https://kite.trade/docs/connect/v3/>
//! - IEEE Std 1016-2009: Software Design Descriptions

use async_trait::async_trait;
use kubera_core::connector::MarketConnector;
use kubera_core::EventBus;
use kubera_models::{MarketEvent, MarketPayload, Side};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use tracing::{info, warn, error, instrument};
use std::process::Command;
use serde::{Deserialize, Serialize};
use chrono::Utc;

/// Base URL for Kite Connect REST API.
const KITE_API_URL: &str = "https://api.kite.trade";

/// Authentication response from Python sidecar.
#[derive(Deserialize)]
struct AuthOutput {
    /// Access token for API authorization.
    access_token: String,
    /// API key from Kite developer console.
    api_key: String,
}

/// Quote response wrapper from Kite API.
#[derive(Debug, Deserialize)]
struct KiteQuoteResponse {
    /// Response status: "success" or "error".
    status: String,
    /// Quote data keyed by instrument identifier.
    data: HashMap<String, KiteQuote>,
}

/// Individual instrument quote from Kite API.
#[derive(Debug, Deserialize)]
struct KiteQuote {
    /// Last traded price.
    last_price: f64,
    /// Last traded quantity.
    last_quantity: Option<u64>,
    /// Total buy quantity in market.
    #[serde(default)]
    buy_quantity: u64,
    /// Total sell quantity in market.
    #[serde(default)]
    sell_quantity: u64,
}

/// Zerodha market data connector.
///
/// # Description
/// Polls Kite Connect API for live quotes and publishes to event bus.
/// Designed for indices which don't have WebSocket access in free tier.
///
/// # Fields
/// * `bus` - Shared event bus for publishing market events
/// * `symbols` - List of symbols to monitor (e.g., ["NIFTY 50", "BANKNIFTY"])
/// * `running` - Atomic flag for graceful shutdown
pub struct ZerodhaConnector {
    /// Event bus for publishing market data.
    bus: Arc<EventBus>,
    /// Symbols to poll (will be prefixed with "NSE:").
    symbols: Vec<String>,
    /// Atomic shutdown flag.
    running: Arc<AtomicBool>,
}

impl ZerodhaConnector {
    /// Creates a new Zerodha connector instance.
    ///
    /// # Parameters
    /// * `bus` - Shared event bus for market event publishing
    /// * `symbols` - List of symbols to monitor (e.g., vec!["NIFTY 50"])
    ///
    /// # Returns
    /// Configured [`ZerodhaConnector`] ready to start.
    pub fn new(bus: Arc<EventBus>, symbols: Vec<String>) -> Self {
        Self {
            bus,
            symbols,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Authenticates with Zerodha using Python sidecar script.
    ///
    /// # Description
    /// Invokes `zerodha_auth.py` which handles TOTP-based login and returns
    /// API credentials as JSON.
    ///
    /// # Returns
    /// * `Ok((api_key, access_token))` - Credentials for API calls
    /// * `Err` - If authentication script fails
    ///
    /// # Errors
    /// * Script execution failure
    /// * Invalid JSON response
    /// * TOTP/login failure
    fn authenticate(&self) -> anyhow::Result<(String, String)> {
        info!("Running Zerodha authentication sidecar...");
        let output = Command::new("python3")
            .arg("crates/kubera-connectors/scripts/zerodha_auth.py")
            .output()?;

        if !output.status.success() {
            let err = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Zerodha auth failed: {}", err));
        }

        let auth: AuthOutput = serde_json::from_slice(&output.stdout)?;
        Ok((auth.api_key, auth.access_token))
    }

    /// Fetches live quotes for specified instruments.
    ///
    /// # Parameters
    /// * `api_key` - Kite API key
    /// * `access_token` - Session access token
    /// * `instruments` - List of instruments (e.g., ["NSE:NIFTY 50"])
    ///
    /// # Returns
    /// HashMap of instrument identifier to quote data.
    async fn fetch_quotes(&self, api_key: &str, access_token: &str, instruments: &[String]) -> anyhow::Result<HashMap<String, KiteQuote>> {
        let client = reqwest::Client::new();
        
        // Build query params: i=NSE:NIFTY 50&i=NSE:BANKNIFTY
        let params: Vec<(&str, &str)> = instruments.iter()
            .map(|s| ("i", s.as_str()))
            .collect();
        
        let url = format!("{}/quote", KITE_API_URL);
        let response = client.get(&url)
            .query(&params)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", api_key, access_token))
            .send()
            .await?
            .error_for_status()?;
        
        let quote_resp: KiteQuoteResponse = response.json().await?;
        
        if quote_resp.status != "success" {
            return Err(anyhow::anyhow!("Kite API returned non-success status"));
        }
        
        Ok(quote_resp.data)
    }
    
    /// Fetches instrument tokens for given symbols from Kite instruments list.
    async fn get_instrument_tokens(&self, api_key: &str, access_token: &str, symbols: &[String]) -> anyhow::Result<Vec<(String, u32)>> {
        let client = reqwest::Client::new();
        
        // Determine exchange based on symbol type
        let mut tokens = Vec::new();
        
        for symbol in symbols {
            let exchange = if symbol.contains("FUT") || symbol.contains("CE") || symbol.contains("PE") {
                "NFO"
            } else {
                "NSE"
            };
            
            // Fetch quote to get instrument token
            let url = format!("{}/quote", KITE_API_URL);
            let instrument = format!("{}:{}", exchange, symbol);
            
            let response = client.get(&url)
                .query(&[("i", &instrument)])
                .header("X-Kite-Version", "3")
                .header("Authorization", format!("token {}:{}", api_key, access_token))
                .send()
                .await?;
            
            if let Ok(quote_resp) = response.json::<KiteQuoteResponse>().await {
                if quote_resp.status == "success" {
                    for (key, _) in quote_resp.data {
                        // Key contains the instrument token info
                        // We need to fetch it from instruments API
                        tokens.push((symbol.clone(), self.lookup_token(&key, api_key, access_token).await.unwrap_or(0)));
                    }
                }
            }
        }
        
        // Filter out zero tokens
        let valid_tokens: Vec<_> = tokens.into_iter().filter(|(_, t)| *t > 0).collect();
        
        if valid_tokens.is_empty() {
            // Fallback: fetch from instruments master
            return self.fetch_tokens_from_master(api_key, access_token, symbols).await;
        }
        
        Ok(valid_tokens)
    }
    
    /// Lookup token from instruments master file
    async fn lookup_token(&self, _key: &str, _api_key: &str, _access_token: &str) -> anyhow::Result<u32> {
        // The key format is "exchange:tradingsymbol"
        // For now, return 0 to trigger fallback
        Ok(0)
    }
    
    /// Fetch tokens from instruments master file
    async fn fetch_tokens_from_master(&self, api_key: &str, access_token: &str, symbols: &[String]) -> anyhow::Result<Vec<(String, u32)>> {
        let client = reqwest::Client::new();
        
        // Fetch NFO instruments
        let url = format!("{}/instruments/NFO", KITE_API_URL);
        let response = client.get(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", api_key, access_token))
            .send()
            .await?
            .text()
            .await?;
        
        let mut tokens = Vec::new();
        
        // Parse CSV (header: instrument_token,exchange_token,tradingsymbol,...)
        for line in response.lines().skip(1) {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 3 {
                let token: u32 = parts[0].parse().unwrap_or(0);
                let tradingsymbol = parts[2];
                
                for symbol in symbols {
                    if tradingsymbol == symbol {
                        tokens.push((symbol.clone(), token));
                        info!(symbol = %symbol, token = token, "Found instrument token");
                    }
                }
            }
        }
        
        Ok(tokens)
    }
    
    /// Parse binary tick data from Kite WebSocket
    /// Format: 2 bytes (num_packets) + N * packet_data
    /// Each packet: 2 bytes (packet_len) + instrument_token (4) + ltp (4) + ... 
    fn parse_binary_ticks(&self, data: &[u8], token_map: &HashMap<u32, String>) -> Option<Vec<(String, f64, u32)>> {
        if data.len() < 4 {
            return None; // Heartbeat or too short
        }
        
        use byteorder::{BigEndian, ByteOrder};
        
        let num_packets = BigEndian::read_i16(&data[0..2]) as usize;
        let mut ticks = Vec::new();
        let mut offset = 2;
        
        for _ in 0..num_packets {
            if offset + 2 > data.len() {
                break;
            }
            
            let packet_len = BigEndian::read_i16(&data[offset..offset+2]) as usize;
            offset += 2;
            
            if offset + packet_len > data.len() || packet_len < 8 {
                break;
            }
            
            let packet = &data[offset..offset + packet_len];
            let token = BigEndian::read_u32(&packet[0..4]);
            let ltp_paise = BigEndian::read_i32(&packet[4..8]);
            let ltp = ltp_paise as f64 / 100.0; // Convert paise to rupees
            
            // For quote mode (44 bytes) or full mode (184 bytes)
            let volume = if packet_len >= 32 {
                BigEndian::read_u32(&packet[28..32])
            } else {
                1
            };
            
            // Log for debugging
            tracing::debug!(token = token, ltp = ltp, packet_len = packet_len, "Parsed Kite tick");
            
            if let Some(symbol) = token_map.get(&token) {
                ticks.push((symbol.clone(), ltp, volume));
            } else {
                // Token not in map - log it
                tracing::warn!(token = token, ltp = ltp, "Unknown instrument token");
            }
            
            offset += packet_len;
        }
        
        if ticks.is_empty() {
            None
        } else {
            Some(ticks)
        }
    }
}

#[async_trait]
impl MarketConnector for ZerodhaConnector {
    #[instrument(skip(self))]
    async fn run(&self) -> anyhow::Result<()> {
        let (api_key, access_token) = self.authenticate()?;
        info!("Zerodha authenticated successfully.");

        // Get instrument tokens for symbols
        let instrument_tokens = self.get_instrument_tokens(&api_key, &access_token, &self.symbols).await?;
        
        if instrument_tokens.is_empty() {
            return Err(anyhow::anyhow!("No valid instrument tokens found for: {:?}", self.symbols));
        }
        
        info!(tokens = ?instrument_tokens, "Starting Zerodha WebSocket streaming");
        
        // WebSocket URL with auth
        let ws_url = format!("wss://ws.kite.trade/?api_key={}&access_token={}", api_key, access_token);
        
        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
        let (mut write, mut read) = futures::StreamExt::split(ws_stream);
        
        // Subscribe to instruments
        let subscribe_msg = serde_json::json!({
            "a": "subscribe",
            "v": instrument_tokens.iter().map(|(_, t)| *t).collect::<Vec<_>>()
        });
        
        use futures::SinkExt;
        use tokio_tungstenite::tungstenite::Message;
        write.send(Message::Text(subscribe_msg.to_string())).await?;
        
        // Set mode to "quote" for full tick data
        let mode_msg = serde_json::json!({
            "a": "mode",
            "v": ["quote", instrument_tokens.iter().map(|(_, t)| *t).collect::<Vec<_>>()]
        });
        write.send(Message::Text(mode_msg.to_string())).await?;
        
        info!("Subscribed to {} instruments via WebSocket", instrument_tokens.len());
        
        // Create token to symbol map
        let token_to_symbol: HashMap<u32, String> = instrument_tokens.into_iter()
            .map(|(s, t)| (t, s))
            .collect();
        
        // Read loop
        while self.running.load(Ordering::SeqCst) {
            use futures::StreamExt;
            match read.next().await {
                Some(Ok(Message::Binary(data))) => {
                    // Parse binary market data
                    if let Some(ticks) = self.parse_binary_ticks(&data, &token_to_symbol) {
                        for (symbol, price, volume) in ticks {
                            let event = MarketEvent {
                                exchange_time: Utc::now(),
                                local_time: Utc::now(),
                                symbol: symbol.clone(),
                                payload: MarketPayload::Tick {
                                    price,
                                    size: volume as f64,
                                    side: Side::Buy, // WebSocket doesn't give side
                                },
                            };
                            
                            if let Err(e) = self.bus.publish_market(event).await {
                                warn!(symbol = %symbol, error = %e, "Failed to publish Zerodha tick");
                            }
                        }
                    }
                }
                Some(Ok(Message::Text(text))) => {
                    // JSON messages (postbacks, errors)
                    if text.len() > 1 { // Skip heartbeats
                        info!(msg = %text, "Zerodha WebSocket message");
                    }
                }
                Some(Ok(Message::Ping(_))) => {
                    // Respond to pings
                }
                Some(Err(e)) => {
                    error!(error = %e, "WebSocket error");
                    break;
                }
                None => break,
                _ => {}
            }
        }

        Ok(())
    }

    fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn name(&self) -> &'static str {
        "Zerodha"
    }
}

// ============================================
// ORDER EXECUTION FOR FnO
// ============================================

/// Order variety for Zerodha
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderVariety {
    Regular,
    Amo,      // After-market order
    Co,       // Cover order
    Iceberg,
    Auction,
}

/// Order type for Zerodha
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    Market,
    Limit,
    Sl,       // Stop-loss
    SlM,      // Stop-loss market
}

/// Transaction type
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransactionType {
    Buy,
    Sell,
}

/// Product type
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ProductType {
    Nrml,     // Normal F&O
    Mis,      // Intraday
    Cnc,      // Cash and Carry (equity delivery)
}

/// Order request for Zerodha
#[derive(Debug, Clone, Serialize)]
pub struct ZerodhaOrderRequest {
    pub tradingsymbol: String,
    pub exchange: String,
    pub transaction_type: TransactionType,
    pub order_type: OrderType,
    pub quantity: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_price: Option<f64>,
    pub product: ProductType,
    pub validity: String,
}

/// Order response from Zerodha
#[derive(Debug, Clone, Deserialize)]
pub struct ZerodhaOrderResponse {
    pub status: String,
    pub data: Option<OrderData>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderData {
    pub order_id: String,
}

/// Zerodha execution client for order placement
pub struct ZerodhaExecutionClient {
    api_key: String,
    access_token: String,
    client: reqwest::Client,
}

impl ZerodhaExecutionClient {
    pub fn new(api_key: String, access_token: String) -> Self {
        Self {
            api_key,
            access_token,
            client: reqwest::Client::new(),
        }
    }

    /// Authenticate and create client
    pub fn from_sidecar() -> anyhow::Result<Self> {
        let output = Command::new("python3")
            .arg("crates/kubera-connectors/scripts/zerodha_auth.py")
            .output()?;

        if !output.status.success() {
            let err = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Zerodha auth failed: {}", err));
        }

        let auth: AuthOutput = serde_json::from_slice(&output.stdout)?;
        Ok(Self::new(auth.api_key, auth.access_token))
    }

    /// Place a market order
    #[instrument(skip(self))]
    pub async fn place_market_order(
        &self,
        tradingsymbol: &str,
        exchange: &str,
        side: TransactionType,
        quantity: u32,
        product: ProductType,
    ) -> anyhow::Result<String> {
        let order = ZerodhaOrderRequest {
            tradingsymbol: tradingsymbol.to_string(),
            exchange: exchange.to_string(),
            transaction_type: side,
            order_type: OrderType::Market,
            quantity,
            price: None,
            trigger_price: None,
            product,
            validity: "DAY".to_string(),
        };

        self.place_order(OrderVariety::Regular, order).await
    }

    /// Place a limit order
    #[instrument(skip(self))]
    pub async fn place_limit_order(
        &self,
        tradingsymbol: &str,
        exchange: &str,
        side: TransactionType,
        quantity: u32,
        price: f64,
        product: ProductType,
    ) -> anyhow::Result<String> {
        let order = ZerodhaOrderRequest {
            tradingsymbol: tradingsymbol.to_string(),
            exchange: exchange.to_string(),
            transaction_type: side,
            order_type: OrderType::Limit,
            quantity,
            price: Some(price),
            trigger_price: None,
            product,
            validity: "DAY".to_string(),
        };

        self.place_order(OrderVariety::Regular, order).await
    }

    /// Place a generic order
    #[instrument(skip(self))]
    pub async fn place_order(
        &self,
        variety: OrderVariety,
        order: ZerodhaOrderRequest,
    ) -> anyhow::Result<String> {
        let variety_str = match variety {
            OrderVariety::Regular => "regular",
            OrderVariety::Amo => "amo",
            OrderVariety::Co => "co",
            OrderVariety::Iceberg => "iceberg",
            OrderVariety::Auction => "auction",
        };

        let url = format!("{}/orders/{}", KITE_API_URL, variety_str);
        
        info!(
            tradingsymbol = %order.tradingsymbol,
            exchange = %order.exchange,
            quantity = order.quantity,
            "Placing Zerodha order"
        );

        let response = self.client.post(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .form(&[
                ("tradingsymbol", order.tradingsymbol.as_str()),
                ("exchange", order.exchange.as_str()),
                ("transaction_type", &format!("{:?}", order.transaction_type).to_uppercase()),
                ("order_type", &format!("{:?}", order.order_type).to_uppercase()),
                ("quantity", &order.quantity.to_string()),
                ("product", &format!("{:?}", order.product).to_uppercase()),
                ("validity", &order.validity),
            ])
            .send()
            .await?;

        let _status = response.status();
        let resp: ZerodhaOrderResponse = response.json().await?;

        if resp.status != "success" {
            let msg = resp.message.unwrap_or_else(|| "Unknown error".to_string());
            error!(error = %msg, "Order placement failed");
            return Err(anyhow::anyhow!("Order failed: {}", msg));
        }

        let order_id = resp.data
            .ok_or_else(|| anyhow::anyhow!("No order ID in response"))?
            .order_id;

        info!(order_id = %order_id, "Order placed successfully");
        Ok(order_id)
    }

    /// Cancel an order
    #[instrument(skip(self))]
    pub async fn cancel_order(&self, order_id: &str, variety: OrderVariety) -> anyhow::Result<()> {
        let variety_str = match variety {
            OrderVariety::Regular => "regular",
            OrderVariety::Amo => "amo",
            OrderVariety::Co => "co",
            OrderVariety::Iceberg => "iceberg",
            OrderVariety::Auction => "auction",
        };

        let url = format!("{}/orders/{}/{}", KITE_API_URL, variety_str, order_id);
        
        info!(order_id = %order_id, "Cancelling Zerodha order");

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

        info!(order_id = %order_id, "Order cancelled successfully");
        Ok(())
    }

    /// Get order history
    #[instrument(skip(self))]
    pub async fn get_orders(&self) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}/orders", KITE_API_URL);
        
        let response = self.client.get(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?;

        let orders = response.json::<serde_json::Value>().await?;
        Ok(orders)
    }

    /// Get positions
    #[instrument(skip(self))]
    pub async fn get_positions(&self) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}/portfolio/positions", KITE_API_URL);
        
        let response = self.client.get(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?;

        let positions = response.json::<serde_json::Value>().await?;
        Ok(positions)
    }
}
