//! # Zerodha Market Connector Module
//!
//! Production-grade WebSocket integration with Zerodha Kite Connect API.
//!
//! ## Description
//! This module implements a robust market data connector for Zerodha, India's
//! largest retail stockbroker. Features automatic reconnection, heartbeat
//! monitoring, and full L2 order book parsing.
//!
//! ## Features
//! - **WebSocket Streaming**: Real-time tick data with full depth (5 levels)
//! - **Auto-Reconnection**: Exponential backoff with configurable max retries
//! - **Heartbeat Monitoring**: Detects stale connections and triggers reconnect
//! - **Full L2 Parsing**: 184-byte full mode packets with bid/ask depth
//! - **Order Execution**: Market, Limit, Stop-Loss orders for F&O
//!
//! ## Kite WebSocket Binary Format
//! | Packet Size | Mode   | Data Available                      |
//! |-------------|--------|-------------------------------------|
//! | 8 bytes     | LTP    | Last traded price only              |
//! | 44 bytes    | Quote  | LTP + OHLC + volume                 |
//! | 184 bytes   | Full   | Quote + 5-level market depth        |
//!
//! ## References
//! - Kite Connect API: <https://kite.trade/docs/connect/v3/>
//! - WebSocket Binary Format: <https://kite.trade/docs/connect/v3/websocket/>

use async_trait::async_trait;
use kubera_core::connector::MarketConnector;
use kubera_core::EventBus;
use kubera_models::{MarketEvent, MarketPayload, Side, L2Update, L2Level};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn, error, debug, instrument};
use std::process::Command;
use serde::{Deserialize, Serialize};
use chrono::{Utc, NaiveDate, Datelike, Weekday, Timelike};
use tokio::time::{sleep, Instant};

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
#[allow(dead_code)]
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

/// Configuration for WebSocket reconnection behavior.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before first reconnection attempt
    pub initial_delay_ms: u64,
    /// Maximum delay between reconnection attempts
    pub max_delay_ms: u64,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Maximum number of reconnection attempts (0 = infinite)
    pub max_retries: u32,
    /// Heartbeat timeout in seconds
    pub heartbeat_timeout_secs: u64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 1000,
            max_delay_ms: 60000,
            backoff_multiplier: 2.0,
            max_retries: 0, // Infinite retries
            heartbeat_timeout_secs: 30,
        }
    }
}

/// Zerodha production-grade market data connector.
///
/// # Description
/// Streams real-time market data via Kite WebSocket with automatic
/// reconnection, heartbeat monitoring, and full L2 depth parsing.
///
/// # Features
/// - Exponential backoff reconnection
/// - Heartbeat timeout detection
/// - Full 184-byte market depth parsing
/// - Automatic token resolution from instruments API
///
/// # Fields
/// * `bus` - Shared event bus for publishing market events
/// * `symbols` - List of symbols to monitor
/// * `running` - Atomic flag for graceful shutdown
/// * `reconnect_config` - Configuration for reconnection behavior
/// * `last_heartbeat` - Timestamp of last received message
/// * `message_count` - Total messages received (for monitoring)
pub struct ZerodhaConnector {
    /// Event bus for publishing market data.
    bus: Arc<EventBus>,
    /// Symbols to stream (F&O symbols like "NIFTY2612025400CE").
    symbols: Vec<String>,
    /// Atomic shutdown flag.
    running: Arc<AtomicBool>,
    /// Reconnection configuration.
    reconnect_config: ReconnectConfig,
    /// Message counter for monitoring.
    message_count: Arc<AtomicU64>,
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
            reconnect_config: ReconnectConfig::default(),
            message_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates a connector with custom reconnection configuration.
    pub fn with_reconnect_config(bus: Arc<EventBus>, symbols: Vec<String>, config: ReconnectConfig) -> Self {
        Self {
            bus,
            symbols,
            running: Arc::new(AtomicBool::new(true)),
            reconnect_config: config,
            message_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the total number of messages received.
    pub fn message_count(&self) -> u64 {
        self.message_count.load(Ordering::Relaxed)
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
    #[allow(dead_code)]
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
    
    /// Parse binary tick data from Kite WebSocket.
    ///
    /// # Kite Binary Format
    /// - Header: 2 bytes (number of packets)
    /// - Each packet: 2 bytes (length) + data
    ///
    /// ## Packet sizes by mode:
    /// - LTP mode (8 bytes): token(4) + ltp(4)
    /// - Quote mode (44 bytes): + ohlc(16) + volume(4) + change(8) + oi(4) + oi_day_change(8)
    /// - Full mode (184 bytes): + depth(160) for 5 bid/ask levels
    ///
    /// # Depth format (per level, 12 bytes each):
    /// - quantity: i32 (4 bytes)
    /// - price: i32 (4 bytes, divide by 100)
    /// - orders: i16 (2 bytes) + padding (2 bytes)
    fn parse_binary_ticks(&self, data: &[u8], token_map: &HashMap<u32, String>) -> Option<Vec<ParsedTick>> {
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

            let packet_len = BigEndian::read_i16(&data[offset..offset + 2]) as usize;
            offset += 2;

            if offset + packet_len > data.len() || packet_len < 8 {
                break;
            }

            let packet = &data[offset..offset + packet_len];
            let token = BigEndian::read_u32(&packet[0..4]);
            let ltp_paise = BigEndian::read_i32(&packet[4..8]);
            let ltp = ltp_paise as f64 / 100.0;

            // Parse based on packet length
            let mut tick = ParsedTick {
                token,
                ltp,
                volume: 0,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                bids: Vec::new(),
                asks: Vec::new(),
            };

            // Quote mode (44+ bytes) - includes OHLC and volume
            if packet_len >= 44 {
                tick.open = BigEndian::read_i32(&packet[8..12]) as f64 / 100.0;
                tick.high = BigEndian::read_i32(&packet[12..16]) as f64 / 100.0;
                tick.low = BigEndian::read_i32(&packet[16..20]) as f64 / 100.0;
                tick.close = BigEndian::read_i32(&packet[20..24]) as f64 / 100.0;
                tick.volume = BigEndian::read_u32(&packet[28..32]);
            }

            // Full mode (184 bytes) - includes 5-level market depth
            if packet_len >= 184 {
                // Depth starts at offset 44
                // Buy depth: 5 levels * 12 bytes = 60 bytes (offsets 44-104)
                // Sell depth: 5 levels * 12 bytes = 60 bytes (offsets 104-164)
                let depth_start = 44;

                // Parse buy side (bids) - 5 levels
                for i in 0..5 {
                    let level_offset = depth_start + (i * 12);
                    if level_offset + 12 <= packet_len {
                        let qty = BigEndian::read_i32(&packet[level_offset..level_offset + 4]);
                        let price = BigEndian::read_i32(&packet[level_offset + 4..level_offset + 8]) as f64 / 100.0;
                        // Note: orders count at offset +8 (2 bytes) not used in L2Level

                        if qty > 0 && price > 0.0 {
                            tick.bids.push(L2Level {
                                price,
                                size: qty as f64,
                            });
                        }
                    }
                }

                // Parse sell side (asks) - 5 levels
                for i in 0..5 {
                    let level_offset = depth_start + 60 + (i * 12); // 60 = 5 * 12 bytes for bids
                    if level_offset + 12 <= packet_len {
                        let qty = BigEndian::read_i32(&packet[level_offset..level_offset + 4]);
                        let price = BigEndian::read_i32(&packet[level_offset + 4..level_offset + 8]) as f64 / 100.0;
                        // Note: orders count at offset +8 (2 bytes) not used in L2Level

                        if qty > 0 && price > 0.0 {
                            tick.asks.push(L2Level {
                                price,
                                size: qty as f64,
                            });
                        }
                    }
                }
            }

            debug!(
                token = token,
                ltp = ltp,
                packet_len = packet_len,
                bid_levels = tick.bids.len(),
                ask_levels = tick.asks.len(),
                "Parsed Kite tick"
            );

            if token_map.contains_key(&token) {
                ticks.push(tick);
            } else {
                warn!(token = token, ltp = ltp, "Unknown instrument token");
            }

            offset += packet_len;
        }

        // Update message counter
        self.message_count.fetch_add(ticks.len() as u64, Ordering::Relaxed);

        if ticks.is_empty() {
            None
        } else {
            Some(ticks)
        }
    }
}

/// Parsed tick data from Kite WebSocket.
#[derive(Debug, Clone)]
pub struct ParsedTick {
    pub token: u32,
    pub ltp: f64,
    pub volume: u32,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub bids: Vec<L2Level>,
    pub asks: Vec<L2Level>,
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

        // Create token to symbol map
        let token_to_symbol: HashMap<u32, String> = instrument_tokens.iter()
            .map(|(s, t)| (*t, s.clone()))
            .collect();

        // Reconnection loop with exponential backoff
        let mut retry_count = 0u32;
        let mut current_delay = self.reconnect_config.initial_delay_ms;

        while self.running.load(Ordering::SeqCst) {
            match self.connect_and_stream(&api_key, &access_token, &instrument_tokens, &token_to_symbol).await {
                Ok(()) => {
                    // Clean shutdown
                    info!("WebSocket connection closed cleanly");
                    break;
                }
                Err(e) => {
                    error!(error = %e, retry = retry_count, "WebSocket connection failed");

                    // Check if we should retry
                    if self.reconnect_config.max_retries > 0 && retry_count >= self.reconnect_config.max_retries {
                        error!("Max reconnection attempts ({}) reached, giving up", self.reconnect_config.max_retries);
                        return Err(e);
                    }

                    if !self.running.load(Ordering::SeqCst) {
                        break;
                    }

                    // Exponential backoff
                    info!(delay_ms = current_delay, "Reconnecting in...");
                    sleep(Duration::from_millis(current_delay)).await;

                    // Increase delay for next retry
                    current_delay = ((current_delay as f64 * self.reconnect_config.backoff_multiplier) as u64)
                        .min(self.reconnect_config.max_delay_ms);
                    retry_count += 1;
                }
            }
        }

        Ok(())
    }

    fn stop(&self) {
        info!("Stopping Zerodha connector (received {} messages)", self.message_count());
        self.running.store(false, Ordering::SeqCst);
    }

    fn name(&self) -> &'static str {
        "Zerodha"
    }
}

impl ZerodhaConnector {
    /// Internal method to handle a single WebSocket connection session.
    async fn connect_and_stream(
        &self,
        api_key: &str,
        access_token: &str,
        instrument_tokens: &[(String, u32)],
        token_to_symbol: &HashMap<u32, String>,
    ) -> anyhow::Result<()> {
        use futures::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        // WebSocket URL with auth
        let ws_url = format!("wss://ws.kite.trade/?api_key={}&access_token={}", api_key, access_token);

        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();

        info!("WebSocket connected to Kite");

        // Subscribe to instruments
        let token_list: Vec<u32> = instrument_tokens.iter().map(|(_, t)| *t).collect();
        let subscribe_msg = serde_json::json!({
            "a": "subscribe",
            "v": token_list
        });
        write.send(Message::Text(subscribe_msg.to_string())).await?;

        // Set mode to "full" for complete L2 depth data (184 bytes per tick)
        let mode_msg = serde_json::json!({
            "a": "mode",
            "v": ["full", token_list]
        });
        write.send(Message::Text(mode_msg.to_string())).await?;

        info!(
            instruments = instrument_tokens.len(),
            mode = "full",
            "Subscribed to instruments with L2 depth"
        );

        // Track last message time for heartbeat monitoring
        let mut last_message_time = Instant::now();
        let heartbeat_timeout = Duration::from_secs(self.reconnect_config.heartbeat_timeout_secs);

        // Read loop with heartbeat monitoring
        loop {
            if !self.running.load(Ordering::SeqCst) {
                info!("Shutdown requested, closing WebSocket");
                let _ = write.send(Message::Close(None)).await;
                break;
            }

            // Check for heartbeat timeout
            if last_message_time.elapsed() > heartbeat_timeout {
                warn!(
                    elapsed_secs = last_message_time.elapsed().as_secs(),
                    timeout_secs = heartbeat_timeout.as_secs(),
                    "Heartbeat timeout - reconnecting"
                );
                return Err(anyhow::anyhow!("Heartbeat timeout"));
            }

            // Use timeout for reading to allow periodic heartbeat checks
            let read_timeout = Duration::from_secs(5);
            match tokio::time::timeout(read_timeout, read.next()).await {
                Ok(Some(Ok(Message::Binary(data)))) => {
                    last_message_time = Instant::now();

                    if let Some(ticks) = self.parse_binary_ticks(&data, token_to_symbol) {
                        for tick in ticks {
                            let symbol = token_to_symbol.get(&tick.token)
                                .cloned()
                                .unwrap_or_else(|| format!("TOKEN:{}", tick.token));

                            // Publish tick event
                            let tick_event = MarketEvent {
                                exchange_time: Utc::now(),
                                local_time: Utc::now(),
                                symbol: symbol.clone(),
                                payload: MarketPayload::Tick {
                                    price: tick.ltp,
                                    size: tick.volume as f64,
                                    side: Side::Buy,
                                },
                            };

                            if let Err(e) = self.bus.publish_market(tick_event).await {
                                warn!(symbol = %symbol, error = %e, "Failed to publish tick");
                            }

                            // Publish L2 update if depth data is available
                            if !tick.bids.is_empty() || !tick.asks.is_empty() {
                                let l2_event = MarketEvent {
                                    exchange_time: Utc::now(),
                                    local_time: Utc::now(),
                                    symbol: symbol.clone(),
                                    payload: MarketPayload::L2Update(L2Update {
                                        bids: tick.bids.clone(),
                                        asks: tick.asks.clone(),
                                        first_update_id: 0, // Zerodha doesn't provide sequence numbers
                                        last_update_id: 0,
                                    }),
                                };

                                if let Err(e) = self.bus.publish_market(l2_event).await {
                                    warn!(symbol = %symbol, error = %e, "Failed to publish L2 update");
                                }
                            }
                        }
                    }
                }
                Ok(Some(Ok(Message::Text(text)))) => {
                    last_message_time = Instant::now();
                    if text.len() > 2 {
                        debug!(msg = %text, "Kite WebSocket text message");
                    }
                }
                Ok(Some(Ok(Message::Ping(data)))) => {
                    last_message_time = Instant::now();
                    let _ = write.send(Message::Pong(data)).await;
                }
                Ok(Some(Ok(Message::Pong(_)))) => {
                    last_message_time = Instant::now();
                }
                Ok(Some(Ok(Message::Close(_)))) => {
                    info!("Server closed WebSocket connection");
                    return Err(anyhow::anyhow!("Server closed connection"));
                }
                Ok(Some(Err(e))) => {
                    error!(error = %e, "WebSocket read error");
                    return Err(e.into());
                }
                Ok(None) => {
                    info!("WebSocket stream ended");
                    return Err(anyhow::anyhow!("Stream ended"));
                }
                Err(_) => {
                    // Timeout - continue loop to check heartbeat and shutdown flag
                    continue;
                }
                _ => {}
            }
        }

        Ok(())
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

// =============================================================================
// AUTO-DISCOVERY MODULE
// Automatically discovers ATM NIFTY/BANKNIFTY options for the nearest expiry
// =============================================================================

/// Represents an NFO instrument from Zerodha's instruments master
#[derive(Debug, Clone)]
pub struct NfoInstrument {
    pub instrument_token: u32,
    pub tradingsymbol: String,
    pub name: String,  // NIFTY, BANKNIFTY, etc.
    pub expiry: NaiveDate,
    pub strike: f64,
    pub instrument_type: String, // CE or PE
    pub lot_size: u32,
}

/// Configuration for auto-discovery
#[derive(Debug, Clone)]
pub struct AutoDiscoveryConfig {
    /// Underlying to discover (NIFTY, BANKNIFTY, FINNIFTY)
    pub underlying: String,
    /// Number of strikes around ATM to include (e.g., 2 = ATM ± 2 strikes)
    pub strikes_around_atm: u32,
    /// Strike interval (50 for NIFTY, 100 for BANKNIFTY)
    pub strike_interval: f64,
    /// Include only weekly expiry (next Thursday)
    pub weekly_only: bool,
}

impl Default for AutoDiscoveryConfig {
    fn default() -> Self {
        Self {
            underlying: "NIFTY".to_string(),
            strikes_around_atm: 1,  // ATM and 1 strike each side
            strike_interval: 50.0,
            weekly_only: true,
        }
    }
}

impl AutoDiscoveryConfig {
    pub fn nifty_atm() -> Self {
        Self::default()
    }

    pub fn banknifty_atm() -> Self {
        Self {
            underlying: "BANKNIFTY".to_string(),
            strikes_around_atm: 1,
            strike_interval: 100.0,
            weekly_only: true,
        }
    }
}

/// Auto-discovery client for Zerodha NFO instruments
pub struct ZerodhaAutoDiscovery {
    api_key: String,
    access_token: String,
    client: reqwest::Client,
}

impl ZerodhaAutoDiscovery {
    pub fn new(api_key: String, access_token: String) -> Self {
        Self {
            api_key,
            access_token,
            client: reqwest::Client::new(),
        }
    }

    /// Create from sidecar authentication
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

    /// Get current spot price for an underlying (NIFTY, BANKNIFTY, etc.)
    pub async fn get_spot_price(&self, underlying: &str) -> anyhow::Result<f64> {
        let symbol = match underlying.to_uppercase().as_str() {
            "NIFTY" => "NSE:NIFTY 50",
            "BANKNIFTY" => "NSE:NIFTY BANK",
            "FINNIFTY" => "NSE:NIFTY FIN SERVICE",
            _ => return Err(anyhow::anyhow!("Unknown underlying: {}", underlying)),
        };

        let url = format!("{}/quote", KITE_API_URL);
        let response = self.client.get(&url)
            .query(&[("i", symbol)])
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?;

        let quote_resp: KiteQuoteResponse = response.json().await?;

        if quote_resp.status != "success" {
            return Err(anyhow::anyhow!("Failed to fetch spot price for {}", underlying));
        }

        for (_, quote) in quote_resp.data {
            return Ok(quote.last_price);
        }

        Err(anyhow::anyhow!("No quote data for {}", underlying))
    }

    /// Fetch all NFO instruments from Zerodha master
    pub async fn fetch_nfo_instruments(&self) -> anyhow::Result<Vec<NfoInstrument>> {
        let url = format!("{}/instruments/NFO", KITE_API_URL);

        info!("[AUTO-DISCOVERY] Fetching NFO instruments master...");

        let response = self.client.get(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?
            .text()
            .await?;

        let mut instruments = Vec::new();

        // CSV format: instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,strike,tick_size,lot_size,instrument_type,segment,exchange
        for line in response.lines().skip(1) {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() < 10 {
                continue;
            }

            let instrument_type = parts[9].trim_matches('"');

            // Only keep options (CE/PE)
            if instrument_type != "CE" && instrument_type != "PE" {
                continue;
            }

            let instrument_token: u32 = parts[0].parse().unwrap_or(0);
            let tradingsymbol = parts[2].trim_matches('"').to_string();
            let name = parts[3].trim_matches('"').to_string();
            let expiry_str = parts[5].trim_matches('"');
            let strike: f64 = parts[6].parse().unwrap_or(0.0);
            let lot_size: u32 = parts[8].parse().unwrap_or(1);

            // Parse expiry date (format: YYYY-MM-DD)
            let expiry = match NaiveDate::parse_from_str(expiry_str, "%Y-%m-%d") {
                Ok(d) => d,
                Err(_) => continue,
            };

            instruments.push(NfoInstrument {
                instrument_token,
                tradingsymbol,
                name,
                expiry,
                strike,
                instrument_type: instrument_type.to_string(),
                lot_size,
            });
        }

        info!("[AUTO-DISCOVERY] Loaded {} NFO option instruments", instruments.len());
        Ok(instruments)
    }

    /// Find the next weekly expiry (Thursday) from today
    pub fn next_weekly_expiry() -> NaiveDate {
        let today = Utc::now().date_naive();
        let days_until_thursday = (Weekday::Thu.num_days_from_monday() as i64
            - today.weekday().num_days_from_monday() as i64 + 7) % 7;

        if days_until_thursday == 0 {
            // If today is Thursday, use today if before market close, otherwise next week
            let now = Utc::now();
            if now.hour() < 15 || (now.hour() == 15 && now.minute() < 30) {
                today
            } else {
                today + chrono::Duration::days(7)
            }
        } else {
            today + chrono::Duration::days(days_until_thursday)
        }
    }

    /// Calculate ATM strike from spot price
    pub fn atm_strike(spot: f64, strike_interval: f64) -> f64 {
        (spot / strike_interval).round() * strike_interval
    }

    /// Auto-discover ATM options based on configuration
    /// Returns a list of (tradingsymbol, instrument_token) pairs
    pub async fn discover_symbols(&self, config: &AutoDiscoveryConfig) -> anyhow::Result<Vec<(String, u32)>> {
        // 1. Get current spot price
        let spot = self.get_spot_price(&config.underlying).await?;
        info!("[AUTO-DISCOVERY] {} spot price: {:.2}", config.underlying, spot);

        // 2. Calculate ATM strike
        let atm = Self::atm_strike(spot, config.strike_interval);
        info!("[AUTO-DISCOVERY] ATM strike: {:.0}", atm);

        // 3. Determine target expiry
        let target_expiry = Self::next_weekly_expiry();
        info!("[AUTO-DISCOVERY] Target expiry: {}", target_expiry);

        // 4. Fetch all NFO instruments
        let instruments = self.fetch_nfo_instruments().await?;

        // 5. Filter to matching underlying and expiry
        let matching: Vec<_> = instruments.iter()
            .filter(|i| i.name == config.underlying)
            .filter(|i| i.expiry == target_expiry)
            .collect();

        if matching.is_empty() {
            // Try finding next available expiry if exact match not found
            let available_expiries: Vec<_> = instruments.iter()
                .filter(|i| i.name == config.underlying)
                .filter(|i| i.expiry >= Utc::now().date_naive())
                .map(|i| i.expiry)
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            if let Some(nearest) = available_expiries.into_iter().min() {
                info!("[AUTO-DISCOVERY] Exact expiry {} not found, using nearest: {}", target_expiry, nearest);
                return self.discover_symbols_for_expiry(config, spot, atm, nearest, &instruments).await;
            }

            return Err(anyhow::anyhow!("No options found for {} expiry {}", config.underlying, target_expiry));
        }

        self.discover_symbols_for_expiry(config, spot, atm, target_expiry, &instruments).await
    }

    async fn discover_symbols_for_expiry(
        &self,
        config: &AutoDiscoveryConfig,
        spot: f64,
        atm: f64,
        expiry: NaiveDate,
        instruments: &[NfoInstrument],
    ) -> anyhow::Result<Vec<(String, u32)>> {
        let mut result = Vec::new();

        // Generate target strikes around ATM
        let mut target_strikes = vec![atm];
        for i in 1..=config.strikes_around_atm {
            target_strikes.push(atm + (i as f64) * config.strike_interval);
            target_strikes.push(atm - (i as f64) * config.strike_interval);
        }

        info!("[AUTO-DISCOVERY] Looking for strikes: {:?}", target_strikes);

        // Find matching instruments
        for instr in instruments.iter()
            .filter(|i| i.name == config.underlying)
            .filter(|i| i.expiry == expiry)
        {
            if target_strikes.iter().any(|&s| (instr.strike - s).abs() < 0.01) {
                info!(
                    "[AUTO-DISCOVERY] Found: {} (token={}, strike={}, type={}, lot={})",
                    instr.tradingsymbol, instr.instrument_token, instr.strike, instr.instrument_type, instr.lot_size
                );
                result.push((instr.tradingsymbol.clone(), instr.instrument_token));
            }
        }

        if result.is_empty() {
            return Err(anyhow::anyhow!(
                "No options found for {} at strikes {:?} expiry {}",
                config.underlying, target_strikes, expiry
            ));
        }

        // Sort by strike and type for consistent ordering
        result.sort_by(|a, b| a.0.cmp(&b.0));

        info!("[AUTO-DISCOVERY] Discovered {} symbols for {}", result.len(), config.underlying);
        Ok(result)
    }

    /// Resolve symbols - replace "AUTO" markers with actual discovered symbols
    /// Markers: "NIFTY-AUTO", "BANKNIFTY-AUTO", "NIFTY-ATM", etc.
    pub async fn resolve_symbols(&self, symbols: &[String]) -> anyhow::Result<Vec<(String, u32)>> {
        let mut result = Vec::new();

        for symbol in symbols {
            let upper = symbol.to_uppercase();

            if upper.contains("-AUTO") || upper.contains("-ATM") {
                // Extract underlying name
                let underlying = upper
                    .replace("-AUTO", "")
                    .replace("-ATM", "")
                    .trim()
                    .to_string();

                let config = match underlying.as_str() {
                    "NIFTY" => AutoDiscoveryConfig::nifty_atm(),
                    "BANKNIFTY" => AutoDiscoveryConfig::banknifty_atm(),
                    _ => AutoDiscoveryConfig {
                        underlying: underlying.clone(),
                        ..Default::default()
                    },
                };

                match self.discover_symbols(&config).await {
                    Ok(discovered) => {
                        info!("[AUTO-DISCOVERY] Resolved {} -> {} symbols", symbol, discovered.len());
                        result.extend(discovered);
                    }
                    Err(e) => {
                        error!("[AUTO-DISCOVERY] Failed to discover {}: {}", symbol, e);
                    }
                }
            } else {
                // Regular symbol - will need token lookup later
                result.push((symbol.clone(), 0));
            }
        }

        Ok(result)
    }
}
