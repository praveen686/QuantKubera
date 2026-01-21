//! # External Market Connectors Hub
//!
//! Integration layer for connecting to external exchanges and venues.
//!
//! ## Description
//! Provides the primary orchestration for market data ingestion and order 
//! execution across multiple venues. Implements standardized trait interfaces
//! for:
//! - **Binance (SBE)**: High-performance binary stream decoder.
//! - **CoinDCX**: Indian crypto exchange integration.
//! - **Zerodha (Kite)**: Indian equity and derivatives broker.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Binance SBE Protocol specification
//! - Kite Connect API specification

pub mod execution;
pub mod rest_client;
pub mod zerodha;
pub mod coindcx;

pub use zerodha::ZerodhaConnector;
pub use coindcx::CoinDCXConnector;

use async_trait::async_trait;
use kubera_core::EventBus;
use kubera_models::{MarketEvent, MarketPayload, Side, L2Update};
use kubera_sbe::{SbeHeader, BinanceSbeDecoder};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite::client::IntoClientRequest};
use futures::{StreamExt, SinkExt};
use url::Url;
use tracing::{info, error, warn, debug, instrument};
use chrono::Utc;

/// Telemetry and health metrics for the connector engine.
#[derive(Default)]
pub struct ConnectorStats {
    pub messages_received: AtomicU64,
    pub trades_decoded: AtomicU64,
    pub depth_updates_decoded: AtomicU64,
    pub depth_updates_applied: AtomicU64,
    pub depth_updates_dropped: AtomicU64,
    pub depth_updates_buffered: AtomicU64,
    pub snapshots_fetched: AtomicU64,
    pub sequence_gaps: AtomicU64,
    pub decode_errors: AtomicU64,
    pub reconnects: AtomicU64,
}

impl ConnectorStats {
    /// Initializes zeroed metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Flushes current counters to the observability log.
    pub fn log_stats(&self) {
        info!(
            msgs = %self.messages_received.load(Ordering::Relaxed),
            trades = %self.trades_decoded.load(Ordering::Relaxed),
            depth_decoded = %self.depth_updates_decoded.load(Ordering::Relaxed),
            depth_applied = %self.depth_updates_applied.load(Ordering::Relaxed),
            depth_dropped = %self.depth_updates_dropped.load(Ordering::Relaxed),
            depth_buffered = %self.depth_updates_buffered.load(Ordering::Relaxed),
            snapshots = %self.snapshots_fetched.load(Ordering::Relaxed),
            gaps = %self.sequence_gaps.load(Ordering::Relaxed),
            errors = %self.decode_errors.load(Ordering::Relaxed),
            reconnects = %self.reconnects.load(Ordering::Relaxed),
            "Connector stats"
        );
    }
}

/// Control state for the L2 book synchronization process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncState {
    /// Collecting initial depth increments while waiting for REST snapshot.
    Buffering,
    /// Applying buffered increments onto the baseline snapshot.
    Synchronizing,
    /// Real-time application of streaming increments.
    Synchronized,
    /// Consistency violation detected; clearing state for full re-fetch.
    Resync,
}

/// Container for out-of-order depth increments used during sync.
#[derive(Debug, Clone)]
struct BufferedDepthUpdate {
    first_update_id: u64,
    last_update_id: u64,
    update: L2Update,
    exchange_time: chrono::DateTime<Utc>,
}

/// High-performance connector for Binance Spot using Simple Binary Encoding.
///
/// # Design
/// Uses a per-symbol actor model where each instrument is managed by its 
/// own isolated task and WebSocket connection to minimize "head-of-line" blocking.
pub struct BinanceConnector {
    bus: Arc<EventBus>,
    symbols: Vec<String>,
    api_key: String,
    stats: Arc<ConnectorStats>,
    running: Arc<AtomicBool>,
}

impl BinanceConnector {
    /// Creates a new connector instance.
    pub fn new(bus: Arc<EventBus>, symbols: Vec<String>, api_key: String) -> Self {
        Self {
            bus,
            symbols,
            api_key,
            stats: Arc::new(ConnectorStats::new()),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Accesses the telemetry shared with the symbol handlers.
    pub fn stats(&self) -> Arc<ConnectorStats> {
        self.stats.clone()
    }

    /// Signals all internal tasks to terminate.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Spawns symbol-specific handlers and manages their lifetime.
    #[instrument(skip(self), fields(symbols = ?self.symbols))]
    pub async fn run_market_data(&self) -> anyhow::Result<()> {
        let mut handlers = Vec::new();
        
        for symbol in &self.symbols {
            let symbol = symbol.clone();
            let bus = self.bus.clone();
            let api_key = self.api_key.clone();
            let stats = self.stats.clone();
            let running = self.running.clone();
            
            handlers.push(tokio::spawn(async move {
                let handler = SymbolHandler {
                    bus,
                    symbol,
                    api_key,
                    stats,
                    running,
                };
                if let Err(e) = handler.run().await {
                    error!(symbol = %handler.symbol, error = %e, "Symbol handler failed");
                }
            }));
        }

        futures::future::join_all(handlers).await;
        Ok(())
    }
}

#[async_trait]
impl kubera_core::connector::MarketConnector for BinanceConnector {
    async fn run(&self) -> anyhow::Result<()> {
        self.run_market_data().await
    }

    fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn name(&self) -> &'static str {
        "Binance"
    }
}

/// Internal actor for managing a single instrument's lifecycle.
struct SymbolHandler {
    bus: Arc<EventBus>,
    symbol: String,
    api_key: String,
    stats: Arc<ConnectorStats>,
    running: Arc<AtomicBool>,
}

impl SymbolHandler {
    /// Execution loop with exponential backoff for connection persistence.
    async fn run(&self) -> anyhow::Result<()> {
        let mut backoff_secs = 1u64;
        const MAX_BACKOFF: u64 = 30;

        while self.running.load(Ordering::SeqCst) {
            match self.connect_and_stream().await {
                Ok(()) => {
                    info!(symbol = %self.symbol, "Connection closed gracefully");
                    break;
                }
                Err(e) => {
                    self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                    error!(symbol = %self.symbol, error = %e, backoff_secs, "Connection error, reconnecting");
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF);
                }
            }
        }
        Ok(())
    }

    /// Retrieves the initial orderbook state via REST.
    #[instrument(skip(self))]
    async fn fetch_snapshot(&self) -> anyhow::Result<(u64, kubera_models::L2Snapshot)> {
        self.stats.snapshots_fetched.fetch_add(1, Ordering::Relaxed);
        let depth = rest_client::fetch_depth_snapshot(&self.symbol, 1000).await?;
        let snapshot = rest_client::depth_to_snapshot(&depth);
        let update_id = depth.last_update_id;
        info!(symbol = %self.symbol, update_id, bids = snapshot.bids.len(), asks = snapshot.asks.len(), "Fetched L2 snapshot");
        Ok((update_id, snapshot))
    }

    /// Manages the WebSocket connection and state transitions.
    #[instrument(skip(self))]
    async fn connect_and_stream(&self) -> anyhow::Result<()> {
        let symbol_lc = self.symbol.to_lowercase().replace("-", "");
        // SBE combined streams: /stream?streams=<stream1>/<stream2>
        let url_str = format!(
            "wss://stream-sbe.binance.com:9443/stream?streams={}@trade/{}@depth",
            symbol_lc, symbol_lc
        );
        let url = Url::parse(&url_str)?;

        info!(symbol = %self.symbol, url = %url, "Connecting to Binance SBE Stream");

        let mut request = url.into_client_request()?;
        request.headers_mut().insert("X-MBX-APIKEY", self.api_key.parse()?);
        request.headers_mut().insert("Sec-WebSocket-Protocol", "binance-sbe".parse()?);

        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();

        let mut sync_state = SyncState::Buffering;
        let mut depth_buffer: VecDeque<BufferedDepthUpdate> = VecDeque::with_capacity(1000);
        let mut snapshot_update_id: u64 = 0;
        let mut last_applied_u: u64 = 0;
        let mut first_event_processed = false;

        let mut last_pong = Instant::now();
        let heartbeat_timeout = Duration::from_secs(60);
        let mut last_stats_log = Instant::now();

        while self.running.load(Ordering::SeqCst) {
            if last_stats_log.elapsed() > Duration::from_secs(30) {
                last_stats_log = Instant::now();
            }

            if last_pong.elapsed() > heartbeat_timeout {
                warn!(symbol = %self.symbol, elapsed = ?last_pong.elapsed(), "Heartbeat timeout, reconnecting");
                break;
            }

            match sync_state {
                SyncState::Buffering => {
                    if depth_buffer.len() >= 10 || (depth_buffer.len() > 0 && last_pong.elapsed() > Duration::from_secs(2)) {
                        info!(symbol = %self.symbol, buffered = depth_buffer.len(), "Fetching snapshot for synchronization");
                        match self.fetch_snapshot().await {
                            Ok((sid, snapshot)) => {
                                snapshot_update_id = sid;
                                let _ = self.bus.publish_market(MarketEvent {
                                    exchange_time: Utc::now(),
                                    local_time: Utc::now(),
                                    symbol: self.symbol.clone(),
                                    payload: MarketPayload::L2Snapshot(snapshot),
                                }).await;
                                sync_state = SyncState::Synchronizing;
                            }
                            Err(e) => error!(symbol = %self.symbol, error = %e, "Failed to fetch snapshot"),
                        }
                    }
                }
                SyncState::Synchronizing => {
                    while let Some(buffered) = depth_buffer.pop_front() {
                        if buffered.last_update_id <= snapshot_update_id {
                            self.stats.depth_updates_dropped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        if !first_event_processed {
                            let target = snapshot_update_id + 1;
                            if buffered.first_update_id <= target && buffered.last_update_id >= target {
                                first_event_processed = true;
                                last_applied_u = buffered.last_update_id;
                                self.publish_depth_update(buffered.update, buffered.exchange_time).await;
                                self.stats.depth_updates_applied.fetch_add(1, Ordering::Relaxed);
                            } else {
                                self.stats.depth_updates_dropped.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            if buffered.first_update_id == last_applied_u + 1 {
                                last_applied_u = buffered.last_update_id;
                                self.publish_depth_update(buffered.update, buffered.exchange_time).await;
                                self.stats.depth_updates_applied.fetch_add(1, Ordering::Relaxed);
                            } else if buffered.first_update_id > last_applied_u + 1 {
                                warn!(symbol = %self.symbol, expected = last_applied_u + 1, got = buffered.first_update_id, "Sequence gap detected in buffered updates");
                                self.stats.sequence_gaps.fetch_add(1, Ordering::Relaxed);
                                sync_state = SyncState::Resync;
                                break;
                            } else {
                                self.stats.depth_updates_dropped.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }

                    if sync_state == SyncState::Synchronizing && first_event_processed {
                        sync_state = SyncState::Synchronized;
                        info!(symbol = %self.symbol, last_applied_u, "Order book synchronized");
                    }
                }
                SyncState::Synchronized => {}
                SyncState::Resync => {
                    warn!(symbol = %self.symbol, "Re-synchronizing order book");
                    depth_buffer.clear();
                    first_event_processed = false;
                    last_applied_u = 0;
                    snapshot_update_id = 0;
                    sync_state = SyncState::Buffering;
                }
            }

            let msg = tokio::time::timeout(Duration::from_millis(100), read.next()).await;

            match msg {
                Ok(Some(Ok(Message::Binary(bin)))) => {
                    // Reset heartbeat on any data received
                    last_pong = Instant::now();
                    self.stats.messages_received.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("kubera_ingestion_messages_total", "symbol" => self.symbol.clone()).increment(1);
                    if bin.len() < 8 { continue; }

                    if let Ok(header) = SbeHeader::decode(&bin[0..8]) {
                        debug!(symbol = %self.symbol, template_id = header.template_id, len = bin.len(), "SBE message received");
                        match header.template_id {
                            10000 => {
                                info!(symbol = %self.symbol, "Trade message (10000) received!");
                                self.handle_trade(&header, &bin[8..]);
                            }
                            10003 => {
                                self.handle_depth_update(
                                    &header,
                                    &bin[8..],
                                    &mut sync_state,
                                    &mut depth_buffer,
                                    snapshot_update_id,
                                    &mut last_applied_u,
                                    first_event_processed,
                                ).await;
                            }
                            _ => debug!(symbol = %self.symbol, template_id = header.template_id, "Unhandled SBE template"),
                        }
                    }
                }
                Ok(Some(Ok(Message::Ping(p)))) => { let _ = write.send(Message::Pong(p)).await; }
                Ok(Some(Ok(Message::Pong(_)))) => { last_pong = Instant::now(); }
                Ok(Some(Err(e))) => { error!(symbol = %self.symbol, error = %e, "WebSocket error"); break; }
                Ok(None) => { info!(symbol = %self.symbol, "WebSocket stream ended"); break; }
                Err(_) => { let _ = write.send(Message::Ping(vec![])).await; }
                _ => {}
            }
        }
        Ok(())
    }

    fn handle_trade(&self, header: &SbeHeader, body: &[u8]) {
        match BinanceSbeDecoder::decode_trade(header, body) {
            Ok(trade) => {
                self.stats.trades_decoded.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("kubera_ingestion_trades_total", "symbol" => self.symbol.clone()).increment(1);
                
                let aggressor_side = if trade.is_buyer_maker { Side::Sell } else { Side::Buy };
                let exchange_time = trade.exchange_time();
                let now = Utc::now();
                let latency_ms = (now - exchange_time).num_milliseconds() as f64;
                metrics::histogram!("kubera_ingestion_latency_ms", "symbol" => self.symbol.clone(), "type" => "trade").record(latency_ms);

                let bus = self.bus.clone();
                let symbol = self.symbol.clone();
                tokio::spawn(async move {
                    let _ = bus.publish_market(MarketEvent {
                        exchange_time,
                        local_time: now,
                        symbol,
                        payload: MarketPayload::Tick {
                            price: trade.price,
                            size: trade.quantity,
                            side: aggressor_side,
                        }
                    }).await;
                });
            }
            Err(e) => {
                self.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                warn!(symbol = %self.symbol, error = %e, "Failed to decode trade");
            }
        }
    }

    async fn handle_depth_update(
        &self,
        header: &SbeHeader,
        body: &[u8],
        sync_state: &mut SyncState,
        depth_buffer: &mut VecDeque<BufferedDepthUpdate>,
        snapshot_update_id: u64,
        last_applied_u: &mut u64,
        _first_event_processed: bool,
    ) {
        match BinanceSbeDecoder::decode_depth_update(header, body) {
            Ok(depth) => {
                self.stats.depth_updates_decoded.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("kubera_ingestion_depth_updates_total", "symbol" => self.symbol.clone()).increment(1);
                
                let exchange_time = depth.exchange_time();
                let now = Utc::now();
                let latency_ms = (now - exchange_time).num_milliseconds() as f64;
                metrics::histogram!("kubera_ingestion_latency_ms", "symbol" => self.symbol.clone(), "type" => "depth").record(latency_ms);
                let update = L2Update {
                    bids: depth.bids,
                    asks: depth.asks,
                    first_update_id: depth.first_update_id,
                    last_update_id: depth.last_update_id,
                };

                match *sync_state {
                    SyncState::Buffering | SyncState::Synchronizing => {
                        self.stats.depth_updates_buffered.fetch_add(1, Ordering::Relaxed);
                        depth_buffer.push_back(BufferedDepthUpdate {
                            first_update_id: depth.first_update_id,
                            last_update_id: depth.last_update_id,
                            update,
                            exchange_time,
                        });
                        if depth_buffer.len() > 10000 { depth_buffer.pop_front(); }
                    }
                    SyncState::Synchronized => {
                        if depth.last_update_id <= snapshot_update_id {
                            self.stats.depth_updates_dropped.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        if depth.first_update_id == *last_applied_u + 1 {
                            *last_applied_u = depth.last_update_id;
                            self.publish_depth_update(update, exchange_time).await;
                            self.stats.depth_updates_applied.fetch_add(1, Ordering::Relaxed);
                        } else if depth.first_update_id > *last_applied_u + 1 {
                            warn!(symbol = %self.symbol, expected = *last_applied_u + 1, got = depth.first_update_id, "Sequence gap detected, triggering resync");
                            self.stats.sequence_gaps.fetch_add(1, Ordering::Relaxed);
                            *sync_state = SyncState::Resync;
                        } else {
                            self.stats.depth_updates_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                self.stats.decode_errors.fetch_add(1, Ordering::Relaxed);
                warn!(symbol = %self.symbol, error = %e, "Failed to decode depth update");
            }
        }
    }

    async fn publish_depth_update(&self, update: L2Update, exchange_time: chrono::DateTime<Utc>) {
        let bus = self.bus.clone();
        let symbol = self.symbol.clone();
        tokio::spawn(async move {
            let _ = bus.publish_market(MarketEvent {
                exchange_time,
                local_time: Utc::now(),
                symbol,
                payload: MarketPayload::L2Update(update),
            }).await;
        });
    }
}
