//! # Deterministic Replay Types (Options + Spot)
//!
//! This module defines **minimal, deterministic** event types used to replay
//! historical data for execution testing.
//!
//! Design goals:
//! - Single time axis (UTC timestamps)
//! - Quote-driven (L1) and depth-driven (L2) execution modes
//! - Deterministic iteration for reproducible backtests
//! - Scaled integer representation for prices/quantities (no float drift)
//!
//! ## Event Types
//! - `QuoteEvent`: L1 best bid/ask snapshot (legacy, for backward compatibility)
//! - `QuoteEventV2`: Full Z-Phase spec with depth-5 and audit trail
//! - `DepthEvent`: L2 order book delta (from `kubera_models::depth`)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

// Re-export deterministic depth types from the models crate (canonical location)
pub use kubera_models::{DepthEvent, DepthLevel};

/// Global event ID counter for monotonic event_id generation.
static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);

/// Quote integrity tier for audit trail.
/// Per Z-Phase spec section 1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum QuoteIntegrity {
    /// Binance SBE only - exchange-grade L2
    CertifiedL2,
    /// Zerodha FULL mode: depth-5
    #[default]
    QuoteGradeD5,
    /// LTP + best only fallback
    QuoteGradeL1,
    /// Post-processed datasets; explicitly watermarked
    NonCertifiedTransformed,
}

impl QuoteIntegrity {
    /// Returns true if this tier is production-grade (not transformed).
    pub fn is_certified(&self) -> bool {
        matches!(self, QuoteIntegrity::CertifiedL2 | QuoteIntegrity::QuoteGradeD5)
    }
}

/// Bitfield flags for quote event metadata.
/// Per Z-Phase spec section 3.1.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuoteFlags {
    /// Depth data was missing, synthetic fallback used
    pub missing_depth: bool,
    /// Quantity was synthetic (not from order book)
    pub synthetic_qty: bool,
    /// Spread was synthetic (derived from LTP)
    pub synthetic_spread: bool,
    /// Quote is stale (exceeded max_quote_age_ms)
    pub stale: bool,
}

impl QuoteFlags {
    /// Returns true if any synthetic fallback was used.
    pub fn any_synthetic(&self) -> bool {
        self.missing_depth || self.synthetic_qty || self.synthetic_spread
    }
}

/// Single depth level: price + quantity.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct DepthLevelF64 {
    pub price: f64,
    pub qty: u32,
}

/// Full Z-Phase quote event with depth-5 and complete audit trail.
/// Per Z-Phase spec section 3.1.
///
/// This is the authoritative event type for Zerodha options trading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteEventV2 {
    /// Monotonic event ID within file (for ordering and dedup).
    pub event_id: u64,
    /// Exchange timestamp (if provided by exchange; null otherwise).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_exchange: Option<DateTime<Utc>>,
    /// Capture receive timestamp (wallclock when data arrived).
    pub ts_recv: DateTime<Utc>,
    /// Replay timestamp used by execution engine.
    pub ts_event: DateTime<Utc>,
    /// Data source identifier.
    pub source: String,
    /// Integrity tier per Z-Phase spec.
    pub integrity_tier: QuoteIntegrity,
    /// SHA256 hash of manifest + chunk for reproducibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
    /// Trading symbol (e.g., "BANKNIFTY26JAN59000CE").
    pub symbol: String,
    /// Zerodha instrument token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument_token: Option<u32>,
    /// Last traded price.
    pub ltp: f64,
    /// Bid depth (up to 5 levels). Index 0 = best bid.
    pub bids: Vec<DepthLevelF64>,
    /// Ask depth (up to 5 levels). Index 0 = best ask.
    pub asks: Vec<DepthLevelF64>,
    /// True if any synthetic fallback was used.
    pub is_synthetic: bool,
    /// Detailed flags for audit trail.
    pub flags: QuoteFlags,
}

impl QuoteEventV2 {
    /// Generate next monotonic event ID.
    pub fn next_event_id() -> u64 {
        NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// Best bid price (level 0).
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }

    /// Best ask price (level 0).
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }

    /// Mid price = (best_bid + best_ask) / 2.
    pub fn mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / 2.0),
            _ => None,
        }
    }

    /// Spread in basis points.
    pub fn spread_bps(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask(), self.mid()) {
            (Some(b), Some(a), Some(m)) if m > 0.0 => Some((a - b) / m * 10_000.0),
            _ => None,
        }
    }

    /// Microprice = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty).
    /// More accurate fair value estimate when imbalanced.
    pub fn microprice(&self) -> Option<f64> {
        let bid = self.bids.first()?;
        let ask = self.asks.first()?;
        let total_qty = bid.qty + ask.qty;
        if total_qty == 0 {
            return self.mid();
        }
        Some((bid.price * ask.qty as f64 + ask.price * bid.qty as f64) / total_qty as f64)
    }

    /// Top-of-book imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty).
    /// Range: [-1, 1]. Positive = more bids, negative = more asks.
    pub fn imbalance_top1(&self) -> Option<f64> {
        let bid = self.bids.first()?;
        let ask = self.asks.first()?;
        let total = bid.qty + ask.qty;
        if total == 0 {
            return Some(0.0);
        }
        Some((bid.qty as f64 - ask.qty as f64) / total as f64)
    }

    /// Top-5 imbalance using all depth levels.
    pub fn imbalance_top5(&self) -> f64 {
        let bid_qty: u32 = self.bids.iter().map(|l| l.qty).sum();
        let ask_qty: u32 = self.asks.iter().map(|l| l.qty).sum();
        let total = bid_qty + ask_qty;
        if total == 0 {
            return 0.0;
        }
        (bid_qty as f64 - ask_qty as f64) / total as f64
    }

    /// Convert to legacy QuoteEvent for backward compatibility.
    pub fn to_legacy(&self) -> QuoteEvent {
        QuoteEvent {
            ts: self.ts_event,
            tradingsymbol: self.symbol.clone(),
            bid: self.best_bid().unwrap_or(0.0),
            ask: self.best_ask().unwrap_or(0.0),
            bid_qty: self.bids.first().map(|l| l.qty).unwrap_or(0),
            ask_qty: self.asks.first().map(|l| l.qty).unwrap_or(0),
            source: Some(self.source.clone()),
            integrity: Some(self.integrity_tier),
            is_synthetic: Some(self.is_synthetic),
        }
    }
}

/// Best bid/ask quote snapshot for an individual instrument (L1).
///
/// All prices are in quote currency (e.g., rupees, USDT) and quantities are
/// in base units.
///
/// ## Integrity Watermarks (Z0 Certification)
/// - `source`: Data origin (e.g., "ZERODHA_WS", "BINANCE_BOOKTICKER")
/// - `integrity`: Quote grade (real depth vs synthetic fallback)
/// - `is_synthetic`: True if bid/ask/qty are synthetic (not from real depth)
///
/// These fields enable "no silent degradation" auditing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteEvent {
    /// Capture timestamp (UTC). Note: This is capture wallclock time,
    /// not exchange timestamp. Determinism is "file deterministic."
    pub ts: DateTime<Utc>,
    pub tradingsymbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: u32,
    pub ask_qty: u32,
    /// Data source identifier (e.g., "ZERODHA_WS", "BINANCE_BOOKTICKER")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Quote integrity tier for audit trail
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integrity: Option<QuoteIntegrity>,
    /// True if bid/ask/qty are synthetic (LTP fallback or transformed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_synthetic: Option<bool>,
}

/// Replay event stream.
///
/// Supports both L1 quotes (backward compatible) and L2 depth updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ReplayEvent {
    /// L1 best bid/ask quote
    Quote(QuoteEvent),
    /// L2 order book depth update
    Depth(DepthEvent),
}

impl ReplayEvent {
    pub fn ts(&self) -> DateTime<Utc> {
        match self {
            ReplayEvent::Quote(q) => q.ts,
            ReplayEvent::Depth(d) => d.ts,
        }
    }

    /// Returns the trading symbol for this event.
    pub fn tradingsymbol(&self) -> &str {
        match self {
            ReplayEvent::Quote(q) => &q.tradingsymbol,
            ReplayEvent::Depth(d) => &d.tradingsymbol,
        }
    }

    /// Returns a sort key for total ordering.
    ///
    /// ## Ordering Priority
    /// 1. Timestamp (primary)
    /// 2. Event kind rank: Depth(snapshot=true)=0, Depth(diff)=1, Quote=2
    /// 3. For Depth events: first_update_id
    /// 4. For ties: symbol (alphabetical)
    ///
    /// This ensures deterministic ordering even when timestamps collide
    /// (common at SBE rates).
    pub fn sort_key(&self) -> (DateTime<Utc>, u8, u64, String) {
        match self {
            ReplayEvent::Depth(d) => {
                let kind_rank = if d.is_snapshot { 0 } else { 1 };
                (d.ts, kind_rank, d.first_update_id, d.tradingsymbol.clone())
            }
            ReplayEvent::Quote(q) => {
                // Quotes have no update_id; use 0
                (q.ts, 2, 0, q.tradingsymbol.clone())
            }
        }
    }
}

/// A deterministic replay feed.
///
/// This is intentionally small. In production you may want a packed binary
/// format (Parquet/Arrow/FlatBuffers) with memory mapping.
pub struct ReplayFeed {
    events: Vec<ReplayEvent>,
    idx: usize,
}

impl ReplayFeed {
    /// Creates a feed from events. The feed **sorts events by total ordering key**
    /// to enforce causality and determinism in the simulator.
    ///
    /// ## Sort Key (Total Ordering)
    /// 1. Timestamp (primary)
    /// 2. Event kind rank: Depth(snapshot)=0, Depth(diff)=1, Quote=2
    /// 3. For Depth events: first_update_id
    /// 4. Symbol (alphabetical tie-breaker)
    ///
    /// This ensures identical ordering across runs even when timestamps collide.
    pub fn new(mut events: Vec<ReplayEvent>) -> Self {
        events.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        Self { events, idx: 0 }
    }

    /// Returns the next event in timestamp order.
    pub fn next(&mut self) -> Option<ReplayEvent> {
        if self.idx >= self.events.len() {
            return None;
        }
        let e = self.events[self.idx].clone();
        self.idx += 1;
        Some(e)
    }

    /// Peek the next event without consuming it.
    pub fn peek(&self) -> Option<&ReplayEvent> {
        self.events.get(self.idx)
    }
}
