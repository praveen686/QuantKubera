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
//! - `DepthEvent`: L2 order book delta (for realistic depth-aware execution)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Best bid/ask quote snapshot for an individual instrument (L1).
///
/// All prices are in quote currency (e.g., rupees, USDT) and quantities are
/// in base units. This is the legacy format for backward compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteEvent {
    pub ts: DateTime<Utc>,
    pub tradingsymbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: u32,
    pub ask_qty: u32,
}

/// A single price level in the order book (scaled integers for determinism).
///
/// Prices and quantities are stored as mantissas. The actual value is:
/// `value = mantissa * 10^exponent` where exponent comes from the parent DepthEvent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DepthLevel {
    /// Price mantissa (multiply by 10^price_exponent to get actual price)
    pub price: i64,
    /// Quantity mantissa (multiply by 10^qty_exponent to get actual quantity)
    pub qty: i64,
}

impl DepthLevel {
    /// Convert to f64 values using the given exponents.
    #[inline]
    pub fn to_f64(&self, price_exp: i8, qty_exp: i8) -> (f64, f64) {
        let price = self.price as f64 * 10f64.powi(price_exp as i32);
        let qty = self.qty as f64 * 10f64.powi(qty_exp as i32);
        (price, qty)
    }

    /// Create from f64 values using the given exponents.
    #[inline]
    pub fn from_f64(price: f64, qty: f64, price_exp: i8, qty_exp: i8) -> Self {
        let price_mantissa = (price / 10f64.powi(price_exp as i32)).round() as i64;
        let qty_mantissa = (qty / 10f64.powi(qty_exp as i32)).round() as i64;
        Self { price: price_mantissa, qty: qty_mantissa }
    }
}

/// Order book depth update event (L2).
///
/// This represents a batch of price level changes from the exchange.
/// All values use scaled integers for deterministic replay.
///
/// ## Update ID Semantics
/// - `first_update_id` and `last_update_id` define the update range
/// - For gap detection: `first_update_id` must equal `previous.last_update_id + 1`
/// - Gaps indicate missing data and should cause replay to fail
///
/// ## Level Semantics
/// - A level with `qty = 0` means remove that price level
/// - A level with `qty > 0` means set that price level to the new quantity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthEvent {
    /// Event timestamp (UTC)
    pub ts: DateTime<Utc>,
    /// Trading symbol (e.g., "BTCUSDT")
    pub tradingsymbol: String,
    /// First update ID in this batch (for gap detection)
    pub first_update_id: u64,
    /// Last update ID in this batch (for gap detection)
    pub last_update_id: u64,
    /// Price exponent: actual_price = mantissa * 10^price_exponent
    pub price_exponent: i8,
    /// Quantity exponent: actual_qty = mantissa * 10^qty_exponent
    pub qty_exponent: i8,
    /// Bid level updates (price descending order expected but not enforced here)
    pub bids: Vec<DepthLevel>,
    /// Ask level updates (price ascending order expected but not enforced here)
    pub asks: Vec<DepthLevel>,
}

impl DepthEvent {
    /// Convert a DepthLevel's price mantissa to f64.
    #[inline]
    pub fn price_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.price_exponent as i32)
    }

    /// Convert a DepthLevel's quantity mantissa to f64.
    #[inline]
    pub fn qty_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.qty_exponent as i32)
    }

    /// Get best bid price as f64 (None if no bids).
    pub fn best_bid_f64(&self) -> Option<f64> {
        self.bids.iter()
            .filter(|l| l.qty > 0)
            .map(|l| self.price_to_f64(l.price))
            .max_by(|a, b| a.partial_cmp(b).unwrap())
    }

    /// Get best ask price as f64 (None if no asks).
    pub fn best_ask_f64(&self) -> Option<f64> {
        self.asks.iter()
            .filter(|l| l.qty > 0)
            .map(|l| self.price_to_f64(l.price))
            .min_by(|a, b| a.partial_cmp(b).unwrap())
    }
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
    /// Creates a feed from events. The feed **sorts events by timestamp** to
    /// enforce causality in the simulator.
    pub fn new(mut events: Vec<ReplayEvent>) -> Self {
        events.sort_by_key(|e| e.ts());
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
