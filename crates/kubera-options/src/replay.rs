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
//! - `DepthEvent`: L2 order book delta (from `kubera_models::depth`)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Re-export deterministic depth types from the models crate (canonical location)
pub use kubera_models::{DepthEvent, DepthLevel};

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
