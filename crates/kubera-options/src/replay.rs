//! # Deterministic Replay Types (Options)
//!
//! This module defines **minimal, deterministic** event types used to replay
//! historical data for options execution testing without calling Zerodha.
//!
//! Design goals:
//! - Single time axis (UTC timestamps)
//! - Quote-driven execution (best bid/ask + sizes)
//! - Deterministic iteration for reproducible backtests

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Best bid/ask quote snapshot for an individual instrument.
///
/// All prices are in *rupees* and quantities are in *contracts* (lot-adjusted
/// quantities should be handled at a higher level).
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplayEvent {
    Quote(QuoteEvent),
}

impl ReplayEvent {
    pub fn ts(&self) -> DateTime<Utc> {
        match self {
            ReplayEvent::Quote(q) => q.ts,
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
