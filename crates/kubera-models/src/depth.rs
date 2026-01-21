//! # Deterministic Depth Types for L2 Replay
//!
//! These types use scaled integers (mantissas) to avoid floating-point drift
//! and ensure deterministic replay across runs.
//!
//! ## Design Goals
//! - **Determinism**: All prices/quantities stored as integer mantissas
//! - **Gap Detection**: Update IDs enable sequence validation
//! - **Bootstrap Support**: Snapshot vs diff distinction for proper book initialization
//!
//! ## Mantissa Convention
//! ```text
//! actual_value = mantissa * 10^exponent
//! ```
//! For example, with `price_exponent = -2`:
//! - mantissa `9000012` represents price `90000.12`

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
/// - Gaps indicate missing data and should cause replay to HARD FAIL
///
/// ## Bootstrap Protocol (SBE)
/// - First event should have `is_snapshot = true` (from REST snapshot)
/// - Subsequent events are diffs with `is_snapshot = false`
/// - Snapshot sets initial book state; diffs are applied incrementally
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
    /// True if this is a full snapshot (bootstrap), false if it's a diff update.
    /// Snapshots replace the entire book; diffs are applied incrementally.
    #[serde(default)]
    pub is_snapshot: bool,
}

impl DepthEvent {
    /// Convert a price mantissa to f64.
    #[inline]
    pub fn price_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.price_exponent as i32)
    }

    /// Convert a quantity mantissa to f64.
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

    /// Returns a sort key for total ordering within depth events.
    ///
    /// ## Ordering Priority
    /// 1. Timestamp (primary)
    /// 2. Snapshot rank: snapshot=0, diff=1 (snapshots before diffs)
    /// 3. first_update_id (for diff ordering)
    /// 4. Symbol (alphabetical tie-breaker)
    pub fn sort_key(&self) -> (DateTime<Utc>, u8, u64, &str) {
        let kind_rank = if self.is_snapshot { 0 } else { 1 };
        (self.ts, kind_rank, self.first_update_id, &self.tradingsymbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_depth_level_conversion() {
        let level = DepthLevel { price: 9000012, qty: 150000000 };
        let (price, qty) = level.to_f64(-2, -8);
        assert!((price - 90000.12).abs() < 0.001);
        assert!((qty - 1.5).abs() < 0.0001);
    }

    #[test]
    fn test_depth_level_from_f64() {
        let level = DepthLevel::from_f64(90000.12, 1.5, -2, -8);
        assert_eq!(level.price, 9000012);
        assert_eq!(level.qty, 150000000);
    }

    #[test]
    fn test_depth_event_best_prices() {
        let event = DepthEvent {
            ts: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 1,
            last_update_id: 1,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![
                DepthLevel { price: 9000000, qty: 100000000 },
                DepthLevel { price: 8999500, qty: 200000000 },
            ],
            asks: vec![
                DepthLevel { price: 9000100, qty: 100000000 },
                DepthLevel { price: 9000500, qty: 200000000 },
            ],
            is_snapshot: true,
        };

        let best_bid = event.best_bid_f64().unwrap();
        let best_ask = event.best_ask_f64().unwrap();

        assert!((best_bid - 90000.0).abs() < 0.01);
        assert!((best_ask - 90001.0).abs() < 0.01);
    }

    #[test]
    fn test_sort_key_ordering() {
        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

        let snapshot = DepthEvent {
            ts,
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 100,
            last_update_id: 100,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![],
            asks: vec![],
            is_snapshot: true,
        };

        let diff = DepthEvent {
            ts,
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 101,
            last_update_id: 101,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![],
            asks: vec![],
            is_snapshot: false,
        };

        // Snapshot should sort before diff at same timestamp
        assert!(snapshot.sort_key() < diff.sort_key());
    }
}
