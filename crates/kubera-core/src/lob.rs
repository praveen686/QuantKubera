//! # Deterministic Limit Order Book (LOB)
//!
//! A minimal, deterministic order book implementation for backtesting and simulation.
//!
//! ## Design Goals
//! - **Determinism**: BTreeMap ensures identical iteration order across runs
//! - **Scaled Integers**: Prices and quantities stored as mantissas to avoid float drift
//! - **Gap Detection**: Update IDs track sequence for detecting missing data
//!
//! ## Architecture
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                    OrderBook                         │
//! │  ┌─────────────────┐   ┌─────────────────┐          │
//! │  │   Bids (BTree)  │   │   Asks (BTree)  │          │
//! │  │  price → qty    │   │  price → qty    │          │
//! │  │  (descending)   │   │  (ascending)    │          │
//! │  └─────────────────┘   └─────────────────┘          │
//! │                                                      │
//! │  price_exponent: i8    qty_exponent: i8             │
//! │  last_update_id: u64   tradingsymbol: String        │
//! └─────────────────────────────────────────────────────┘
//! ```

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
// Import from models (leaf crate) - breaks kubera-core -> kubera-options cycle
use kubera_models::DepthEvent;

/// Error types for order book operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LobError {
    /// Gap detected in update sequence.
    /// Contains (expected_first_update_id, actual_first_update_id).
    SequenceGap { expected: u64, actual: u64 },
    /// Price exponent mismatch.
    PriceExponentMismatch { book: i8, event: i8 },
    /// Quantity exponent mismatch.
    QtyExponentMismatch { book: i8, event: i8 },
    /// Symbol mismatch.
    SymbolMismatch { book: String, event: String },
}

impl std::fmt::Display for LobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LobError::SequenceGap { expected, actual } => {
                write!(f, "Sequence gap: expected first_update_id={}, got {}", expected, actual)
            }
            LobError::PriceExponentMismatch { book, event } => {
                write!(f, "Price exponent mismatch: book={}, event={}", book, event)
            }
            LobError::QtyExponentMismatch { book, event } => {
                write!(f, "Qty exponent mismatch: book={}, event={}", book, event)
            }
            LobError::SymbolMismatch { book, event } => {
                write!(f, "Symbol mismatch: book={}, event={}", book, event)
            }
        }
    }
}

impl std::error::Error for LobError {}

/// A single price level delta for applying to the order book.
#[derive(Debug, Clone, Copy)]
pub struct LevelDelta {
    /// Price mantissa.
    pub price: i64,
    /// Quantity mantissa (0 = remove level).
    pub qty: i64,
}

/// Deterministic Limit Order Book.
///
/// Uses BTreeMap for deterministic iteration order. Prices and quantities
/// are stored as scaled integers (mantissas) to avoid floating-point drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    /// Trading symbol (e.g., "BTCUSDT").
    tradingsymbol: String,
    /// Bid levels: price mantissa → quantity mantissa.
    /// Higher prices are better (BTreeMap gives ascending order, we iterate in reverse).
    bids: BTreeMap<i64, i64>,
    /// Ask levels: price mantissa → quantity mantissa.
    /// Lower prices are better (BTreeMap gives ascending order, we iterate forward).
    asks: BTreeMap<i64, i64>,
    /// Last processed update ID for gap detection.
    last_update_id: u64,
    /// Price exponent: actual_price = mantissa * 10^price_exponent.
    price_exponent: i8,
    /// Quantity exponent: actual_qty = mantissa * 10^qty_exponent.
    qty_exponent: i8,
}

impl OrderBook {
    /// Creates a new empty order book.
    ///
    /// # Parameters
    /// * `tradingsymbol` - The trading pair symbol (e.g., "BTCUSDT")
    /// * `price_exponent` - Exponent for price conversion (e.g., -2 for 2 decimal places)
    /// * `qty_exponent` - Exponent for quantity conversion
    pub fn new(tradingsymbol: String, price_exponent: i8, qty_exponent: i8) -> Self {
        Self {
            tradingsymbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            price_exponent,
            qty_exponent,
        }
    }

    /// Returns the trading symbol.
    pub fn tradingsymbol(&self) -> &str {
        &self.tradingsymbol
    }

    /// Returns the last processed update ID.
    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    /// Returns the price exponent.
    pub fn price_exponent(&self) -> i8 {
        self.price_exponent
    }

    /// Returns the quantity exponent.
    pub fn qty_exponent(&self) -> i8 {
        self.qty_exponent
    }

    /// Applies a batch of level deltas to the order book.
    ///
    /// # Semantics
    /// - qty > 0: Set the level to the new quantity
    /// - qty = 0: Remove the level entirely
    ///
    /// # Gap Detection
    /// If `first_update_id` is provided and doesn't match `last_update_id + 1`,
    /// returns an error indicating missing data.
    ///
    /// # Parameters
    /// * `bids` - Bid level deltas
    /// * `asks` - Ask level deltas
    /// * `first_update_id` - First update ID in this batch (for gap detection)
    /// * `last_update_id` - Last update ID in this batch
    pub fn apply_delta(
        &mut self,
        bids: &[LevelDelta],
        asks: &[LevelDelta],
        first_update_id: u64,
        last_update_id: u64,
    ) -> Result<(), LobError> {
        // Gap detection: first_update_id must equal last_update_id + 1
        // Exception: first update (last_update_id == 0)
        if self.last_update_id > 0 && first_update_id != self.last_update_id + 1 {
            return Err(LobError::SequenceGap {
                expected: self.last_update_id + 1,
                actual: first_update_id,
            });
        }

        // Apply bid deltas
        for delta in bids {
            if delta.qty == 0 {
                self.bids.remove(&delta.price);
            } else {
                self.bids.insert(delta.price, delta.qty);
            }
        }

        // Apply ask deltas
        for delta in asks {
            if delta.qty == 0 {
                self.asks.remove(&delta.price);
            } else {
                self.asks.insert(delta.price, delta.qty);
            }
        }

        self.last_update_id = last_update_id;
        Ok(())
    }

    /// Applies a delta without gap checking (for initial snapshot or testing).
    pub fn apply_delta_unchecked(
        &mut self,
        bids: &[LevelDelta],
        asks: &[LevelDelta],
        last_update_id: u64,
    ) {
        for delta in bids {
            if delta.qty == 0 {
                self.bids.remove(&delta.price);
            } else {
                self.bids.insert(delta.price, delta.qty);
            }
        }

        for delta in asks {
            if delta.qty == 0 {
                self.asks.remove(&delta.price);
            } else {
                self.asks.insert(delta.price, delta.qty);
            }
        }

        self.last_update_id = last_update_id;
    }

    /// Applies a DepthEvent from the replay module.
    ///
    /// This is the primary integration point between L2 replay events and the order book.
    /// Validates symbol match and exponent consistency, performs gap detection.
    ///
    /// # Errors
    /// - `LobError::SymbolMismatch` if the event symbol doesn't match the book
    /// - `LobError::PriceExponentMismatch` if price exponents don't match
    /// - `LobError::QtyExponentMismatch` if qty exponents don't match
    /// - `LobError::SequenceGap` if update IDs indicate missing data
    pub fn apply_depth_event(&mut self, event: &DepthEvent) -> Result<(), LobError> {
        // Validate symbol match
        if event.tradingsymbol != self.tradingsymbol {
            return Err(LobError::SymbolMismatch {
                book: self.tradingsymbol.clone(),
                event: event.tradingsymbol.clone(),
            });
        }

        // Validate exponent consistency
        if event.price_exponent != self.price_exponent {
            return Err(LobError::PriceExponentMismatch {
                book: self.price_exponent,
                event: event.price_exponent,
            });
        }
        if event.qty_exponent != self.qty_exponent {
            return Err(LobError::QtyExponentMismatch {
                book: self.qty_exponent,
                event: event.qty_exponent,
            });
        }

        // Convert DepthLevels to LevelDeltas
        let bid_deltas: Vec<LevelDelta> = event.bids.iter()
            .map(|l| LevelDelta { price: l.price, qty: l.qty })
            .collect();
        let ask_deltas: Vec<LevelDelta> = event.asks.iter()
            .map(|l| LevelDelta { price: l.price, qty: l.qty })
            .collect();

        // Apply with gap detection
        self.apply_delta(
            &bid_deltas,
            &ask_deltas,
            event.first_update_id,
            event.last_update_id,
        )
    }

    /// Applies a DepthEvent without validation (for initial snapshots or testing).
    pub fn apply_depth_event_unchecked(&mut self, event: &DepthEvent) {
        let bid_deltas: Vec<LevelDelta> = event.bids.iter()
            .map(|l| LevelDelta { price: l.price, qty: l.qty })
            .collect();
        let ask_deltas: Vec<LevelDelta> = event.asks.iter()
            .map(|l| LevelDelta { price: l.price, qty: l.qty })
            .collect();

        self.apply_delta_unchecked(&bid_deltas, &ask_deltas, event.last_update_id);
    }

    /// Returns the best bid (highest price) as (price_mantissa, qty_mantissa).
    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.iter().next_back().map(|(&p, &q)| (p, q))
    }

    /// Returns the best ask (lowest price) as (price_mantissa, qty_mantissa).
    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.iter().next().map(|(&p, &q)| (p, q))
    }

    /// Returns the best bid price as f64.
    pub fn best_bid_f64(&self) -> Option<f64> {
        self.best_bid().map(|(p, _)| self.price_to_f64(p))
    }

    /// Returns the best ask price as f64.
    pub fn best_ask_f64(&self) -> Option<f64> {
        self.best_ask().map(|(p, _)| self.price_to_f64(p))
    }

    /// Returns the mid price as f64.
    pub fn mid_f64(&self) -> Option<f64> {
        match (self.best_bid_f64(), self.best_ask_f64()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    /// Returns the spread in price units (as f64).
    pub fn spread_f64(&self) -> Option<f64> {
        match (self.best_bid_f64(), self.best_ask_f64()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Converts a price mantissa to f64.
    #[inline]
    pub fn price_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.price_exponent as i32)
    }

    /// Converts a quantity mantissa to f64.
    #[inline]
    pub fn qty_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.qty_exponent as i32)
    }

    /// Converts an f64 price to mantissa.
    #[inline]
    pub fn f64_to_price(&self, price: f64) -> i64 {
        (price / 10f64.powi(self.price_exponent as i32)).round() as i64
    }

    /// Converts an f64 quantity to mantissa.
    #[inline]
    pub fn f64_to_qty(&self, qty: f64) -> i64 {
        (qty / 10f64.powi(self.qty_exponent as i32)).round() as i64
    }

    /// Returns all bid levels in descending price order (best first).
    pub fn bids_desc(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.bids.iter().rev().map(|(&p, &q)| (p, q))
    }

    /// Returns all ask levels in ascending price order (best first).
    pub fn asks_asc(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.asks.iter().map(|(&p, &q)| (p, q))
    }

    /// Simulates a market buy order, walking through ask levels.
    ///
    /// Returns (total_cost, total_qty_filled, avg_price) where:
    /// - total_cost: Sum of (price * qty) for each level touched
    /// - total_qty_filled: Sum of quantities filled
    /// - avg_price: total_cost / total_qty_filled
    ///
    /// Does NOT modify the book state.
    pub fn simulate_market_buy(&self, qty_mantissa: i64) -> Option<(f64, f64, f64)> {
        let mut remaining = qty_mantissa;
        let mut total_cost = 0i128;
        let mut total_filled = 0i64;

        for (&price, &level_qty) in self.asks.iter() {
            if remaining <= 0 {
                break;
            }
            let fill = remaining.min(level_qty);
            total_cost += (price as i128) * (fill as i128);
            total_filled += fill;
            remaining -= fill;
        }

        if total_filled == 0 {
            return None;
        }

        let cost_f64 = (total_cost as f64) * 10f64.powi((self.price_exponent + self.qty_exponent) as i32);
        let filled_f64 = self.qty_to_f64(total_filled);
        let avg_price = cost_f64 / filled_f64;

        Some((cost_f64, filled_f64, avg_price))
    }

    /// Simulates a market sell order, walking through bid levels.
    ///
    /// Returns (total_proceeds, total_qty_filled, avg_price).
    ///
    /// Does NOT modify the book state.
    pub fn simulate_market_sell(&self, qty_mantissa: i64) -> Option<(f64, f64, f64)> {
        let mut remaining = qty_mantissa;
        let mut total_proceeds = 0i128;
        let mut total_filled = 0i64;

        // Walk bids from highest to lowest
        for (&price, &level_qty) in self.bids.iter().rev() {
            if remaining <= 0 {
                break;
            }
            let fill = remaining.min(level_qty);
            total_proceeds += (price as i128) * (fill as i128);
            total_filled += fill;
            remaining -= fill;
        }

        if total_filled == 0 {
            return None;
        }

        let proceeds_f64 = (total_proceeds as f64) * 10f64.powi((self.price_exponent + self.qty_exponent) as i32);
        let filled_f64 = self.qty_to_f64(total_filled);
        let avg_price = proceeds_f64 / filled_f64;

        Some((proceeds_f64, filled_f64, avg_price))
    }

    /// Returns the number of bid levels.
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    /// Returns the number of ask levels.
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    /// Clears all levels from the book (preserves exponents and symbol).
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        // Apply initial snapshot
        let bids = vec![
            LevelDelta { price: 9000000, qty: 100000000 }, // 90000.00 @ 1.0
            LevelDelta { price: 8999000, qty: 200000000 }, // 89990.00 @ 2.0
        ];
        let asks = vec![
            LevelDelta { price: 9001000, qty: 50000000 },  // 90010.00 @ 0.5
            LevelDelta { price: 9002000, qty: 100000000 }, // 90020.00 @ 1.0
        ];

        book.apply_delta_unchecked(&bids, &asks, 100);

        assert_eq!(book.best_bid(), Some((9000000, 100000000)));
        assert_eq!(book.best_ask(), Some((9001000, 50000000)));
        assert!((book.best_bid_f64().unwrap() - 90000.0).abs() < 0.01);
        assert!((book.best_ask_f64().unwrap() - 90010.0).abs() < 0.01);
    }

    #[test]
    fn test_level_removal() {
        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        let bids = vec![LevelDelta { price: 9000000, qty: 100000000 }];
        let asks = vec![LevelDelta { price: 9001000, qty: 50000000 }];
        book.apply_delta_unchecked(&bids, &asks, 100);

        // Remove the bid level
        let remove_bids = vec![LevelDelta { price: 9000000, qty: 0 }];
        book.apply_delta(&remove_bids, &[], 101, 101).unwrap();

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.bid_depth(), 0);
    }

    #[test]
    fn test_gap_detection() {
        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        let bids = vec![LevelDelta { price: 9000000, qty: 100000000 }];
        book.apply_delta_unchecked(&bids, &[], 100);

        // Try to apply update with gap
        let result = book.apply_delta(&[], &[], 102, 102);
        assert!(matches!(result, Err(LobError::SequenceGap { expected: 101, actual: 102 })));
    }

    #[test]
    fn test_simulate_market_buy() {
        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        // Set up asks: 90010 @ 0.5, 90020 @ 1.0
        let asks = vec![
            LevelDelta { price: 9001000, qty: 50000000 },  // 90010.00 @ 0.5
            LevelDelta { price: 9002000, qty: 100000000 }, // 90020.00 @ 1.0
        ];
        book.apply_delta_unchecked(&[], &asks, 100);

        // Buy 0.3 BTC (should fill entirely at first level)
        let (_cost, filled, avg) = book.simulate_market_buy(30000000).unwrap();
        assert!((filled - 0.3).abs() < 0.0001);
        assert!((avg - 90010.0).abs() < 0.01);

        // Buy 1.0 BTC (should span both levels)
        let (_cost, filled, avg) = book.simulate_market_buy(100000000).unwrap();
        assert!((filled - 1.0).abs() < 0.0001);
        // 0.5 @ 90010 + 0.5 @ 90020 = 45005 + 45010 = 90015 avg
        assert!((avg - 90015.0).abs() < 0.01);
    }

    #[test]
    fn test_deterministic_iteration() {
        let mut book1 = OrderBook::new("BTCUSDT".to_string(), -2, -8);
        let mut book2 = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        // Apply same updates in same order
        let bids = vec![
            LevelDelta { price: 100, qty: 10 },
            LevelDelta { price: 200, qty: 20 },
            LevelDelta { price: 150, qty: 15 },
        ];

        book1.apply_delta_unchecked(&bids, &[], 1);
        book2.apply_delta_unchecked(&bids, &[], 1);

        // Verify identical iteration order
        let levels1: Vec<_> = book1.bids_desc().collect();
        let levels2: Vec<_> = book2.bids_desc().collect();

        assert_eq!(levels1, levels2);
        assert_eq!(levels1, vec![(200, 20), (150, 15), (100, 10)]);
    }

    #[test]
    fn test_apply_depth_event() {
        use chrono::Utc;
        use kubera_models::{DepthLevel, IntegrityTier};

        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        // Create a DepthEvent
        let event = DepthEvent {
            ts: Utc::now(),
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 1,
            last_update_id: 1,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![
                DepthLevel { price: 9000000, qty: 100000000 },
            ],
            asks: vec![
                DepthLevel { price: 9001000, qty: 50000000 },
            ],
            is_snapshot: false,
            integrity_tier: IntegrityTier::Certified,
            source: None,
        };

        book.apply_depth_event(&event).unwrap();

        assert_eq!(book.best_bid(), Some((9000000, 100000000)));
        assert_eq!(book.best_ask(), Some((9001000, 50000000)));
    }

    #[test]
    fn test_depth_event_symbol_mismatch() {
        use chrono::Utc;
        use kubera_models::IntegrityTier;

        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        let event = DepthEvent {
            ts: Utc::now(),
            tradingsymbol: "ETHUSDT".to_string(), // Wrong symbol
            first_update_id: 1,
            last_update_id: 1,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![],
            asks: vec![],
            is_snapshot: false,
            integrity_tier: IntegrityTier::Certified,
            source: None,
        };

        let result = book.apply_depth_event(&event);
        assert!(matches!(result, Err(LobError::SymbolMismatch { .. })));
    }

    #[test]
    fn test_depth_event_exponent_mismatch() {
        use chrono::Utc;
        use kubera_models::IntegrityTier;

        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        let event = DepthEvent {
            ts: Utc::now(),
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 1,
            last_update_id: 1,
            price_exponent: -3, // Wrong exponent
            qty_exponent: -8,
            bids: vec![],
            asks: vec![],
            is_snapshot: false,
            integrity_tier: IntegrityTier::Certified,
            source: None,
        };

        let result = book.apply_depth_event(&event);
        assert!(matches!(result, Err(LobError::PriceExponentMismatch { .. })));
    }

    #[test]
    fn test_depth_event_gap_detection() {
        use chrono::Utc;
        use kubera_models::{DepthLevel, IntegrityTier};

        let mut book = OrderBook::new("BTCUSDT".to_string(), -2, -8);

        // First event
        let event1 = DepthEvent {
            ts: Utc::now(),
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 100,
            last_update_id: 100,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![DepthLevel { price: 9000000, qty: 100000000 }],
            asks: vec![],
            is_snapshot: false,
            integrity_tier: IntegrityTier::Certified,
            source: None,
        };
        book.apply_depth_event_unchecked(&event1);

        // Second event with gap (should be 101, but is 103)
        let event2 = DepthEvent {
            ts: Utc::now(),
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 103, // Gap!
            last_update_id: 103,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![],
            asks: vec![],
            is_snapshot: false,
            integrity_tier: IntegrityTier::Certified,
            source: None,
        };

        let result = book.apply_depth_event(&event2);
        assert!(matches!(result, Err(LobError::SequenceGap { expected: 101, actual: 103 })));
    }
}
