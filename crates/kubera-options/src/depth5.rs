//! # Depth5Book: Authoritative Quote-Grade Order Book
//!
//! Per Z-Phase spec section 6: Single authoritative book per instrument.
//!
//! We do not pretend full L2. We implement:
//! - Depth5Book per instrument
//! - Update with each QuoteEvent/QuoteEventV2
//! - Derived: best_bid, best_ask, mid, microprice, spread_bps, imbalance
//! - Staleness tracking per spec section 5.3
//!
//! This is sufficient for:
//! - Realistic execution simulation
//! - Microstructure features
//! - Options quoting sanity

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

use crate::replay::{DepthLevelF64, QuoteEventV2, QuoteIntegrity};

/// Configuration for staleness gating per Z-Phase spec section 5.3.
#[derive(Debug, Clone)]
pub struct StalenessConfig {
    /// Maximum quote age before considered stale (default: 2000ms).
    pub max_quote_age_ms: i64,
    /// If true, reject market orders on stale quotes.
    pub reject_on_stale: bool,
    /// Slippage multiplier when quote is stale (e.g., 2.0 = double slippage).
    pub stale_slippage_multiplier: f64,
}

impl Default for StalenessConfig {
    fn default() -> Self {
        Self {
            max_quote_age_ms: 2000,
            reject_on_stale: false,
            stale_slippage_multiplier: 2.0,
        }
    }
}

/// Single instrument's depth-5 book state.
#[derive(Debug, Clone)]
pub struct Depth5Book {
    /// Trading symbol.
    pub symbol: String,
    /// Instrument token (Zerodha).
    pub instrument_token: Option<u32>,
    /// Last traded price.
    pub ltp: f64,
    /// Bid depth (up to 5 levels). Index 0 = best bid.
    pub bids: Vec<DepthLevelF64>,
    /// Ask depth (up to 5 levels). Index 0 = best ask.
    pub asks: Vec<DepthLevelF64>,
    /// Last update timestamp.
    pub last_update: DateTime<Utc>,
    /// Integrity tier of last update.
    pub integrity_tier: QuoteIntegrity,
    /// True if last update was synthetic.
    pub is_synthetic: bool,
    /// Number of updates received.
    pub update_count: u64,
}

impl Depth5Book {
    /// Create a new empty book.
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            instrument_token: None,
            ltp: 0.0,
            bids: Vec::with_capacity(5),
            asks: Vec::with_capacity(5),
            last_update: Utc::now(),
            integrity_tier: QuoteIntegrity::QuoteGradeL1,
            is_synthetic: true,
            update_count: 0,
        }
    }

    /// Update book from QuoteEventV2.
    pub fn update(&mut self, event: &QuoteEventV2) {
        self.ltp = event.ltp;
        self.bids = event.bids.clone();
        self.asks = event.asks.clone();
        self.last_update = event.ts_event;
        self.integrity_tier = event.integrity_tier;
        self.is_synthetic = event.is_synthetic;
        self.instrument_token = event.instrument_token;
        self.update_count += 1;
    }

    /// Best bid price.
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().filter(|l| l.price > 0.0).map(|l| l.price)
    }

    /// Best ask price.
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().filter(|l| l.price > 0.0).map(|l| l.price)
    }

    /// Best bid quantity.
    pub fn best_bid_qty(&self) -> u32 {
        self.bids.first().map(|l| l.qty).unwrap_or(0)
    }

    /// Best ask quantity.
    pub fn best_ask_qty(&self) -> u32 {
        self.asks.first().map(|l| l.qty).unwrap_or(0)
    }

    /// Mid price = (best_bid + best_ask) / 2.
    pub fn mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / 2.0),
            _ => None,
        }
    }

    /// Spread = best_ask - best_bid.
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some(a - b),
            _ => None,
        }
    }

    /// Spread in basis points.
    pub fn spread_bps(&self) -> Option<f64> {
        match (self.spread(), self.mid()) {
            (Some(s), Some(m)) if m > 0.0 => Some(s / m * 10_000.0),
            _ => None,
        }
    }

    /// Microprice = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty).
    /// More accurate fair value estimate when imbalanced.
    pub fn microprice(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        let bid_qty = self.best_bid_qty();
        let ask_qty = self.best_ask_qty();
        let total_qty = bid_qty + ask_qty;
        if total_qty == 0 {
            return self.mid();
        }
        Some((bid * ask_qty as f64 + ask * bid_qty as f64) / total_qty as f64)
    }

    /// Top-of-book imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty).
    /// Range: [-1, 1]. Positive = more bids (buying pressure).
    pub fn imbalance_top1(&self) -> f64 {
        let bid_qty = self.best_bid_qty();
        let ask_qty = self.best_ask_qty();
        let total = bid_qty + ask_qty;
        if total == 0 {
            return 0.0;
        }
        (bid_qty as f64 - ask_qty as f64) / total as f64
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

    /// Check if book is stale based on current time.
    pub fn is_stale(&self, now: DateTime<Utc>, config: &StalenessConfig) -> bool {
        let age = now - self.last_update;
        age > Duration::milliseconds(config.max_quote_age_ms)
    }

    /// Get quote age in milliseconds.
    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        (now - self.last_update).num_milliseconds()
    }

    /// Total bid depth (sum of all levels).
    pub fn total_bid_depth(&self) -> u32 {
        self.bids.iter().map(|l| l.qty).sum()
    }

    /// Total ask depth (sum of all levels).
    pub fn total_ask_depth(&self) -> u32 {
        self.asks.iter().map(|l| l.qty).sum()
    }

    /// Volume-weighted average bid price (VWAP of bid side).
    pub fn vwap_bid(&self) -> Option<f64> {
        let total_qty: u32 = self.bids.iter().map(|l| l.qty).sum();
        if total_qty == 0 {
            return None;
        }
        let weighted_sum: f64 = self.bids.iter().map(|l| l.price * l.qty as f64).sum();
        Some(weighted_sum / total_qty as f64)
    }

    /// Volume-weighted average ask price (VWAP of ask side).
    pub fn vwap_ask(&self) -> Option<f64> {
        let total_qty: u32 = self.asks.iter().map(|l| l.qty).sum();
        if total_qty == 0 {
            return None;
        }
        let weighted_sum: f64 = self.asks.iter().map(|l| l.price * l.qty as f64).sum();
        Some(weighted_sum / total_qty as f64)
    }
}

/// Market state manager: holds all Depth5Books.
/// Per Z-Phase spec section 2.1 "Market State".
#[derive(Debug, Default)]
pub struct MarketState {
    /// Per-instrument depth books.
    books: HashMap<String, Depth5Book>,
    /// Staleness configuration.
    pub staleness_config: StalenessConfig,
}

impl MarketState {
    /// Create new market state with default config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom staleness config.
    pub fn with_staleness_config(config: StalenessConfig) -> Self {
        Self {
            books: HashMap::new(),
            staleness_config: config,
        }
    }

    /// Update book from QuoteEventV2.
    pub fn on_quote(&mut self, event: &QuoteEventV2) {
        let book = self.books.entry(event.symbol.clone()).or_insert_with(|| {
            Depth5Book::new(&event.symbol)
        });
        book.update(event);
    }

    /// Get book for symbol (if exists).
    pub fn get_book(&self, symbol: &str) -> Option<&Depth5Book> {
        self.books.get(symbol)
    }

    /// Get mutable book for symbol (if exists).
    pub fn get_book_mut(&mut self, symbol: &str) -> Option<&mut Depth5Book> {
        self.books.get_mut(symbol)
    }

    /// Get or create book for symbol.
    pub fn get_or_create_book(&mut self, symbol: &str) -> &mut Depth5Book {
        self.books.entry(symbol.to_string()).or_insert_with(|| {
            Depth5Book::new(symbol)
        })
    }

    /// Check if any book is stale.
    pub fn any_stale(&self, now: DateTime<Utc>) -> bool {
        self.books.values().any(|b| b.is_stale(now, &self.staleness_config))
    }

    /// Get list of stale symbols.
    pub fn stale_symbols(&self, now: DateTime<Utc>) -> Vec<String> {
        self.books
            .iter()
            .filter(|(_, b)| b.is_stale(now, &self.staleness_config))
            .map(|(s, _)| s.clone())
            .collect()
    }

    /// Get all symbols.
    pub fn symbols(&self) -> Vec<String> {
        self.books.keys().cloned().collect()
    }

    /// Number of instruments.
    pub fn len(&self) -> usize {
        self.books.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    /// Clear all books.
    pub fn clear(&mut self) {
        self.books.clear();
    }

    /// Snapshot of all mid prices.
    pub fn mid_snapshot(&self) -> HashMap<String, f64> {
        self.books
            .iter()
            .filter_map(|(s, b)| b.mid().map(|m| (s.clone(), m)))
            .collect()
    }

    /// Snapshot of all microprices.
    pub fn microprice_snapshot(&self) -> HashMap<String, f64> {
        self.books
            .iter()
            .filter_map(|(s, b)| b.microprice().map(|m| (s.clone(), m)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::QuoteFlags;

    fn make_test_event(symbol: &str, bid: f64, ask: f64, bid_qty: u32, ask_qty: u32) -> QuoteEventV2 {
        QuoteEventV2 {
            event_id: 1,
            ts_exchange: None,
            ts_recv: Utc::now(),
            ts_event: Utc::now(),
            source: "TEST".to_string(),
            integrity_tier: QuoteIntegrity::QuoteGradeD5,
            dataset_id: None,
            symbol: symbol.to_string(),
            instrument_token: None,
            ltp: (bid + ask) / 2.0,
            bids: vec![DepthLevelF64 { price: bid, qty: bid_qty }],
            asks: vec![DepthLevelF64 { price: ask, qty: ask_qty }],
            is_synthetic: false,
            flags: QuoteFlags::default(),
        }
    }

    #[test]
    fn test_depth5_book_metrics() {
        let mut book = Depth5Book::new("BANKNIFTY26JAN59000CE");
        let event = make_test_event("BANKNIFTY26JAN59000CE", 100.0, 101.0, 150, 100);
        book.update(&event);

        assert_eq!(book.best_bid(), Some(100.0));
        assert_eq!(book.best_ask(), Some(101.0));
        assert_eq!(book.mid(), Some(100.5));
        assert_eq!(book.spread(), Some(1.0));

        // Microprice: (100*100 + 101*150) / 250 = (10000 + 15150) / 250 = 100.6
        let mp = book.microprice().unwrap();
        assert!((mp - 100.6).abs() < 0.001);

        // Imbalance: (150 - 100) / 250 = 0.2
        let imb = book.imbalance_top1();
        assert!((imb - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_staleness_check() {
        let mut book = Depth5Book::new("TEST");
        book.last_update = Utc::now() - Duration::milliseconds(3000);

        let config = StalenessConfig {
            max_quote_age_ms: 2000,
            ..Default::default()
        };

        assert!(book.is_stale(Utc::now(), &config));
        assert!(book.age_ms(Utc::now()) >= 3000);
    }

    #[test]
    fn test_market_state() {
        let mut state = MarketState::new();

        let event1 = make_test_event("SYM1", 100.0, 101.0, 100, 100);
        let event2 = make_test_event("SYM2", 200.0, 202.0, 200, 200);

        state.on_quote(&event1);
        state.on_quote(&event2);

        assert_eq!(state.len(), 2);
        assert_eq!(state.get_book("SYM1").unwrap().mid(), Some(100.5));
        assert_eq!(state.get_book("SYM2").unwrap().mid(), Some(201.0));
    }
}
