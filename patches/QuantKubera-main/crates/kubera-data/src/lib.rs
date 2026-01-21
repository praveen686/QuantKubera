//! # Market Data Processing Module
//!
//! Tick-to-bar aggregation, orderbook management, and VPIN calculation.
//!
//! ## Description
//! Provides real-time market data processing components:
//! - **BarAggregator**: Converts tick streams to OHLCV bars
//! - **Level2Book**: Maintains local orderbook from snapshots/deltas
//! - **VpinCalculator**: Volume-synchronized PIN for toxicity detection
//!
//! ## Components
//! | Component | Purpose |
//! |-----------|---------|
//! | [`BarAggregator`] | Tick → Bar conversion |
//! | [`Level2Book`] | L2 orderbook maintenance |
//! | [`VpinCalculator`] | Flow toxicity metric |
//!
//! ## References
//! - Easley, D., Lopez de Prado, M., & O'Hara, M. (2012). VPIN
//! - IEEE Std 1016-2009: Software Design Descriptions

use kubera_models::{MarketEvent, MarketPayload, Side, L2Snapshot, L2Update};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, BTreeMap};
use ordered_float::OrderedFloat;

pub mod parquet_exporter;
pub use parquet_exporter::ParquetExporter;

/// Aggregates tick data into OHLCV bars.
///
/// # Description
/// Converts a stream of ticks into time-bucketed bars. Each bar contains
/// open, high, low, close prices and cumulative volume.
///
/// # Fields
/// * `interval_ms` - Bar duration in milliseconds
/// * `current_bars` - In-progress bars by symbol
pub struct BarAggregator {
    /// Bar interval duration in milliseconds.
    interval_ms: u64,
    /// Current incomplete bars keyed by symbol.
    current_bars: HashMap<String, BarState>,
}

/// Internal state for an in-progress bar.
struct BarState {
    /// Opening price of the bar.
    open: f64,
    /// Highest price during the bar.
    high: f64,
    /// Lowest price during the bar.
    low: f64,
    /// Most recent price (becomes close).
    close: f64,
    /// Cumulative volume.
    volume: f64,
    /// Bar start timestamp.
    start_time: DateTime<Utc>,
}

impl BarAggregator {
    /// Creates a new bar aggregator with specified interval.
    ///
    /// # Parameters
    /// * `interval_ms` - Bar duration in milliseconds (e.g., 60000 for 1-minute bars)
    ///
    /// # Returns
    /// New [`BarAggregator`] instance.
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            current_bars: HashMap::new(),
        }
    }

    pub fn handle_tick(&mut self, event: &MarketEvent) -> Option<MarketEvent> {
        let (price, size) = match &event.payload {
            MarketPayload::Tick { price, size, .. } => (*price, *size),
            _ => return None,
        };

        let bar_start = self.get_bar_start(event.exchange_time);
        let state = self.current_bars.entry(event.symbol.clone()).or_insert_with(|| BarState {
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0.0,
            start_time: bar_start,
        });

        // Check if bar closed
        if bar_start > state.start_time {
            let completed_bar = MarketEvent {
                exchange_time: state.start_time,
                local_time: Utc::now(),
                symbol: event.symbol.clone(),
                payload: MarketPayload::Bar {
                    open: state.open,
                    high: state.high,
                    low: state.low,
                    close: state.close,
                    volume: state.volume,
                    interval_ms: self.interval_ms,
                },
            };

            // Reset for new bar
            state.open = price;
            state.high = price;
            state.low = price;
            state.close = price;
            state.volume = size;
            state.start_time = bar_start;

            Some(completed_bar)
        } else {
            state.high = state.high.max(price);
            state.low = state.low.min(price);
            state.close = price;
            state.volume += size;
            None
        }
    }

    fn get_bar_start(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let timestamp = time.timestamp_millis();
        let interval = self.interval_ms as i64;
        let start_ms = (timestamp / interval) * interval;
        DateTime::from_timestamp_millis(start_ms).unwrap_or(time).with_timezone(&Utc)
    }
}

pub struct Level2Book {
    pub symbol: String,
    pub bids: BTreeMap<OrderedFloat<f64>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    pub last_update_id: u64,
}

impl Level2Book {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: L2Snapshot, update_id: u64) {
        if update_id < self.last_update_id {
            return;
        }
        self.bids.clear();
        self.asks.clear();
        for level in snapshot.bids {
            self.bids.insert(OrderedFloat(level.price), level.size);
        }
        for level in snapshot.asks {
            self.asks.insert(OrderedFloat(level.price), level.size);
        }
        self.last_update_id = update_id;
    }

    /// Apply a depth update to the book. Uses last_update_id from the update for sequencing.
    pub fn apply_update(&mut self, update: &L2Update) {
        if update.last_update_id <= self.last_update_id {
            return;
        }

        for level in &update.bids {
            let price = OrderedFloat(level.price);
            if level.size == 0.0 {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, level.size);
            }
        }
        for level in &update.asks {
            let price = OrderedFloat(level.price);
            if level.size == 0.0 {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, level.size);
            }
        }
        self.last_update_id = update.last_update_id;
    }

    pub fn get_best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(p, s)| (p.0, *s))
    }

    pub fn get_best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, s)| (p.0, *s))
    }
}

pub struct VpinCalculator {
    pub symbol: String,
    pub bucket_size: f64,
    pub current_bucket_volume: f64,
    pub current_bucket_imbalance: f64,
    pub vpin_history: Vec<f64>,
    pub window_size: usize,
}

impl VpinCalculator {
    pub fn new(symbol: String, bucket_size: f64, window_size: usize) -> Self {
        Self {
            symbol,
            bucket_size,
            current_bucket_volume: 0.0,
            current_bucket_imbalance: 0.0,
            vpin_history: Vec::new(),
            window_size,
        }
    }

    pub fn on_trade(&mut self, _price: f64, size: f64, side: Side) -> Option<f64> {
        let mut remaining_size = size;
        let mut last_vpin = None;

        while remaining_size > 0.0 {
            let space_in_bucket = self.bucket_size - self.current_bucket_volume;
            let trade_size_in_bucket = f64::min(remaining_size, space_in_bucket);
            
            let sign = match side {
                Side::Buy => 1.0,
                Side::Sell => -1.0,
            };

            self.current_bucket_imbalance += sign * trade_size_in_bucket;
            self.current_bucket_volume += trade_size_in_bucket;
            remaining_size -= trade_size_in_bucket;

            if (self.current_bucket_volume - self.bucket_size).abs() < 1e-9 {
                last_vpin = Some(self.calculate_vpin());
                self.current_bucket_volume = 0.0;
                self.current_bucket_imbalance = 0.0;
            }
        }
        last_vpin
    }

    fn calculate_vpin(&mut self) -> f64 {
        let imbalance_abs = self.current_bucket_imbalance.abs();
        self.vpin_history.push(imbalance_abs);
        if self.vpin_history.len() > self.window_size {
            self.vpin_history.remove(0);
        }

        if self.vpin_history.is_empty() {
            return 0.0;
        }

        let total_imbalance: f64 = self.vpin_history.iter().sum();
        total_imbalance / (self.vpin_history.len() as f64 * self.bucket_size)
    }
}

pub mod simulator {
    use super::*;
    use kubera_core::EventBus;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration as TokioDuration};
    use rand::Rng;

    pub async fn run_simulated_feed(
        bus: Arc<EventBus>,
        symbol: String,
        base_price: f64,
    ) -> anyhow::Result<()> {
        let mut current_price = base_price;

        loop {
            let (tick, sleep_duration) = {
                let mut rng = rand::thread_rng();
                let change = (rng.r#gen::<f64>() - 0.5) * 0.1;
                current_price += change;
                
                let tick = MarketEvent {
                    exchange_time: Utc::now(),
                    local_time: Utc::now(),
                    symbol: symbol.clone(),
                    payload: MarketPayload::Tick {
                        price: current_price,
                        size: rng.r#gen_range(1.0..10.0),
                        side: if change > 0.0 { Side::Buy } else { Side::Sell },
                    },
                };
                (tick, rng.r#gen_range(100..500))
            };

            bus.publish_market(tick).await?;
            sleep(TokioDuration::from_millis(sleep_duration)).await;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use kubera_models::{L2Level, L2Snapshot, L2Update, Side};

    #[test]
    fn test_l2_book_snapshot_and_update() {
        let mut book = Level2Book::new("BTC-USDT".to_string());
        let snapshot = L2Snapshot {
            bids: vec![L2Level { price: 50000.0, size: 1.0 }, L2Level { price: 49990.0, size: 2.0 }],
            asks: vec![L2Level { price: 50010.0, size: 1.0 }, L2Level { price: 50020.0, size: 2.0 }],
            update_id: 100,
        };
        book.apply_snapshot(snapshot, 100);
        assert_eq!(book.get_best_bid(), Some((50000.0, 1.0)));
        assert_eq!(book.get_best_ask(), Some((50010.0, 1.0)));

        let update = L2Update {
            bids: vec![L2Level { price: 50000.0, size: 0.0 }, L2Level { price: 50005.0, size: 0.5 }],
            asks: vec![],
            first_update_id: 101,
            last_update_id: 101,
        };
        book.apply_update(&update);
        assert_eq!(book.get_best_bid(), Some((50005.0, 0.5)));
    }

    #[test]
    fn test_vpin_calculator() {
        let mut calc = VpinCalculator::new("BTC-USDT".to_string(), 10.0, 5);
        
        // Buy 5 units - Bucket [5/10] Imbalance +5
        assert!(calc.on_trade(50000.0, 5.0, Side::Buy).is_none());
        
        // Sell 5 units - Bucket [10/10] Imbalance 0 (5 - 5)
        // VPIN for 1 bucket: 0 / (1 * 10) = 0
        let vpin = calc.on_trade(50005.0, 5.0, Side::Sell);
        assert!(vpin.is_some());
        assert_eq!(vpin.unwrap(), 0.0);

        // Buy 20 units - Fills 2 buckets
        // Bucket 2: 10 units Buy. Imbalance 10. VPIN history: [0, 10]
        // VPIN: (0 + 10) / (2 * 10) = 0.5
        // Bucket 3: 10 units Buy. Imbalance 10. VPIN history: [0, 10, 10]
        // VPIN: (0 + 10 + 10) / (3 * 10) = 0.666...
        let vpin2 = calc.on_trade(50010.0, 20.0, Side::Buy);
        assert!(vpin2.is_some());
        assert!((vpin2.unwrap() - 0.66666666).abs() < 1e-6);
    }
}
