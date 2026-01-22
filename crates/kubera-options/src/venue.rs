//! # ExecutionVenue: Unified Interface for Backtest + Paper
//!
//! Per Z-Phase spec section 7.1: Unified Execution Contract.
//!
//! Both modes implement ExecutionVenue:
//! - KiteSimVenue (backtest)
//! - PaperVenue (paper trading, simulated fills on live quotes)
//!
//! Paper trading in this phase means:
//! - Simulated fills (like KiteSim) but driven by live stream
//! - Optionally "shadow" place real orders later (out of scope now)

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::depth5::Depth5Book;
use crate::execution::{LegOrder, LegExecutionResult, MultiLegOrder, MultiLegResult};
use crate::ledger::OptionsLedger;
use crate::replay::QuoteEventV2;

/// Order acknowledgment from venue.
#[derive(Debug, Clone)]
pub struct OrderAck {
    /// Order ID assigned by venue.
    pub order_id: String,
    /// Accepted or rejected.
    pub accepted: bool,
    /// Rejection reason if not accepted.
    pub reject_reason: Option<String>,
    /// Timestamp of acknowledgment.
    pub ts: DateTime<Utc>,
}

/// Execution venue statistics.
#[derive(Debug, Clone, Default)]
pub struct VenueStats {
    /// Number of orders submitted.
    pub orders_submitted: u64,
    /// Number of orders accepted.
    pub orders_accepted: u64,
    /// Number of orders rejected.
    pub orders_rejected: u64,
    /// Number of fills.
    pub fills: u64,
    /// Number of partial fills.
    pub partial_fills: u64,
    /// Total slippage in basis points.
    pub total_slippage_bps: f64,
    /// Slippage samples for percentile calculation.
    pub slippage_samples: Vec<f64>,
}

impl VenueStats {
    /// Average slippage in basis points.
    pub fn avg_slippage_bps(&self) -> f64 {
        if self.fills == 0 {
            0.0
        } else {
            self.total_slippage_bps / self.fills as f64
        }
    }

    /// Fill rate = fills / orders_accepted.
    pub fn fill_rate(&self) -> f64 {
        if self.orders_accepted == 0 {
            0.0
        } else {
            self.fills as f64 / self.orders_accepted as f64
        }
    }

    /// Slippage percentile (0-100).
    pub fn slippage_percentile(&self, p: f64) -> f64 {
        if self.slippage_samples.is_empty() {
            return 0.0;
        }
        let mut sorted = self.slippage_samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
}

/// Unified execution venue trait per Z-Phase spec.
#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    /// Venue name (e.g., "KiteSim", "Paper", "Zerodha-Live").
    fn name(&self) -> &'static str;

    /// Submit a single-leg order.
    async fn submit(&mut self, order: &LegOrder) -> OrderAck;

    /// Submit a multi-leg order (atomic intent).
    async fn submit_multi(&mut self, order: &MultiLegOrder) -> MultiLegResult;

    /// Process incoming quote (updates internal state, may trigger fills).
    fn on_quote(&mut self, quote: &QuoteEventV2);

    /// Mark positions to market using current book state.
    fn mark_positions(&mut self, books: &HashMap<String, Depth5Book>);

    /// Get ledger reference.
    fn ledger(&self) -> &OptionsLedger;

    /// Get mutable ledger reference.
    fn ledger_mut(&mut self) -> &mut OptionsLedger;

    /// Get venue statistics.
    fn stats(&self) -> &VenueStats;

    /// End of day processing (generates report).
    fn end_of_day(&mut self) -> EndOfDayReport;

    /// Check if venue is ready to accept orders.
    fn is_ready(&self) -> bool;

    /// Current simulation/execution time.
    fn now(&self) -> DateTime<Utc>;
}

/// End of day report from venue.
#[derive(Debug, Clone, Default)]
pub struct EndOfDayReport {
    /// Venue name.
    pub venue: String,
    /// Date.
    pub date: String,
    /// Total PnL (realized + unrealized).
    pub total_pnl: f64,
    /// Realized PnL.
    pub realized_pnl: f64,
    /// Unrealized PnL.
    pub unrealized_pnl: f64,
    /// Total fees.
    pub fees: f64,
    /// Net PnL (after fees).
    pub net_pnl: f64,
    /// Number of trades.
    pub trades: u64,
    /// Number of open positions.
    pub open_positions: usize,
    /// Venue statistics.
    pub stats: VenueStats,
}

/// Paper venue: simulated execution on live quotes.
pub struct PaperVenue {
    ledger: OptionsLedger,
    stats: VenueStats,
    books: HashMap<String, Depth5Book>,
    now: DateTime<Utc>,
    next_order_id: u64,
    /// Latency simulation (milliseconds).
    pub latency_ms: i64,
    /// Slippage model (basis points).
    pub slippage_bps: f64,
}

impl PaperVenue {
    /// Create new paper venue.
    pub fn new() -> Self {
        Self {
            ledger: OptionsLedger::new(),
            stats: VenueStats::default(),
            books: HashMap::new(),
            now: Utc::now(),
            next_order_id: 1,
            latency_ms: 50,
            slippage_bps: 5.0,
        }
    }

    /// Generate next order ID.
    fn next_id(&mut self) -> String {
        let id = format!("PAPER-{}", self.next_order_id);
        self.next_order_id += 1;
        id
    }

    /// Simulate fill for a leg order.
    fn simulate_fill(&mut self, order: &LegOrder) -> LegExecutionResult {
        let book = self.books.get(&order.tradingsymbol);

        let (fill_price, filled_qty) = match book {
            Some(b) => {
                let base_price = match order.side {
                    crate::execution::LegSide::Buy => b.best_ask().unwrap_or(0.0),
                    crate::execution::LegSide::Sell => b.best_bid().unwrap_or(0.0),
                };

                if base_price == 0.0 {
                    return LegExecutionResult {
                        tradingsymbol: order.tradingsymbol.clone(),
                        order_id: Some(self.next_id()),
                        status: crate::execution::LegStatus::Rejected,
                        filled_qty: 0,
                        fill_price: None,
                        error: Some("No quote available".to_string()),
                    };
                }

                // Apply slippage
                let slip = base_price * (self.slippage_bps / 10_000.0);
                let fill_price = match order.side {
                    crate::execution::LegSide::Buy => base_price + slip,
                    crate::execution::LegSide::Sell => base_price - slip,
                };

                // Check quantity available
                let available = match order.side {
                    crate::execution::LegSide::Buy => b.best_ask_qty(),
                    crate::execution::LegSide::Sell => b.best_bid_qty(),
                };

                let filled = order.quantity.min(available);
                (fill_price, filled)
            }
            None => {
                return LegExecutionResult {
                    tradingsymbol: order.tradingsymbol.clone(),
                    order_id: Some(self.next_id()),
                    status: crate::execution::LegStatus::Rejected,
                    filled_qty: 0,
                    fill_price: None,
                    error: Some("Symbol not found in book".to_string()),
                };
            }
        };

        if filled_qty == 0 {
            return LegExecutionResult {
                tradingsymbol: order.tradingsymbol.clone(),
                order_id: Some(self.next_id()),
                status: crate::execution::LegStatus::Rejected,
                filled_qty: 0,
                fill_price: None,
                error: Some("No liquidity".to_string()),
            };
        }

        // Record in ledger
        let is_buy = matches!(order.side, crate::execution::LegSide::Buy);
        self.ledger.on_fill(
            &order.tradingsymbol,
            is_buy,
            filled_qty,
            fill_price,
            None,
            None,
        );

        // Update stats
        self.stats.fills += 1;
        let mid = self.books.get(&order.tradingsymbol).and_then(|b| b.mid()).unwrap_or(fill_price);
        let slip_bps = ((fill_price - mid).abs() / mid) * 10_000.0;
        self.stats.total_slippage_bps += slip_bps;
        self.stats.slippage_samples.push(slip_bps);

        let status = if filled_qty == order.quantity {
            crate::execution::LegStatus::Filled
        } else {
            self.stats.partial_fills += 1;
            crate::execution::LegStatus::PartiallyFilled
        };

        LegExecutionResult {
            tradingsymbol: order.tradingsymbol.clone(),
            order_id: Some(self.next_id()),
            status,
            filled_qty,
            fill_price: Some(fill_price),
            error: None,
        }
    }
}

impl Default for PaperVenue {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionVenue for PaperVenue {
    fn name(&self) -> &'static str {
        "Paper"
    }

    async fn submit(&mut self, order: &LegOrder) -> OrderAck {
        self.stats.orders_submitted += 1;

        // Basic validation
        if order.quantity == 0 {
            self.stats.orders_rejected += 1;
            return OrderAck {
                order_id: self.next_id(),
                accepted: false,
                reject_reason: Some("Quantity is zero".to_string()),
                ts: self.now,
            };
        }

        self.stats.orders_accepted += 1;
        OrderAck {
            order_id: self.next_id(),
            accepted: true,
            reject_reason: None,
            ts: self.now,
        }
    }

    async fn submit_multi(&mut self, order: &MultiLegOrder) -> MultiLegResult {
        let mut leg_results = Vec::with_capacity(order.legs.len());
        let mut total_premium = 0.0;
        let mut all_filled = true;

        for leg in &order.legs {
            let result = self.simulate_fill(leg);
            if result.status != crate::execution::LegStatus::Filled {
                all_filled = false;
            }
            if let Some(px) = result.fill_price {
                let sign = match leg.side {
                    crate::execution::LegSide::Buy => -1.0,
                    crate::execution::LegSide::Sell => 1.0,
                };
                total_premium += sign * px * result.filled_qty as f64;
            }
            leg_results.push(result);
        }

        MultiLegResult {
            strategy_name: order.strategy_name.clone(),
            leg_results,
            all_filled,
            total_premium,
        }
    }

    fn on_quote(&mut self, quote: &QuoteEventV2) {
        self.now = quote.ts_event;
        let book = self.books.entry(quote.symbol.clone()).or_insert_with(|| {
            crate::depth5::Depth5Book::new(&quote.symbol)
        });
        book.update(quote);
    }

    fn mark_positions(&mut self, books: &HashMap<String, Depth5Book>) {
        self.ledger.mark_to_market(books);
    }

    fn ledger(&self) -> &OptionsLedger {
        &self.ledger
    }

    fn ledger_mut(&mut self) -> &mut OptionsLedger {
        &mut self.ledger
    }

    fn stats(&self) -> &VenueStats {
        &self.stats
    }

    fn end_of_day(&mut self) -> EndOfDayReport {
        self.ledger.mark_to_market(&self.books);

        EndOfDayReport {
            venue: self.name().to_string(),
            date: self.now.format("%Y-%m-%d").to_string(),
            total_pnl: self.ledger.total_pnl(),
            realized_pnl: self.ledger.total_realized_pnl(),
            unrealized_pnl: self.ledger.total_unrealized_pnl(),
            fees: self.ledger.total_fees(),
            net_pnl: self.ledger.net_pnl(),
            trades: self.stats.fills,
            open_positions: self.ledger.open_position_count(),
            stats: self.stats.clone(),
        }
    }

    fn is_ready(&self) -> bool {
        !self.books.is_empty()
    }

    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{LegOrderType, LegSide};
    use crate::replay::{DepthLevelF64, QuoteFlags, QuoteIntegrity};

    fn make_quote(symbol: &str, bid: f64, ask: f64) -> QuoteEventV2 {
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
            bids: vec![DepthLevelF64 { price: bid, qty: 100 }],
            asks: vec![DepthLevelF64 { price: ask, qty: 100 }],
            is_synthetic: false,
            flags: QuoteFlags::default(),
        }
    }

    #[tokio::test]
    async fn test_paper_venue_fill() {
        let mut venue = PaperVenue::new();
        venue.slippage_bps = 0.0; // No slippage for test

        // Send quote
        let quote = make_quote("BANKNIFTY26JAN59000CE", 100.0, 101.0);
        venue.on_quote(&quote);

        // Submit buy order
        let order = LegOrder {
            tradingsymbol: "BANKNIFTY26JAN59000CE".to_string(),
            exchange: "NFO".to_string(),
            side: LegSide::Buy,
            quantity: 1,
            order_type: LegOrderType::Market,
            price: None,
        };

        let result = venue.simulate_fill(&order);
        assert_eq!(result.status, crate::execution::LegStatus::Filled);
        assert_eq!(result.fill_price, Some(101.0)); // Bought at ask
    }
}
