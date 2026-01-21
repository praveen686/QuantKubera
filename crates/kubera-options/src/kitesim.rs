//! # KiteSim: Offline Zerodha Options Execution Simulator
//!
//! This module provides a **quote-driven** execution simulator intended for
//! *strategy + execution* validation before any live deployment.
//!
//! Scope (intentional):
//! - L1 quote driven (best bid/ask + sizes) - legacy mode
//! - L2 depth driven (full order book) - Phase-2A mode
//! - Market + Limit orders
//! - Configurable latency and partial-fill behavior
//! - Atomic multi-leg execution coordinator with rollback and optional hedging
//!
//! ## Execution Modes
//! - `L1Quote`: Uses best bid/ask snapshots (backward compatible)
//! - `L2Book`: Uses full order book depth for realistic multi-level fills

use crate::execution::{LegOrder, LegOrderType, LegSide, LegStatus, MultiLegOrder, MultiLegResult, LegExecutionResult};
use crate::replay::{QuoteEvent, ReplayEvent, DepthEvent};
use crate::specs::SpecStore;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
// Use the canonical OrderBook from kubera-core (proper dependency layering)
use kubera_core::lob::OrderBook;

/// Errors that can occur during KiteSim execution.
#[derive(Debug, Clone)]
pub enum KiteSimError {
    /// Gap detected in depth update sequence.
    /// This is a HARD FAILURE - replay data is corrupted or incomplete.
    DepthSequenceGap {
        symbol: String,
        expected: u64,
        actual: u64,
    },
    /// Price or quantity exponent mismatch in depth event.
    ExponentMismatch {
        symbol: String,
        detail: String,
    },
}

impl std::fmt::Display for KiteSimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KiteSimError::DepthSequenceGap { symbol, expected, actual } => {
                write!(
                    f,
                    "FATAL: Depth sequence gap for {}: expected update_id={}, got {} (gap={})",
                    symbol, expected, actual, actual.saturating_sub(*expected)
                )
            }
            KiteSimError::ExponentMismatch { symbol, detail } => {
                write!(f, "Exponent mismatch for {}: {}", symbol, detail)
            }
        }
    }
}

impl std::error::Error for KiteSimError {}

/// Execution mode for the simulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SimExecutionMode {
    /// L1 quote-driven execution (best bid/ask only).
    /// This is the legacy mode for backward compatibility.
    #[default]
    L1Quote,
    /// L2 depth-driven execution (full order book).
    /// Uses multi-level fill simulation for realistic slippage.
    L2Book,
}

/// Configuration for the simulator.
#[derive(Debug, Clone)]
pub struct KiteSimConfig {
    /// Order becomes eligible to match only after this delay.
    pub latency: Duration,
    /// If true, an order may partially fill based on displayed quote size.
    pub allow_partial: bool,
    /// If set, adds a pessimistic slippage in basis points to marketable fills.
    /// This is applied on top of bid/ask.
    pub taker_slippage_bps: f64,
    /// Maximum adverse selection in basis points (Patch 2 hook).
    /// Applied when mid moves against the order before fill.
    pub adverse_selection_max_bps: f64,
    /// Reject order if no quote received within this duration (Patch 2 hook).
    pub reject_if_no_quote_after: Duration,
    /// Execution mode: L1Quote (legacy) or L2Book (depth-based).
    pub execution_mode: SimExecutionMode,
}

impl Default for KiteSimConfig {
    fn default() -> Self {
        Self {
            latency: Duration::milliseconds(150),
            allow_partial: true,
            taker_slippage_bps: 0.0,
            adverse_selection_max_bps: 0.0,
            reject_if_no_quote_after: Duration::seconds(10),
            execution_mode: SimExecutionMode::L1Quote,
        }
    }
}

/// Statistics collected during a KiteSim run (Patch 2).
/// Wire these to BacktestReport for certification.
#[derive(Debug, Default, Clone)]
pub struct KiteSimRunStats {
    /// Number of orders that timed out waiting for quotes.
    pub timeouts: u64,
    /// Number of multi-leg rollbacks triggered.
    pub rollbacks: u64,
    /// Number of hedge orders attempted during rollback.
    pub hedges_attempted: u64,
    /// Number of hedge orders filled.
    pub hedges_filled: u64,
    /// Slippage samples in basis points for analysis.
    pub slippage_samples_bps: Vec<f64>,
}

/// Snapshot of last known quote.
#[derive(Debug, Clone, Copy)]
struct Quote {
    ts: DateTime<Utc>,
    bid: f64,
    ask: f64,
    bid_qty: u32,
    ask_qty: u32,
}

/// Internal order state.
#[derive(Debug, Clone)]
struct SimOrder {
    order_id: String,
    tradingsymbol: String,
    side: LegSide,
    order_type: LegOrderType,
    limit_price: Option<f64>,
    qty: u32,
    filled_qty: u32,
    avg_price: Option<f64>,
    status: LegStatus,
    created_ts: DateTime<Utc>,
    eligible_ts: DateTime<Utc>,
    /// Mid price at order placement (Patch 3: for adverse selection).
    mid_at_place: f64,
}

impl SimOrder {
    fn remaining(&self) -> u32 {
        self.qty.saturating_sub(self.filled_qty)
    }
}

/// L2 order book wrapper for KiteSim.
// NOTE: SimOrderBook has been replaced by kubera_core::lob::OrderBook
// This is the canonical LOB implementation with proper layering:
// kubera-models → kubera-core → kubera-options

/// Primary simulator: accepts quote events and matches eligible orders.
pub struct KiteSim {
    cfg: KiteSimConfig,
    specs: Option<SpecStore>,
    last_quotes: HashMap<String, Quote>,
    /// L2 order books per symbol (only used in L2Book mode).
    /// Uses the canonical OrderBook from kubera-core.
    order_books: HashMap<String, OrderBook>,
    orders: HashMap<String, SimOrder>,
    stats: KiteSimRunStats,
    next_id: AtomicU64,
    /// Current simulation time (public for Patch 2 stats wiring).
    pub now: DateTime<Utc>,
}

impl KiteSim {
    pub fn new(cfg: KiteSimConfig) -> Self {
        Self {
            cfg,
            specs: None,
            last_quotes: HashMap::new(),
            order_books: HashMap::new(),
            orders: HashMap::new(),
            stats: KiteSimRunStats::default(),
            next_id: AtomicU64::new(1),
            now: Utc::now(),
        }
    }

    /// Attach instrument specs for lot size / tick size validation (Patch 2).
    pub fn with_specs(mut self, specs: SpecStore) -> Self {
        self.specs = Some(specs);
        self
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }

    /// Set the simulator clock to a specific time (for scheduled intents).
    pub fn set_now(&mut self, ts: DateTime<Utc>) {
        self.now = ts;
    }

    /// Ingest a replay event (updates book state without advancing clock).
    ///
    /// # Errors
    /// Returns `KiteSimError::DepthSequenceGap` if a depth sequence gap is detected.
    pub fn ingest_event(&mut self, event: &ReplayEvent) -> Result<(), KiteSimError> {
        match event {
            ReplayEvent::Quote(q) => {
                self.on_quote(q.clone());
                Ok(())
            }
            ReplayEvent::Depth(d) => self.on_depth(d.clone()),
        }
    }

    /// Returns collected stress statistics (Patch 3).
    pub fn stats(&self) -> KiteSimRunStats {
        self.stats.clone()
    }

    /// Calculate mid price from quote.
    fn mid(q: &Quote) -> f64 {
        (q.bid + q.ask) * 0.5
    }

    /// Apply adverse selection when mid moves against the order (Patch 3).
    /// Returns adjusted fill price.
    fn adverse_adjust(&self, px: f64, side: LegSide, mid0: f64, mid1: f64) -> f64 {
        if self.cfg.adverse_selection_max_bps <= 0.0 || mid0 <= 0.0 {
            return px;
        }
        // Measure mid movement in bps
        let mv = ((mid1 - mid0) / mid0) * 10000.0;
        // Harmful move: up for buys, down for sells
        let harm = match side {
            LegSide::Buy => mv.max(0.0),
            LegSide::Sell => (-mv).max(0.0),
        };
        let bps = harm.min(self.cfg.adverse_selection_max_bps) / 10000.0;
        match side {
            LegSide::Buy => px * (1.0 + bps),
            LegSide::Sell => px * (1.0 - bps),
        }
    }

    /// Record slippage sample in bps (Patch 3).
    fn record_slip(&mut self, side: LegSide, fill_px: f64, mid: f64) {
        if mid <= 0.0 { return; }
        let bps = match side {
            LegSide::Buy => ((fill_px - mid) / mid) * 10000.0,
            LegSide::Sell => ((mid - fill_px) / mid) * 10000.0,
        };
        if bps.is_finite() {
            self.stats.slippage_samples_bps.push(bps);
        }
    }

    /// Attempt final reconciliation for timeout / race simulation (Patch 3).
    /// Returns Some(result) if order reached terminal state.
    pub fn final_reconcile(&mut self, order_id: &str) -> Option<LegExecutionResult> {
        // Attempt one more match before returning terminal state
        if let Some(o) = self.orders.get(order_id) {
            let sym = o.tradingsymbol.clone();
            self.match_eligible_orders_for(&sym);
        }
        self.orders.get(order_id).map(|o| LegExecutionResult {
            tradingsymbol: o.tradingsymbol.clone(),
            order_id: Some(order_id.to_string()),
            status: o.status,
            filled_qty: o.filled_qty,
            fill_price: o.avg_price,
            error: None,
        })
    }

    /// Advances simulator time and processes a single replay event.
    ///
    /// # Errors
    /// Returns `KiteSimError::DepthSequenceGap` if a depth sequence gap is detected.
    pub fn on_event(&mut self, event: ReplayEvent) -> Result<(), KiteSimError> {
        self.now = event.ts();
        match event {
            ReplayEvent::Quote(q) => {
                self.on_quote(q);
                Ok(())
            }
            ReplayEvent::Depth(d) => self.on_depth(d),
        }
    }

    /// Process an L2 depth event.
    /// In L2Book mode: updates the order book and triggers matching.
    /// In L1Quote mode: ignored (use on_quote for L1).
    ///
    /// # Errors
    /// Returns `KiteSimError::DepthSequenceGap` if a gap is detected in update IDs.
    /// This is a HARD FAILURE - the caller MUST abort replay on this error.
    pub fn on_depth(&mut self, d: DepthEvent) -> Result<(), KiteSimError> {
        if self.cfg.execution_mode != SimExecutionMode::L2Book {
            return Ok(()); // Ignore depth events in L1 mode
        }

        let symbol = d.tradingsymbol.clone();

        // Handle snapshot vs diff
        let is_snapshot = d.is_snapshot;

        // Get or create the order book for this symbol
        // OrderBook requires (symbol, price_exponent, qty_exponent)
        let book = self.order_books.entry(symbol.clone()).or_insert_with(|| {
            OrderBook::new(symbol.clone(), d.price_exponent, d.qty_exponent)
        });

        // Apply the depth update with strict gap checking
        if is_snapshot {
            // Snapshots bypass gap checking - they reset the book
            book.clear();
            book.apply_depth_event_unchecked(&d);
        } else {
            // Diffs require strict gap checking - HARD FAIL on gaps
            if let Err(_e) = book.apply_depth_event(&d) {
                // This is a HARD FAILURE - do NOT continue
                return Err(KiteSimError::DepthSequenceGap {
                    symbol: symbol.clone(),
                    expected: book.last_update_id() + 1,
                    actual: d.first_update_id,
                });
            }
        }

        // Trigger order matching against the updated book
        self.match_eligible_orders_for_l2(&symbol);
        Ok(())
    }

    pub fn on_quote(&mut self, q: QuoteEvent) {
        self.last_quotes.insert(
            q.tradingsymbol.clone(),
            Quote {
                ts: q.ts,
                bid: q.bid,
                ask: q.ask,
                bid_qty: q.bid_qty,
                ask_qty: q.ask_qty,
            },
        );
        self.match_eligible_orders_for(&q.tradingsymbol);
    }

    /// Places a leg order into the simulator.
    pub fn place_order(&mut self, leg: &LegOrder, ts: DateTime<Utc>) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let order_id = format!("SIM-{}", id);

        // Capture mid price at placement for adverse selection (Patch 3)
        let mid_at_place = self.last_quotes.get(&leg.tradingsymbol)
            .map(|q| Self::mid(q))
            .unwrap_or(0.0);

        let eligible_ts = ts + self.cfg.latency;
        let order = SimOrder {
            order_id: order_id.clone(),
            tradingsymbol: leg.tradingsymbol.clone(),
            side: leg.side,
            order_type: leg.order_type,
            limit_price: leg.price,
            qty: leg.quantity,
            filled_qty: 0,
            avg_price: None,
            status: LegStatus::Placed,
            created_ts: ts,
            eligible_ts,
            mid_at_place,
        };

        self.orders.insert(order_id.clone(), order);
        // Attempt immediate match if we already have quote/book data and we're beyond latency.
        match self.cfg.execution_mode {
            SimExecutionMode::L2Book => self.match_eligible_orders_for_l2(&leg.tradingsymbol),
            SimExecutionMode::L1Quote => self.match_eligible_orders_for(&leg.tradingsymbol),
        }
        order_id
    }

    /// Attempts to cancel an order. Filled orders remain Filled.
    pub fn cancel_order(&mut self, order_id: &str) {
        if let Some(o) = self.orders.get_mut(order_id) {
            if matches!(o.status, LegStatus::Placed | LegStatus::Pending | LegStatus::PartiallyFilled) {
                o.status = LegStatus::Cancelled;
            }
        }
    }

    /// Returns a current snapshot of a simulated order.
    pub fn get_order(&self, order_id: &str) -> Option<(LegStatus, Option<f64>, u32, u32)> {
        self.orders.get(order_id).map(|o| (o.status, o.avg_price, o.filled_qty, o.qty))
    }

    fn match_eligible_orders_for(&mut self, tradingsymbol: &str) {
        let Some(q) = self.last_quotes.get(tradingsymbol).copied() else { return; };

        // Collect order ids to avoid borrowing issues.
        let ids: Vec<String> = self
            .orders
            .iter()
            .filter_map(|(id, o)| {
                if o.tradingsymbol == tradingsymbol {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for id in ids {
            let eligible;
            {
                let Some(o) = self.orders.get(&id) else { continue; };
                eligible = self.now >= o.eligible_ts
                    && matches!(o.status, LegStatus::Placed | LegStatus::PartiallyFilled);
            }
            if !eligible {
                continue;
            }
            self.try_fill_order(&id, q);
        }
    }

    /// L2 order matching using the full order book.
    fn match_eligible_orders_for_l2(&mut self, tradingsymbol: &str) {
        let Some(book) = self.order_books.get(tradingsymbol) else { return; };

        // Extract book data to avoid borrow conflicts
        let mid = book.mid_f64();

        // Collect order ids to process
        let ids: Vec<String> = self
            .orders
            .iter()
            .filter_map(|(id, o)| {
                if o.tradingsymbol == tradingsymbol {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for id in ids {
            let eligible;
            let (rem, side, order_type, limit_price, mid_at_place, filled_qty, avg_price);
            {
                let Some(o) = self.orders.get(&id) else { continue; };
                eligible = self.now >= o.eligible_ts
                    && matches!(o.status, LegStatus::Placed | LegStatus::PartiallyFilled);
                rem = o.remaining();
                side = o.side;
                order_type = o.order_type;
                limit_price = o.limit_price;
                mid_at_place = o.mid_at_place;
                filled_qty = o.filled_qty;
                avg_price = o.avg_price;
            }

            if !eligible || rem == 0 {
                continue;
            }

            // Get book reference again for simulation
            let Some(book) = self.order_books.get(tradingsymbol) else { continue; };

            // Convert order quantity to mantissa for L2 book simulation
            // qty_mantissa = qty * 10^(-qty_exponent)
            let qty_mantissa = book.f64_to_qty(rem as f64);

            // Simulate the fill using the full order book
            // Returns (total_cost, total_filled_f64, avg_price)
            let sim_result = match side {
                LegSide::Buy => book.simulate_market_buy(qty_mantissa),
                LegSide::Sell => book.simulate_market_sell(qty_mantissa),
            };

            let Some((_sim_cost, sim_filled_qty, sim_avg_price)) = sim_result else {
                continue; // No liquidity
            };

            // For limit orders, check if we can fill at or better than limit
            if order_type == LegOrderType::Limit {
                if let Some(lim) = limit_price {
                    match side {
                        LegSide::Buy if sim_avg_price > lim => continue,
                        LegSide::Sell if sim_avg_price < lim => continue,
                        _ => {}
                    }
                } else {
                    // Malformed limit order
                    if let Some(o) = self.orders.get_mut(&id) {
                        o.status = LegStatus::Rejected;
                    }
                    continue;
                }
            }

            // Apply slippage adjustment
            let slip = sim_avg_price * (self.cfg.taker_slippage_bps / 10_000.0);
            let base_px = match side {
                LegSide::Buy => sim_avg_price + slip,
                LegSide::Sell => sim_avg_price - slip,
            };

            // Apply adverse selection if enabled
            let current_mid = mid.unwrap_or(0.0);
            let fill_px = self.adverse_adjust(base_px, side, mid_at_place, current_mid);

            // Calculate fill quantity (may be partial based on book depth)
            let fill_qty = (sim_filled_qty as u32).min(rem);
            if fill_qty == 0 {
                continue;
            }

            // Update order state
            if let Some(o) = self.orders.get_mut(&id) {
                let total_qty = filled_qty + fill_qty;
                let new_avg = if let Some(old_avg) = avg_price {
                    let old_val = old_avg * (filled_qty as f64);
                    let new_val = fill_px * (fill_qty as f64);
                    (old_val + new_val) / (total_qty as f64)
                } else {
                    fill_px
                };
                o.filled_qty = total_qty;
                o.avg_price = Some(new_avg);
                o.status = if o.remaining() == 0 {
                    LegStatus::Filled
                } else {
                    LegStatus::PartiallyFilled
                };

                // Record slippage
                self.record_slip(side, fill_px, current_mid);
            }
        }
    }

    fn try_fill_order(&mut self, order_id: &str, q: Quote) {
        // Extract order info to avoid borrow conflicts
        let (rem, side, order_type, limit_price, mid_at_place, filled_qty, avg_price) = {
            let Some(o) = self.orders.get(order_id) else { return; };
            (o.remaining(), o.side, o.order_type, o.limit_price, o.mid_at_place, o.filled_qty, o.avg_price)
        };

        if rem == 0 {
            if let Some(o) = self.orders.get_mut(order_id) {
                o.status = LegStatus::Filled;
            }
            return;
        }

        // Determine executable price and visible quantity.
        let (px, visible_qty) = match side {
            LegSide::Buy => (q.ask, q.ask_qty),
            LegSide::Sell => (q.bid, q.bid_qty),
        };

        // Limit price check.
        if order_type == LegOrderType::Limit {
            if let Some(lim) = limit_price {
                match side {
                    LegSide::Buy if px > lim => return,
                    LegSide::Sell if px < lim => return,
                    _ => {}
                }
            } else {
                // Malformed: limit order without price.
                if let Some(o) = self.orders.get_mut(order_id) {
                    o.status = LegStatus::Rejected;
                }
                return;
            }
        }

        if visible_qty == 0 {
            return;
        }

        let fill_qty = if self.cfg.allow_partial {
            visible_qty.min(rem)
        } else {
            if visible_qty >= rem { rem } else { 0 }
        };

        if fill_qty == 0 {
            return;
        }

        // Apply pessimistic slippage for taker-style fills.
        let slip = px * (self.cfg.taker_slippage_bps / 10_000.0);
        let base_px = match side {
            LegSide::Buy => px + slip,
            LegSide::Sell => px - slip,
        };

        // Apply adverse selection (Patch 3): penalize when mid moved against order
        let mid_now = Self::mid(&q);
        let fill_px = self.adverse_adjust(base_px, side, mid_at_place, mid_now);

        // Record slippage sample (Patch 3)
        self.record_slip(side, fill_px, mid_now);

        // Update VWAP.
        let prev_notional = avg_price.unwrap_or(0.0) * (filled_qty as f64);
        let add_notional = fill_px * (fill_qty as f64);
        let new_filled = filled_qty + fill_qty;

        if let Some(o) = self.orders.get_mut(order_id) {
            o.avg_price = Some((prev_notional + add_notional) / (new_filled as f64));
            o.filled_qty = new_filled;

            if o.filled_qty == o.qty {
                o.status = LegStatus::Filled;
            } else {
                o.status = LegStatus::PartiallyFilled;
            }
        }
    }
}

/// Execution policy for coordinating multi-leg orders inside KiteSim.
#[derive(Debug, Clone)]
pub struct AtomicExecPolicy {
    /// Total wall-clock timeout for the multi-leg execution.
    pub timeout: Duration,
    /// If true, attempts to **neutralize** already-filled legs if a later leg fails.
    ///
    /// This is recommended for realistic testing because in production a leg can
    /// fill even if later legs fail.
    pub hedge_on_failure: bool,
}

impl Default for AtomicExecPolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::seconds(5),
            hedge_on_failure: true,
        }
    }
}

/// Atomic multi-leg coordinator operating against a KiteSim instance.
pub struct MultiLegCoordinator<'a> {
    sim: &'a mut KiteSim,
    policy: AtomicExecPolicy,
}

impl<'a> MultiLegCoordinator<'a> {
    pub fn new(sim: &'a mut KiteSim, policy: AtomicExecPolicy) -> Self {
        Self { sim, policy }
    }

    /// Execute a multi-leg order using sequential placement + fill confirmation.
    ///
    /// This is intentionally **simple**:
    /// - place leg i
    /// - wait until Filled / terminal / timeout
    /// - if failure, cancel remaining and optionally hedge filled legs
    ///
    /// # Errors
    /// Returns `KiteSimError::DepthSequenceGap` if a depth sequence gap is detected.
    /// This is a HARD FAILURE - the caller MUST abort the backtest on this error.
    pub async fn execute_with_feed(
        &mut self,
        order: &MultiLegOrder,
        feed: &mut crate::replay::ReplayFeed,
    ) -> Result<MultiLegResult, KiteSimError> {
        // Prime simulator time from first replay event to ensure order eligibility
        // aligns with event timestamps (critical for offline replay).
        if let Some(ev) = feed.peek() {
            self.sim.now = ev.ts();
        }

        let start = self.sim.now();
        let deadline = start + self.policy.timeout;

        let mut leg_results: Vec<LegExecutionResult> = Vec::with_capacity(order.legs.len());
        let mut placed_ids: Vec<String> = Vec::with_capacity(order.legs.len());
        let mut total_premium = 0.0;

        for leg in &order.legs {
            let oid = self.sim.place_order(leg, self.sim.now());
            placed_ids.push(oid.clone());

            // Drive the simulator forward until this leg completes or deadline.
            loop {
                if self.sim.now() >= deadline {
                    break;
                }
                // Process next event if available and within deadline.
                match feed.peek() {
                    Some(ev) if ev.ts() <= deadline => {
                        let ev = feed.next().expect("peek ensured Some");
                        self.sim.on_event(ev)?;
                    }
                    _ => {
                        // No more events available in window.
                        break;
                    }
                }

                let Some((st, avg, filled, qty)) = self.sim.get_order(&oid) else { break; };

                if matches!(st, LegStatus::Filled | LegStatus::Rejected | LegStatus::Cancelled) {
                    // Record premium for filled leg.
                    if st == LegStatus::Filled {
                        if let Some(px) = avg {
                            // Premium sign convention: buys pay premium (negative), sells receive (positive)
                            let sgn = match leg.side { LegSide::Buy => -1.0, LegSide::Sell => 1.0 };
                            total_premium += sgn * px * (filled as f64);
                        }
                    }
                    leg_results.push(LegExecutionResult {
                        tradingsymbol: leg.tradingsymbol.clone(),
                        order_id: Some(oid.clone()),
                        status: st,
                        filled_qty: filled,
                        fill_price: avg,
                        error: None,
                    });
                    break;
                }

                // Treat partial fill as a terminal failure for atomic strategy.
                if st == LegStatus::PartiallyFilled {
                    leg_results.push(LegExecutionResult {
                        tradingsymbol: leg.tradingsymbol.clone(),
                        order_id: Some(oid.clone()),
                        status: st,
                        filled_qty: filled,
                        fill_price: avg,
                        error: Some(format!("partial fill: {}/{}", filled, qty)),
                    });
                    break;
                }
            }

            // If this leg did not fill, rollback.
            let this_ok = leg_results.last().map(|r| r.status == LegStatus::Filled).unwrap_or(false);
            if !this_ok {
                self.rollback(&order.legs, &placed_ids, &leg_results).await;
                return Ok(MultiLegResult {
                    strategy_name: order.strategy_name.clone(),
                    leg_results,
                    all_filled: false,
                    total_premium,
                });
            }
        }

        Ok(MultiLegResult {
            strategy_name: order.strategy_name.clone(),
            leg_results,
            all_filled: true,
            total_premium,
        })
    }

    async fn rollback(
        &mut self,
        legs: &[LegOrder],
        placed_ids: &[String],
        leg_results: &[LegExecutionResult],
    ) {
        // 1) Cancel any active orders.
        for oid in placed_ids {
            self.sim.cancel_order(oid);
        }

        // 2) Optional hedge: for any already-filled legs, send a reverse market order.
        if !self.policy.hedge_on_failure {
            return;
        }

        for (idx, res) in leg_results.iter().enumerate() {
            if res.status != LegStatus::Filled {
                continue;
            }
            let Some(oid) = res.order_id.as_deref() else { continue; };
            let Some((_, _, filled, _)) = self.sim.get_order(oid) else { continue; };
            if filled == 0 {
                continue;
            }

            let leg = &legs[idx];
            let hedge_side = match leg.side { LegSide::Buy => LegSide::Sell, LegSide::Sell => LegSide::Buy };
            let hedge_leg = LegOrder {
                tradingsymbol: leg.tradingsymbol.clone(),
                exchange: leg.exchange.clone(),
                side: hedge_side,
                quantity: filled,
                order_type: LegOrderType::Market,
                price: None,
            };
            let hedge_oid = self.sim.place_order(&hedge_leg, self.sim.now());
            // Best-effort: immediately try to fill on current quote if present.
            // We do not wait here; the test harness can enforce hedging completion.
            let _ = hedge_oid;
        }
    }

    /// Execute with feed and collect stats (Patch 2 entrypoint).
    /// Wire stats to BacktestReport for certification.
    ///
    /// # Errors
    /// Returns `KiteSimError::DepthSequenceGap` if a depth sequence gap is detected.
    pub async fn execute_with_feed_and_stats(
        &mut self,
        order: &MultiLegOrder,
        feed: &mut crate::replay::ReplayFeed,
    ) -> Result<(MultiLegResult, KiteSimRunStats), KiteSimError> {
        let mut stats = KiteSimRunStats::default();

        let result = self.execute_with_feed(order, feed).await?;

        if !result.all_filled {
            stats.rollbacks += 1;
        }

        // TODO (Patch 3): Populate slippage_samples_bps from fill prices vs mid
        // TODO (Patch 3): Populate timeouts from reject_if_no_quote_after

        Ok((result, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{ReplayFeed, ReplayEvent};
    use chrono::TimeZone;

    /// Integration-level test (within crate): a two-leg order where the second
    /// leg can only partially fill should trigger rollback and report failure.
    #[tokio::test]
    async fn kitesim_atomic_two_leg_partial_triggers_rollback() {
        let cfg = KiteSimConfig {
            latency: Duration::milliseconds(0),
            allow_partial: true,
            taker_slippage_bps: 0.0,
            adverse_selection_max_bps: 0.0,
            reject_if_no_quote_after: Duration::seconds(10),
            execution_mode: SimExecutionMode::L1Quote,
        };
        let mut sim = KiteSim::new(cfg);

        // Start time.
        let t0 = Utc.with_ymd_and_hms(2025, 1, 2, 9, 30, 0).unwrap();
        sim.now = t0;

        let order = MultiLegOrder {
            strategy_name: "TEST-SPREAD".to_string(),
            legs: vec![
                LegOrder {
                    tradingsymbol: "NIFTY25JAN20000CE".to_string(),
                    exchange: "NFO".to_string(),
                    side: LegSide::Buy,
                    quantity: 50,
                    order_type: LegOrderType::Market,
                    price: None,
                },
                LegOrder {
                    tradingsymbol: "NIFTY25JAN20100CE".to_string(),
                    exchange: "NFO".to_string(),
                    side: LegSide::Sell,
                    quantity: 50,
                    order_type: LegOrderType::Market,
                    price: None,
                },
            ],
            total_margin_required: 0.0,
        };

        // Feed: first leg has enough size, second leg has only 10 contracts available.
        let events = vec![
            ReplayEvent::Quote(QuoteEvent {
                ts: t0 + Duration::milliseconds(10),
                tradingsymbol: "NIFTY25JAN20000CE".to_string(),
                bid: 100.0,
                ask: 101.0,
                bid_qty: 500,
                ask_qty: 500,
            }),
            ReplayEvent::Quote(QuoteEvent {
                ts: t0 + Duration::milliseconds(20),
                tradingsymbol: "NIFTY25JAN20100CE".to_string(),
                bid: 80.0,
                ask: 81.0,
                bid_qty: 10,
                ask_qty: 10,
            }),
        ];
        let mut feed = ReplayFeed::new(events);

        let policy = AtomicExecPolicy {
            timeout: Duration::seconds(2),
            hedge_on_failure: true,
        };
        let mut coord = MultiLegCoordinator::new(&mut sim, policy);
        let res = coord.execute_with_feed(&order, &mut feed).await
            .expect("execute_with_feed should not fail on L1 mode");

        assert!(!res.all_filled);
        assert_eq!(res.leg_results.len(), 2);
        assert_eq!(res.leg_results[0].status, LegStatus::Filled);
        assert_eq!(res.leg_results[1].status, LegStatus::PartiallyFilled);
        assert!(res.leg_results[1].error.as_ref().unwrap().contains("partial fill"));
    }

    /// P1.3: Golden determinism test - same input always produces identical output.
    /// This verifies that replay is completely deterministic.
    #[tokio::test]
    async fn kitesim_l2_determinism_golden_test() {
        use crate::replay::DepthEvent;
        use crate::replay::DepthLevel;
        use kubera_models::IntegrityTier;

        // Fixed seed depth events (L2 mode)
        let t0 = Utc.with_ymd_and_hms(2025, 1, 2, 9, 30, 0).unwrap();
        let make_depth_events = || {
            vec![
                ReplayEvent::Depth(DepthEvent {
                    ts: t0,
                    tradingsymbol: "BTCUSDT".to_string(),
                    first_update_id: 1000,
                    last_update_id: 1000,
                    price_exponent: -2,
                    qty_exponent: -8,
                    bids: vec![
                        DepthLevel { price: 9000000, qty: 100000000 }, // 90000.00 @ 1.0
                        DepthLevel { price: 8999500, qty: 200000000 }, // 89995.00 @ 2.0
                    ],
                    asks: vec![
                        DepthLevel { price: 9000100, qty: 100000000 }, // 90001.00 @ 1.0
                        DepthLevel { price: 9000500, qty: 200000000 }, // 90005.00 @ 2.0
                    ],
                    is_snapshot: true,
                    integrity_tier: IntegrityTier::Certified,
                    source: None,
                }),
                ReplayEvent::Depth(DepthEvent {
                    ts: t0 + Duration::milliseconds(100),
                    tradingsymbol: "BTCUSDT".to_string(),
                    first_update_id: 1001,
                    last_update_id: 1001,
                    price_exponent: -2,
                    qty_exponent: -8,
                    bids: vec![
                        DepthLevel { price: 9000050, qty: 50000000 }, // 90000.50 @ 0.5
                    ],
                    asks: vec![
                        DepthLevel { price: 9000100, qty: 150000000 }, // 90001.00 @ 1.5
                    ],
                    is_snapshot: false,
                    integrity_tier: IntegrityTier::Certified,
                    source: None,
                }),
            ]
        };

        let order = MultiLegOrder {
            strategy_name: "DETERMINISM-TEST".to_string(),
            legs: vec![LegOrder {
                tradingsymbol: "BTCUSDT".to_string(),
                exchange: "BINANCE".to_string(),
                side: LegSide::Buy,
                quantity: 1, // Buy 1 unit (will be scaled by qty_exponent)
                order_type: LegOrderType::Market,
                price: None,
            }],
            total_margin_required: 0.0,
        };

        let policy = AtomicExecPolicy {
            timeout: Duration::seconds(2),
            hedge_on_failure: false,
        };

        // Run 1
        let mut sim1 = KiteSim::new(KiteSimConfig {
            latency: Duration::milliseconds(0),
            allow_partial: true,
            taker_slippage_bps: 0.0,
            adverse_selection_max_bps: 0.0,
            reject_if_no_quote_after: Duration::seconds(10),
            execution_mode: SimExecutionMode::L2Book,
        });
        let mut feed1 = ReplayFeed::new(make_depth_events());
        let mut coord1 = MultiLegCoordinator::new(&mut sim1, policy.clone());
        let res1 = coord1.execute_with_feed(&order, &mut feed1).await
            .expect("run 1 should succeed");

        // Run 2 (identical input)
        let mut sim2 = KiteSim::new(KiteSimConfig {
            latency: Duration::milliseconds(0),
            allow_partial: true,
            taker_slippage_bps: 0.0,
            adverse_selection_max_bps: 0.0,
            reject_if_no_quote_after: Duration::seconds(10),
            execution_mode: SimExecutionMode::L2Book,
        });
        let mut feed2 = ReplayFeed::new(make_depth_events());
        let mut coord2 = MultiLegCoordinator::new(&mut sim2, policy.clone());
        let res2 = coord2.execute_with_feed(&order, &mut feed2).await
            .expect("run 2 should succeed");

        // Golden assertion: results must be IDENTICAL
        assert_eq!(res1.all_filled, res2.all_filled, "all_filled must match");
        assert_eq!(res1.leg_results.len(), res2.leg_results.len(), "leg count must match");

        for (lr1, lr2) in res1.leg_results.iter().zip(res2.leg_results.iter()) {
            assert_eq!(lr1.status, lr2.status, "status must match");
            assert_eq!(lr1.filled_qty, lr2.filled_qty, "filled_qty must match");
            assert_eq!(lr1.fill_price, lr2.fill_price, "fill_price must match exactly");
        }
    }

    /// P1.4: Gap detection test - missing depth update causes hard fail.
    #[tokio::test]
    async fn kitesim_l2_gap_detection_hard_fail() {
        use crate::replay::DepthEvent;
        use crate::replay::DepthLevel;
        use kubera_models::IntegrityTier;

        let t0 = Utc.with_ymd_and_hms(2025, 1, 2, 9, 30, 0).unwrap();

        // Depth events with a GAP: snapshot at 1000, then diff at 1002 (missing 1001)
        let events_with_gap = vec![
            ReplayEvent::Depth(DepthEvent {
                ts: t0,
                tradingsymbol: "BTCUSDT".to_string(),
                first_update_id: 1000,
                last_update_id: 1000,
                price_exponent: -2,
                qty_exponent: -8,
                bids: vec![DepthLevel { price: 9000000, qty: 100000000 }],
                asks: vec![DepthLevel { price: 9000100, qty: 100000000 }],
                is_snapshot: true,
                integrity_tier: IntegrityTier::Certified,
                source: None,
            }),
            // GAP: first_update_id=1002 but last was 1000, expecting 1001
            ReplayEvent::Depth(DepthEvent {
                ts: t0 + Duration::milliseconds(100),
                tradingsymbol: "BTCUSDT".to_string(),
                first_update_id: 1002, // GAP!
                last_update_id: 1002,
                price_exponent: -2,
                qty_exponent: -8,
                bids: vec![DepthLevel { price: 9000050, qty: 50000000 }],
                asks: vec![],
                is_snapshot: false,
                integrity_tier: IntegrityTier::Certified,
                source: None,
            }),
        ];

        let mut sim = KiteSim::new(KiteSimConfig {
            latency: Duration::milliseconds(0),
            allow_partial: true,
            taker_slippage_bps: 0.0,
            adverse_selection_max_bps: 0.0,
            reject_if_no_quote_after: Duration::seconds(10),
            execution_mode: SimExecutionMode::L2Book,
        });

        // Manually process events to test gap detection
        let mut feed = ReplayFeed::new(events_with_gap);

        // First event (snapshot) should succeed
        let ev1 = feed.next().unwrap();
        let result1 = sim.on_event(ev1);
        assert!(result1.is_ok(), "snapshot should succeed");

        // Second event (with gap) should HARD FAIL
        let ev2 = feed.next().unwrap();
        let result2 = sim.on_event(ev2);

        assert!(result2.is_err(), "gap should cause hard failure");
        match result2 {
            Err(KiteSimError::DepthSequenceGap { symbol, expected, actual }) => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(expected, 1001, "expected update ID should be 1001");
                assert_eq!(actual, 1002, "actual update ID should be 1002");
            }
            _ => panic!("expected DepthSequenceGap error"),
        }
    }
}
