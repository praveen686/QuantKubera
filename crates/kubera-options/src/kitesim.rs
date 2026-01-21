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
/// Stores the order book along with its exponents for price/qty conversion.
#[derive(Debug, Clone)]
pub struct SimOrderBook {
    /// Price mantissa → quantity mantissa
    bids: std::collections::BTreeMap<i64, i64>,
    /// Price mantissa → quantity mantissa
    asks: std::collections::BTreeMap<i64, i64>,
    /// Price exponent: actual_price = mantissa * 10^price_exponent
    price_exponent: i8,
    /// Quantity exponent: actual_qty = mantissa * 10^qty_exponent
    qty_exponent: i8,
    /// Last processed update ID for gap detection.
    last_update_id: u64,
}

impl SimOrderBook {
    fn new(price_exponent: i8, qty_exponent: i8) -> Self {
        Self {
            bids: std::collections::BTreeMap::new(),
            asks: std::collections::BTreeMap::new(),
            price_exponent,
            qty_exponent,
            last_update_id: 0,
        }
    }

    fn price_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.price_exponent as i32)
    }

    fn qty_to_f64(&self, mantissa: i64) -> f64 {
        mantissa as f64 * 10f64.powi(self.qty_exponent as i32)
    }

    fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(&p, &q)| (self.price_to_f64(p), self.qty_to_f64(q)))
    }

    fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(&p, &q)| (self.price_to_f64(p), self.qty_to_f64(q)))
    }

    fn mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    /// Simulates a market buy, walking through ask levels.
    /// Returns (avg_price, filled_qty) or None if no liquidity.
    fn simulate_market_buy(&self, qty: f64) -> Option<(f64, f64)> {
        let qty_mantissa = (qty / 10f64.powi(self.qty_exponent as i32)).round() as i64;
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

        Some((avg_price, filled_f64))
    }

    /// Simulates a market sell, walking through bid levels.
    /// Returns (avg_price, filled_qty) or None if no liquidity.
    fn simulate_market_sell(&self, qty: f64) -> Option<(f64, f64)> {
        let qty_mantissa = (qty / 10f64.powi(self.qty_exponent as i32)).round() as i64;
        let mut remaining = qty_mantissa;
        let mut total_proceeds = 0i128;
        let mut total_filled = 0i64;

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

        Some((avg_price, filled_f64))
    }

    /// Apply a depth event to the order book.
    fn apply_depth(&mut self, event: &DepthEvent) -> Result<(), String> {
        // Gap detection
        if self.last_update_id > 0 && event.first_update_id != self.last_update_id + 1 {
            return Err(format!(
                "Sequence gap: expected {}, got {}",
                self.last_update_id + 1,
                event.first_update_id
            ));
        }

        // Apply bid updates
        for level in &event.bids {
            if level.qty == 0 {
                self.bids.remove(&level.price);
            } else {
                self.bids.insert(level.price, level.qty);
            }
        }

        // Apply ask updates
        for level in &event.asks {
            if level.qty == 0 {
                self.asks.remove(&level.price);
            } else {
                self.asks.insert(level.price, level.qty);
            }
        }

        self.last_update_id = event.last_update_id;
        Ok(())
    }
}

/// Primary simulator: accepts quote events and matches eligible orders.
pub struct KiteSim {
    cfg: KiteSimConfig,
    specs: Option<SpecStore>,
    last_quotes: HashMap<String, Quote>,
    /// L2 order books per symbol (only used in L2Book mode).
    order_books: HashMap<String, SimOrderBook>,
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
    pub fn ingest_event(&mut self, event: &ReplayEvent) {
        match event {
            ReplayEvent::Quote(q) => self.on_quote(q.clone()),
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
    pub fn on_event(&mut self, event: ReplayEvent) {
        self.now = event.ts();
        match event {
            ReplayEvent::Quote(q) => self.on_quote(q),
            ReplayEvent::Depth(d) => self.on_depth(d),
        }
    }

    /// Process an L2 depth event.
    /// In L2Book mode: updates the order book and triggers matching.
    /// In L1Quote mode: ignored (use on_quote for L1).
    pub fn on_depth(&mut self, d: DepthEvent) {
        if self.cfg.execution_mode != SimExecutionMode::L2Book {
            return; // Ignore depth events in L1 mode
        }

        let symbol = d.tradingsymbol.clone();

        // Get or create the order book for this symbol
        let book = self.order_books.entry(symbol.clone()).or_insert_with(|| {
            SimOrderBook::new(d.price_exponent, d.qty_exponent)
        });

        // Apply the depth update
        if let Err(e) = book.apply_depth(&d) {
            tracing::warn!("Depth update error for {}: {}", symbol, e);
            return;
        }

        // Trigger order matching against the updated book
        self.match_eligible_orders_for_l2(&symbol);
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
        // Attempt immediate match if we already have a quote and we're beyond latency.
        self.match_eligible_orders_for(&leg.tradingsymbol);
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
        let mid = book.mid();

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

            // Simulate the fill using the full order book
            let sim_result = match side {
                LegSide::Buy => book.simulate_market_buy(rem as f64),
                LegSide::Sell => book.simulate_market_sell(rem as f64),
            };

            let Some((sim_avg_price, sim_filled_qty)) = sim_result else {
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
    pub async fn execute_with_feed(
        &mut self,
        order: &MultiLegOrder,
        feed: &mut crate::replay::ReplayFeed,
    ) -> MultiLegResult {
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
                        self.sim.on_event(ev);
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
                return MultiLegResult {
                    strategy_name: order.strategy_name.clone(),
                    leg_results,
                    all_filled: false,
                    total_premium,
                };
            }
        }

        MultiLegResult {
            strategy_name: order.strategy_name.clone(),
            leg_results,
            all_filled: true,
            total_premium,
        }
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
    pub async fn execute_with_feed_and_stats(
        &mut self,
        order: &MultiLegOrder,
        feed: &mut crate::replay::ReplayFeed,
    ) -> (MultiLegResult, KiteSimRunStats) {
        let mut stats = KiteSimRunStats::default();

        let result = self.execute_with_feed(order, feed).await;

        if !result.all_filled {
            stats.rollbacks += 1;
        }

        // TODO (Patch 3): Populate slippage_samples_bps from fill prices vs mid
        // TODO (Patch 3): Populate timeouts from reject_if_no_quote_after

        (result, stats)
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
        let res = coord.execute_with_feed(&order, &mut feed).await;

        assert!(!res.all_filled);
        assert_eq!(res.leg_results.len(), 2);
        assert_eq!(res.leg_results[0].status, LegStatus::Filled);
        assert_eq!(res.leg_results[1].status, LegStatus::PartiallyFilled);
        assert!(res.leg_results[1].error.as_ref().unwrap().contains("partial fill"));
    }
}
