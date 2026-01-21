//! # KiteSim: Offline Zerodha Options Execution Simulator
//!
//! This module provides a **quote-driven** execution simulator intended for
//! *strategy + execution* validation before any live deployment.
//!
//! Scope (intentional):
//! - L1 quote driven (best bid/ask + sizes)
//! - Market + Limit orders
//! - Configurable latency and partial-fill behavior
//! - Atomic multi-leg execution coordinator with rollback and optional hedging
//!
//! Non-goals (for the skeleton):
//! - Full order book / L2 depth
//! - Exchange auction phases / freeze quantities
//! - Sophisticated adverse selection models

use crate::execution::{LegOrder, LegOrderType, LegSide, LegStatus, MultiLegOrder, MultiLegResult, LegExecutionResult};
use crate::replay::{QuoteEvent, ReplayEvent};
use crate::specs::SpecStore;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
}

impl Default for KiteSimConfig {
    fn default() -> Self {
        Self {
            latency: Duration::milliseconds(150),
            allow_partial: true,
            taker_slippage_bps: 0.0,
            adverse_selection_max_bps: 0.0,
            reject_if_no_quote_after: Duration::seconds(10),
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
}

impl SimOrder {
    fn remaining(&self) -> u32 {
        self.qty.saturating_sub(self.filled_qty)
    }
}

/// Primary simulator: accepts quote events and matches eligible orders.
pub struct KiteSim {
    cfg: KiteSimConfig,
    specs: Option<SpecStore>,
    last_quotes: HashMap<String, Quote>,
    orders: HashMap<String, SimOrder>,
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
            orders: HashMap::new(),
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

    /// Patch 2 hook: attempt final reconciliation for timeout / race simulation.
    /// Returns Some(result) if order was filled during the cancel window.
    pub fn final_reconcile(&mut self, _order_id: &str) -> Option<LegExecutionResult> {
        // Stub: Patch 3 will implement "filled while cancelling" behavior.
        None
    }

    /// Advances simulator time and processes a single replay event.
    pub fn on_event(&mut self, event: ReplayEvent) {
        self.now = event.ts();
        match event {
            ReplayEvent::Quote(q) => self.on_quote(q),
        }
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

    fn try_fill_order(&mut self, order_id: &str, q: Quote) {
        let Some(o) = self.orders.get_mut(order_id) else { return; };
        let rem = o.remaining();
        if rem == 0 {
            o.status = LegStatus::Filled;
            return;
        }

        // Determine executable price and visible quantity.
        let (px, visible_qty) = match o.side {
            LegSide::Buy => (q.ask, q.ask_qty),
            LegSide::Sell => (q.bid, q.bid_qty),
        };

        // Limit price check.
        if o.order_type == LegOrderType::Limit {
            if let Some(lim) = o.limit_price {
                match o.side {
                    LegSide::Buy if px > lim => return,
                    LegSide::Sell if px < lim => return,
                    _ => {}
                }
            } else {
                // Malformed: limit order without price.
                o.status = LegStatus::Rejected;
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
        let fill_px = match o.side {
            LegSide::Buy => px + slip,
            LegSide::Sell => px - slip,
        };

        // Update VWAP.
        let prev_notional = o.avg_price.unwrap_or(0.0) * (o.filled_qty as f64);
        let add_notional = fill_px * (fill_qty as f64);
        let new_filled = o.filled_qty + fill_qty;
        o.avg_price = Some((prev_notional + add_notional) / (new_filled as f64));
        o.filled_qty = new_filled;

        if o.filled_qty == o.qty {
            o.status = LegStatus::Filled;
        } else {
            o.status = LegStatus::PartiallyFilled;
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
