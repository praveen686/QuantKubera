//! # Project AEON: Adaptive Entropy-Optimized Network
//!
//! AEON is a hierarchical flagship strategy that fuses information theory (BIF),
//! structural analysis (FTI), and online meta-allocation (HYDRA).
//!
//! ## Architecture
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        AEON COORDINATOR                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  LEVEL 1: INFORMATIONAL SENTRY (BIF)                            │
//! │  ├─ Measures market predictability via mutual information       │
//! │  └─ Gates trading when entropy is too high                      │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  LEVEL 2: STRUCTURAL FILTER (FTI)                               │
//! │  ├─ Classifies market regime (Trend vs Mean-Reversion)          │
//! │  └─ Routes signals to appropriate expert strategies             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  LEVEL 3: EXPERT ORCHESTRATION (HYDRA)                          │
//! │  └─ Executes via multi-expert ensemble with online learning     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;
use std::collections::VecDeque;
use tracing::{info, debug, warn};
use kubera_models::{MarketEvent, MarketPayload, Side, SignalEvent, OrderEvent, OrderPayload, OrderStatus};

use crate::{Strategy, EventBus};
use crate::aeon::{AeonConfig, BifIndicator};
use crate::aeon::fti::FtiEngine;
use crate::hydra::{HydraStrategy, HydraConfig};

/// AEON: The flagship meta-strategy combining information theory and structural analysis.
pub struct AeonStrategy {
    name: String,
    bus: Option<Arc<EventBus>>,
    config: AeonConfig,

    // Core Engines
    bif_engine: BifIndicator,
    fti_engine: FtiEngine,

    // Internal state
    prices: VecDeque<f64>,
    /// Current net position (positive = long, negative = short)
    position: f64,
    /// Target position from AEON's gating logic
    target_position: f64,
    /// Last symbol processed
    last_symbol: String,
    /// Last price for position sizing
    last_price: f64,
    /// Cumulative PnL tracking
    realized_pnl: f64,
    /// Peak equity for drawdown monitoring
    peak_equity: f64,

    // Expert Management (Reusing HYDRA's ensemble)
    hydra: HydraStrategy,
}

impl AeonStrategy {
    pub fn new(config: AeonConfig, hydra_config: HydraConfig) -> Self {
        let bif_engine = BifIndicator::new(&config);
        let fti_engine = FtiEngine::new(
            config.fti_min_period,
            config.fti_max_period,
            config.fti_half_length,
            config.lookback,
            config.fti_beta,
            config.fti_noise_cut,
        );

        Self {
            name: "AEON-v1".to_string(),
            bus: None,
            bif_engine,
            fti_engine,
            prices: VecDeque::with_capacity(config.lookback + config.fti_half_length + 1),
            position: 0.0,
            target_position: 0.0,
            last_symbol: String::new(),
            last_price: 0.0,
            realized_pnl: 0.0,
            peak_equity: 0.0,
            hydra: HydraStrategy::with_config(hydra_config),
            config,
        }
    }

    /// Emits a trading signal to the event bus.
    ///
    /// # Position Management
    /// - Tracks position changes from signals
    /// - Calculates appropriate quantity based on position delta
    fn emit_signal(&mut self, symbol: &str, side: Side, price: f64, quantity: f64) {
        if let Some(bus) = &self.bus {
            let signal = SignalEvent {
                timestamp: chrono::Utc::now(),
                strategy_id: self.name.clone(),
                symbol: symbol.to_string(),
                side: side.clone(),
                price,
                quantity,
                intent_id: None,
                // HFT V2: Default book context (AEON doesn't track LOB)
                decision_bid: price,
                decision_ask: price,
                decision_mid: price,
                spread_bps: 0.0,
                book_ts_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                expected_edge_bps: 0.0,
            };

            // Update target position based on signal
            match side {
                Side::Buy => self.target_position += quantity,
                Side::Sell => self.target_position -= quantity,
            }

            debug!(
                "[AEON] Emitting signal: {} {} @ {:.2}, qty={:.4}, target_pos={:.4}",
                match side { Side::Buy => "BUY", Side::Sell => "SELL" },
                symbol, price, quantity, self.target_position
            );

            if let Err(e) = bus.publish_signal_sync(signal) {
                warn!("[AEON] Failed to publish signal: {}", e);
            }
        }
    }

    /// Manages position based on AEON gating output.
    ///
    /// When AEON gates are open (high predictability + clear structure):
    /// - Allows HYDRA to execute with full conviction
    /// - Tracks position for risk management
    ///
    /// When AEON gates are closed:
    /// - Emits flattening signals if position is open
    /// - Prevents new entries
    fn manage_position(&mut self, should_trade: bool, fti_score: f64, price: f64, symbol: &str) {
        let is_trend = fti_score > self.config.fti_trend_threshold;
        let is_mean_rev = fti_score < self.config.fti_mean_rev_threshold;

        if !should_trade {
            // AEON gates closed - flatten any open position
            if self.position.abs() > 0.01 {
                let flatten_qty = self.position.abs();
                let side = if self.position > 0.0 { Side::Sell } else { Side::Buy };

                info!(
                    "[AEON] Gating closed - flattening position: {} {:.4} @ {:.2}",
                    match side { Side::Buy => "BUY", Side::Sell => "SELL" },
                    flatten_qty, price
                );

                self.emit_signal(symbol, side, price, flatten_qty);
            }
            return;
        }

        // AEON gates open - determine position sizing based on regime
        let regime_multiplier = if is_trend {
            1.2  // Boost in trending regimes
        } else if is_mean_rev {
            1.0  // Normal in mean-reverting
        } else {
            0.5  // Reduce in unclear regimes
        };

        // Let HYDRA handle the actual signal generation with regime context
        // AEON's role is gating and position oversight
        debug!(
            "[AEON] Gates OPEN - regime_mult={:.2}, is_trend={}, is_mean_rev={}, current_pos={:.4}",
            regime_multiplier, is_trend, is_mean_rev, self.position
        );
    }

    /// Returns current position
    pub fn get_position(&self) -> f64 {
        self.position
    }

    /// Returns realized PnL
    pub fn get_realized_pnl(&self) -> f64 {
        self.realized_pnl
    }
}

impl Strategy for AeonStrategy {
    fn on_start(&mut self, bus: Arc<EventBus>) {
        info!("[AEON] Sentry Online. Gating threshold: {}", self.config.bif_predictability_threshold);
        self.bus = Some(bus.clone());
        self.hydra.on_start(bus);
    }

    fn on_tick(&mut self, event: &MarketEvent) {
        let price = match &event.payload {
            MarketPayload::Tick { price, .. } => *price,
            MarketPayload::Trade { price, .. } => *price,
            _ => {
                // Pass-through other events (like L2) to HYDRA without gating
                self.hydra.on_tick(event);
                return;
            }
        };

        // Update state tracking
        self.last_price = price;
        self.last_symbol = event.symbol.clone();

        self.prices.push_back(price);
        if self.prices.len() > self.config.lookback + self.config.fti_half_length {
            self.prices.pop_front();
        }

        if self.prices.len() < self.config.lookback {
            return;
        }

        // --- LEVEL 1: INFORMATIONAL SENTRY (BIF) ---
        let predictability = self.bif_engine.calculate(&self.prices);
        let should_trade = predictability >= self.config.bif_predictability_threshold;

        if !should_trade {
            debug!("[AEON] Market High Entropy ({:.3}). Sentry active - idling.", predictability);
            // Manage position when gates close (may flatten)
            self.manage_position(false, 0.0, price, &event.symbol.clone());
            return;
        }

        // --- LEVEL 2: STRUCTURAL FILTER (FTI) ---
        let fti_score = match self.fti_engine.calculate(&self.prices) {
            Some(s) => s,
            None => return,
        };

        // --- LEVEL 3: DYNAMIC EXPERT ORCHESTRATION ---
        // Propagate predictability to HYDRA for entropy-adjusted scaling
        self.hydra.set_aeon_bif_score(predictability);

        // Update HYDRA experts
        self.hydra.on_tick(event);

        // Gating Logic based on FTI
        let is_trend = fti_score > self.config.fti_trend_threshold;
        let is_mean_rev = fti_score < self.config.fti_mean_rev_threshold;

        debug!("[AEON] Predictability: {:.3} | FTI: {:.2} | Trend: {} | MeanRev: {} | Pos: {:.4}",
            predictability, fti_score, is_trend, is_mean_rev, self.position);

        // Track peak equity for drawdown monitoring
        let current_equity = self.realized_pnl + (self.position * price);
        if current_equity > self.peak_equity {
            self.peak_equity = current_equity;
        }

        if !is_trend && !is_mean_rev {
            debug!("[AEON] Structural Noise. Managing position.");
            self.manage_position(false, fti_score, price, &event.symbol.clone());
            return;
        }

        // Gates are open - allow trading
        self.manage_position(true, fti_score, price, &event.symbol.clone());
    }

    fn on_bar(&mut self, event: &MarketEvent) {
        self.hydra.on_bar(event);
    }

    fn on_fill(&mut self, fill: &OrderEvent) {
        // Update AEON's position tracking from fills
        if let OrderPayload::Update { status, filled_quantity, avg_price, .. } = &fill.payload {
            if *status == OrderStatus::Filled {
                let signed_qty = match fill.side {
                    Side::Buy => *filled_quantity,
                    Side::Sell => -*filled_quantity,
                };

                // Calculate PnL if closing a position
                if self.position.signum() != signed_qty.signum() && self.position.abs() > 0.0 {
                    let close_qty = signed_qty.abs().min(self.position.abs());
                    let entry_price = self.last_price; // Simplified - in production track actual entry
                    let pnl = if self.position > 0.0 {
                        close_qty * (*avg_price - entry_price)
                    } else {
                        close_qty * (entry_price - *avg_price)
                    };
                    self.realized_pnl += pnl;

                    info!(
                        "[AEON] Position closed: PnL={:.2}, Total Realized={:.2}",
                        pnl, self.realized_pnl
                    );
                }

                self.position += signed_qty;

                debug!(
                    "[AEON] Fill received: {} {:.4} @ {:.2}, New Position: {:.4}",
                    if signed_qty > 0.0 { "BUY" } else { "SELL" },
                    filled_quantity, avg_price, self.position
                );
            }
        }

        // Forward to HYDRA for its tracking
        self.hydra.on_fill(fill);
    }

    fn on_stop(&mut self) {
        info!(
            "[AEON] Shutting down. Position: {:.4}, Realized PnL: {:.2}, Peak Equity: {:.2}",
            self.position, self.realized_pnl, self.peak_equity
        );
        self.hydra.on_stop();
    }

    fn name(&self) -> &str {
        &self.name
    }
}
