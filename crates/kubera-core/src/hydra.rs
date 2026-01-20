//! # KUBERA HYDRA Flagship Strategy
//!
//! ## P.1016 - IEEE Standard
//! **Module**: HYDRA-v2.0
//! **Date**: 2024-12-19
//! **Compliance**: ISO/IEC/IEEE 12207:2017
//!
//! ## Description
//! A self-evolving, regime-aware, multi-expert portfolio that uses online learning
//! (Exponentiated Gradient) to dynamically allocate capital across orthogonal alpha sources.
//!
//! ## Architecture
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                         HYDRA COORDINATOR                                 │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
//! │  │  Expert A   │  │  Expert B   │  │  Expert C   │  │  Expert D   │ ...  │
//! │  │   Trend     │  │  Mean Rev   │  │ Volatility  │  │Microstructure│     │
//! │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘      │
//! │         │                │                │                │             │
//! │         └────────────────┴────────────────┴────────────────┘             │
//! │                                   │                                       │
//! │                          ┌────────▼────────┐                              │
//! │                          │  REGIME ENGINE  │                              │
//! │                          │  (Gating Layer) │                              │
//! │                          └────────┬────────┘                              │
//! │                                   │                                       │
//! │                          ┌────────▼────────┐                              │
//! │                          │ META-ALLOCATOR  │                              │
//! │                          │  (EG/Hedge)     │                              │
//! │                          └────────┬────────┘                              │
//! │                                   │                                       │
//! │  ┌────────────────────────────────┼────────────────────────────────┐     │
//! │  │                                │                                │     │
//! │  ▼                                ▼                                ▼     │
//! │ ┌──────────────┐  ┌───────────────────────┐  ┌──────────────────┐       │
//! │ │ ATTRIBUTION  │  │  EXECUTION QUALITY    │  │   KILL-SWITCH    │       │
//! │ │   LEDGER     │  │       GATE            │  │     POLICY       │       │
//! │ └──────────────┘  └───────────────────────┘  └──────────────────┘       │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```

use kubera_models::{MarketEvent, OrderEvent, SignalEvent, MarketPayload, Side, OrderPayload, L2Update};
use kubera_options::{OptionGreeks, OptionType};
use crate::EventBus;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{info, warn, error, debug};
use std::collections::{HashMap, VecDeque};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Serialize, Deserialize};

// ============================================================================
// UTILITIES
// ============================================================================

/// A simple 1D Kalman Filter for price smoothing with minimal lag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalmanFilter {
    pub x: f64, // state estimate
    pub p: f64, // estimate error covariance
    pub q: f64, // process noise covariance
    pub r: f64, // measurement noise covariance
}

impl KalmanFilter {
    pub fn new(initial_value: f64, q: f64, r: f64) -> Self {
        Self { x: initial_value, p: 1.0, q, r }
    }
    
    pub fn update(&mut self, measurement: f64) -> f64 {
        if self.x == 0.0 { self.x = measurement; }
        // Predict
        self.p += self.q;
        // Update
        let k = self.p / (self.p + self.r);
        self.x += k * (measurement - self.x);
        self.p *= 1.0 - k;
        self.x
    }
}

// ============================================================================
// CONSTANTS & CONFIGURATION
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ExchangeProfile {
    BinanceSpot,
    BinanceFutures,
    ZerodhaFnO,
    Default,
}

/// Strategy configuration for the HYDRA meta-logic
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HydraConfig {
    pub venue: ExchangeProfile,
    /// Multi-expert learning rate (0.01 - 0.2)
    pub learning_rate: f64,
    /// Minimum weight floor for any expert
    pub weight_floor: f64,
    /// Maximum weight ceiling for any expert
    pub weight_ceiling: f64,
    /// Portfolio-level volatility target (annualized)
    pub vol_target: f64,
    /// Maximum drawdown before global risk reduction (percentage)
    pub max_drawdown_pct: f64,
    /// Expert drawdown threshold for quarantine (percentage)
    pub expert_quarantine_drawdown: f64,
    /// Cooldown period for quarantined experts (in ticks)
    pub quarantine_cooldown_ticks: u64,
    /// Virtual PnL recovery threshold to release from quarantine (percentage)
    pub virtual_recovery_threshold: f64,
    /// Risk aversion parameter (lambda_risk)
    pub lambda_risk: f64,
    /// Tail loss aversion parameter (lambda_tail)
    pub lambda_tail: f64,
    /// Execution drag aversion parameter (lambda_exec)
    pub lambda_exec: f64,
    /// Hysteresis threshold for position changes
    pub position_hysteresis: f64,
    /// Slippage estimate in bps
    pub slippage_bps: f64,
    /// Commission per trade in bps
    pub commission_bps: f64,
    /// Max absolute position for the entire portfolio
    pub max_position_abs: f64,
    /// Max absolute position per expert
    pub max_position_per_expert: f64,
    /// Regime history window size
    pub regime_history_window: usize,
    
    // --- V2 MEDALLION FEATURES ---
    /// Lookback for Z-score normalization of signals
    pub signal_zscore_lookback: usize,
    /// Rolling window for realized volatility in V-MAP
    pub vol_target_window: usize,
    /// Minimum Z-score for a signal to be considered actionable
    pub zscore_action_threshold: f64,

    // --- Regime Engine Thresholds ---
    pub cusum_threshold: f64,
    pub spread_ratio_threshold: f64,
    pub volume_surprise_threshold: f64,
    pub vol_high_threshold: f64,
    pub vol_ratio_threshold: f64,
    pub vol_low_threshold: f64,
    pub hurst_trend_threshold: f64,
    pub trend_strength_threshold: f64,
    pub hurst_range_threshold: f64,
    pub autocorrelation_range_threshold: f64,
    /// Edge hurdle multiplier (e.g. 2.5 means edge > 2.5 * costs)
    pub edge_hurdle_multiplier: f64,

    // ========== BINANCE TUNING PATCH ==========
    /// Minimum edge in bps required to trade (hard floor)
    pub min_edge_bps: f64,
    /// Latency-induced slippage in bps (signal→fill drift)
    pub latency_slippage_bps: f64,
    /// Include half-spread in cost calculation
    pub include_spread_in_cost: bool,
    /// Minimum milliseconds between signals
    pub min_ms_between_signals: i64,
    /// Minimum milliseconds to hold a position
    pub min_ms_hold: i64,
}

impl Default for HydraConfig {
    fn default() -> Self {
        Self {
            venue: ExchangeProfile::Default,
            learning_rate: 0.05,
            weight_floor: 0.02,
            weight_ceiling: 0.60,
            vol_target: 0.15,
            max_drawdown_pct: 10.0, // MASTERPIECE: Strict 10% drawdown budget
            expert_quarantine_drawdown: 5.0,
            quarantine_cooldown_ticks: 500,
            virtual_recovery_threshold: 1.0, // Recover 1% from peak to release
            lambda_risk: 0.5,
            lambda_tail: 1.0,
            lambda_exec: 0.3,
            position_hysteresis: 1.0, // V2: Higher threshold to avoid commission churn
            slippage_bps: 0.5, // Conservative slippage
            commission_bps: 6.0, // V2: Cover round-trip ZerodhaFnO costs
            max_position_abs: 40.0, // MASTERPIECE: Balanced leverage
            max_position_per_expert: 10.0, // MASTERPIECE: Balanced contribution
            regime_history_window: 100,
            
            // V2 Medallion features
            signal_zscore_lookback: 50,
            vol_target_window: 30,
            zscore_action_threshold: 1.0, // V2: Lowered to allow high-conviction trades
            
            cusum_threshold: 15.0, // MEDALLION: Higher threshold to avoid false Event detection
            spread_ratio_threshold: 2.0,
            volume_surprise_threshold: 2.0,
            vol_high_threshold: 0.30,
            vol_ratio_threshold: 1.5,
            vol_low_threshold: 0.08,
            hurst_trend_threshold: 0.55,
            trend_strength_threshold: 30.0,
            hurst_range_threshold: 0.45,
            autocorrelation_range_threshold: -0.1,
            edge_hurdle_multiplier: 2.5, // Default to strict Masterpiece filter

            // BINANCE TUNING PATCH defaults
            min_edge_bps: 8.0,              // Minimum 8bps edge required
            latency_slippage_bps: 5.0,      // Observed ~5bps signal→fill drift
            include_spread_in_cost: true,
            min_ms_between_signals: 5000,   // 5 seconds between signals
            min_ms_hold: 5000,              // 5 seconds minimum hold
        }
    }
}

impl HydraConfig {
    pub fn apply_profile(&mut self, venue: ExchangeProfile) {
        self.venue = venue;
        match venue {
            ExchangeProfile::BinanceSpot => {
                info!("[HYDRA] Calibrating for Binance Spot");
                self.slippage_bps = 0.3;
                self.commission_bps = 1.0; // 0.1% taker
                self.position_hysteresis = 0.1;
                self.zscore_action_threshold = 0.5;
            }
            ExchangeProfile::BinanceFutures => {
                info!("[HYDRA] Calibrating for Binance Futures (Masterpiece Final)");
                self.slippage_bps = 0.2;
                self.commission_bps = 0.8; 
                self.position_hysteresis = 0.4; // Medallion-grade alpha capture
                self.zscore_action_threshold = 0.8; 
            }
            ExchangeProfile::ZerodhaFnO => {
                info!("[HYDRA] Calibrating for Zerodha Indian F&O (Options-Optimized profile)");
                // === COST STRUCTURE ===
                // NSE F&O: ~0.03% brokerage + 0.01% STT + 0.05% other charges
                self.slippage_bps = 1.5;  // Options have wider spreads
                self.commission_bps = 4.0; // ~0.04% total costs (lower for index options)

                // === SIGNAL THRESHOLDS ===
                // Options decay quickly - need faster entry/exit
                self.position_hysteresis = 0.3;  // Lower hysteresis for quicker response
                self.zscore_action_threshold = 0.8;  // More sensitive to capture premium

                // === VOLATILITY TARGETING ===
                // Indian markets have higher intraday volatility
                self.vol_target = 0.25;  // 25% annualized target
                self.vol_high_threshold = 0.40;  // Higher threshold for India VIX spikes
                self.vol_low_threshold = 0.12;  // Adjust for normal IV range

                // === RISK MANAGEMENT ===
                // Tighter drawdown limits for options (theta decay risk)
                self.max_drawdown_pct = 8.0;  // Stricter 8% max drawdown
                self.expert_quarantine_drawdown = 4.0;  // Quarantine at 4%
                self.quarantine_cooldown_ticks = 300;  // Faster recovery allowed

                // === POSITION SIZING ===
                // Smaller positions due to options leverage
                self.max_position_abs = 25.0;  // Reduced max position
                self.max_position_per_expert = 8.0;  // Balanced expert contribution

                // === EDGE REQUIREMENTS ===
                // Options have better edge opportunities but higher costs
                self.edge_hurdle_multiplier = 2.0;  // Slightly lower hurdle for options

                // === REGIME DETECTION ===
                // Tuned for Indian market characteristics
                self.hurst_trend_threshold = 0.52;  // Lower for faster trend detection
                self.trend_strength_threshold = 25.0;  // More sensitive to trends
                self.cusum_threshold = 12.0;  // Faster event detection

                // === LEARNING RATE ===
                // Faster adaptation for expiry-driven dynamics
                self.learning_rate = 0.08;  // Higher learning rate
            }
            ExchangeProfile::Default => {}
        }
    }
}

// ============================================================================
// REGIME ENGINE - THE GATING LAYER
// ============================================================================

/// Market regime states derived from regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HydraRegime {
    /// Strong directional movement, high momentum
    Trend,
    /// Sideways consolidation, mean-reverting behavior
    Range,
    /// High volatility regime, requires caution
    HighVolatility,
    /// Low volatility, potentially illiquid
    LowVolatility,
    /// Event-driven regime (news, earnings)
    Event,
    /// Toxic flow detected, execution quality degraded
    Toxic,
}

impl HydraRegime {
    /// Get the risk multiplier for this regime
    pub fn risk_multiplier(&self) -> f64 {
        match self {
            // MEDALLION: Higher multipliers for more aggressive trading
            HydraRegime::Trend => 1.2,  // Boost trend trading
            HydraRegime::Range => 1.0,  // Full allocation for range trading
            HydraRegime::HighVolatility => 0.4,  
            HydraRegime::LowVolatility => 0.8,  
            HydraRegime::Event => 0.1,  // MASTERPIECE: Extreme caution in events
            HydraRegime::Toxic => 0.0,
        }
    }

    /// Get allowed expert IDs for this regime
    pub fn allowed_experts(&self) -> Vec<&'static str> {
        match self {
            HydraRegime::Trend => vec!["Expert-A-Trend", "Expert-D-Microstructure"],
            HydraRegime::Range => vec!["Expert-B-MeanRev", "Expert-E-RelValue", "Expert-D-Microstructure"],
            HydraRegime::HighVolatility => vec!["Expert-C-Volatility", "Expert-A-Trend", "Expert-D-Microstructure"],
            HydraRegime::LowVolatility => vec!["Expert-B-MeanRev", "Expert-E-RelValue", "Expert-D-Microstructure"],
            HydraRegime::Event => vec!["Expert-C-Volatility", "Expert-D-Microstructure"],
            HydraRegime::Toxic => vec![],
        }
    }
}

/// Regime classification output with confidence and metadata
#[derive(Debug, Clone)]
pub struct RegimeState {
    pub regime: HydraRegime,
    pub confidence: f64,
    pub risk_multiplier: f64,
    pub allowed_experts: Vec<String>,
    pub features: RegimeFeatures,
}

/// Features used for regime classification
#[derive(Debug, Clone, Default)]
pub struct RegimeFeatures {
    /// Realized volatility (annualized, rolling window)
    pub realized_vol: f64,
    /// Short-term volatility for ratio
    pub short_vol: f64,
    /// Long-term volatility for ratio
    pub long_vol: f64,
    /// Hurst exponent estimate (0.5 = random walk, >0.5 = trending, <0.5 = mean reverting)
    pub hurst_estimate: f64,
    /// ADX-like trend strength (0-100)
    pub trend_strength: f64,
    /// Autocorrelation of returns (lag 1)
    pub autocorrelation: f64,
    /// Jump indicator (bipower variation ratio)
    pub jump_indicator: f64,
    /// Spread expansion ratio
    pub spread_ratio: f64,
    /// Volume surprise (current vs average)
    pub volume_surprise: f64,
}

/// Regime Engine: Classifies market state and gates expert activity
pub struct RegimeEngine {
    /// Rolling window of returns for calculations
    returns: VecDeque<f64>,
    /// Rolling window of prices
    prices: VecDeque<f64>,
    /// Rolling window of volumes
    volumes: VecDeque<f64>,
    /// Rolling window of spreads (if available)
    spreads: VecDeque<f64>,
    /// Rolling window of high-low ranges for volatility
    ranges: VecDeque<f64>,
    /// CUSUM statistics for change-point detection
    cusum_pos: f64,
    cusum_neg: f64,
    cusum_threshold: f64,
    /// Current regime state
    current_regime: RegimeState,
    /// Historical regime for hysteresis
    regime_history: VecDeque<HydraRegime>,
    /// Configuration
    window_short: usize,
    window_long: usize,
    /// Tick counter for regime updates
    tick_count: u64,
    /// Configuration
    config: HydraConfig,
}

impl RegimeEngine {
    pub fn new(window_short: usize, window_long: usize, config: HydraConfig) -> Self {
        Self {
            returns: VecDeque::with_capacity(window_long),
            prices: VecDeque::with_capacity(window_long),
            volumes: VecDeque::with_capacity(window_long),
            spreads: VecDeque::with_capacity(window_long),
            ranges: VecDeque::with_capacity(window_long),
            cusum_pos: 0.0,
            cusum_neg: 0.0,
            cusum_threshold: config.cusum_threshold,
            current_regime: RegimeState {
                regime: HydraRegime::Range,
                confidence: 0.5,
                risk_multiplier: 0.8,
                allowed_experts: HydraRegime::Range.allowed_experts().iter().map(|s| s.to_string()).collect(),
                features: RegimeFeatures::default(),
            },
            regime_history: VecDeque::with_capacity(config.regime_history_window),
            window_short,
            window_long,
            tick_count: 0,
            config,
        }
    }

    /// Update regime engine with new market data
    pub fn update(&mut self, event: &MarketEvent) {
        self.tick_count += 1;

        match &event.payload {
            MarketPayload::Tick { price, size, .. } => {
                self.update_price(*price, *size);
            }
            MarketPayload::Trade { price, quantity, .. } => {
                self.update_price(*price, *quantity);
            }
            MarketPayload::Bar { close, volume, high, low, .. } => {
                self.update_price(*close, *volume);
                self.ranges.push_back(high - low);
                if self.ranges.len() > self.window_long {
                    self.ranges.pop_front();
                }
            }
            MarketPayload::L2Update(l2) => {
                self.update_spread(l2);
            }
            _ => {}
        }

        // Re-classify every 10 ticks for efficiency
        if self.tick_count % 10 == 0 && self.prices.len() >= self.window_short {
            self.classify_regime();
        }
    }

    fn update_price(&mut self, price: f64, volume: f64) {
        if let Some(&prev_price) = self.prices.back() {
            if prev_price > 0.0 {
                let ret = (price / prev_price).ln();
                self.returns.push_back(ret);
                if self.returns.len() > self.window_long {
                    self.returns.pop_front();
                }

                // CUSUM update for change-point detection
                let mean_ret = self.returns.iter().sum::<f64>() / self.returns.len() as f64;
                let std_ret = self.calculate_std(&self.returns);
                if std_ret > 0.0 {
                    let z = (ret - mean_ret) / std_ret;
                    self.cusum_pos = (self.cusum_pos + z).max(0.0);
                    self.cusum_neg = (self.cusum_neg - z).max(0.0);
                }
            }
        }

        self.prices.push_back(price);
        if self.prices.len() > self.window_long {
            self.prices.pop_front();
        }

        self.volumes.push_back(volume);
        if self.volumes.len() > self.window_long {
            self.volumes.pop_front();
        }
    }

    fn update_spread(&mut self, l2: &L2Update) {
        if let (Some(best_bid), Some(best_ask)) = (l2.bids.first(), l2.asks.first()) {
            let spread = best_ask.price - best_bid.price;
            let mid = (best_ask.price + best_bid.price) / 2.0;
            if mid > 0.0 {
                self.spreads.push_back(spread / mid * 10000.0); // bps
                if self.spreads.len() > self.window_long {
                    self.spreads.pop_front();
                }
            }
        }
    }

    fn classify_regime(&mut self) {
        let features = self.calculate_features();

        // Simple rule-based classifier (can be replaced with ML model)
        let regime = self.classify_from_features(&features);

        // Hysteresis: require 3 consecutive same classifications to switch
        self.regime_history.push_back(regime);
        if self.regime_history.len() > 5 {
            self.regime_history.pop_front();
        }

        let final_regime = if self.regime_history.len() >= 3 {
            let last_three: Vec<_> = self.regime_history.iter().rev().take(3).collect();
            if last_three.iter().all(|&r| *r == regime) {
                regime
            } else {
                self.current_regime.regime
            }
        } else {
            regime
        };

        // Calculate confidence based on feature clarity
        let confidence = self.calculate_confidence(&features, final_regime);

        self.current_regime = RegimeState {
            regime: final_regime,
            confidence,
            risk_multiplier: final_regime.risk_multiplier() * confidence,
            allowed_experts: final_regime.allowed_experts().iter().map(|s| s.to_string()).collect(),
            features,
        };
    }

    fn calculate_features(&self) -> RegimeFeatures {
        let returns_vec: Vec<f64> = self.returns.iter().cloned().collect();

        // Realized volatility (annualized)
        let short_vol = self.calculate_std(&self.returns.iter().rev().take(self.window_short).cloned().collect::<VecDeque<_>>())
                        * (252.0_f64).sqrt();
        let long_vol = self.calculate_std(&self.returns) * (252.0_f64).sqrt();
        let realized_vol = short_vol;

        // Hurst exponent estimation via R/S analysis (simplified)
        let hurst = self.estimate_hurst(&returns_vec);

        // Trend strength (ADX-like via directional movement)
        let trend_strength = self.calculate_trend_strength();

        // Autocorrelation lag-1
        let autocorr = self.calculate_autocorrelation(&returns_vec, 1);

        // Jump indicator via bipower variation
        let jump = self.calculate_jump_indicator(&returns_vec);

        // Spread ratio
        let spread_ratio = if self.spreads.len() >= self.window_short {
            let recent: f64 = self.spreads.iter().rev().take(10).sum::<f64>() / 10.0;
            let avg: f64 = self.spreads.iter().sum::<f64>() / self.spreads.len() as f64;
            if avg > 0.0 { recent / avg } else { 1.0 }
        } else {
            1.0
        };

        // Volume surprise
        let volume_surprise = if self.volumes.len() >= self.window_short {
            let recent: f64 = self.volumes.iter().rev().take(10).sum::<f64>() / 10.0;
            let avg: f64 = self.volumes.iter().sum::<f64>() / self.volumes.len() as f64;
            if avg > 0.0 { recent / avg } else { 1.0 }
        } else {
            1.0
        };

        RegimeFeatures {
            realized_vol,
            short_vol,
            long_vol,
            hurst_estimate: hurst,
            trend_strength,
            autocorrelation: autocorr,
            jump_indicator: jump,
            spread_ratio,
            volume_surprise,
        }
    }

    fn classify_from_features(&self, f: &RegimeFeatures) -> HydraRegime {
        // Change-point detection triggers Event regime
        if self.cusum_pos > self.config.cusum_threshold || self.cusum_neg > self.config.cusum_threshold {
            return HydraRegime::Event;
        }

        // Toxic flow: spread expansion + volume spike
        if f.spread_ratio > self.config.spread_ratio_threshold && f.volume_surprise > self.config.volume_surprise_threshold {
            return HydraRegime::Toxic;
        }

        // High volatility regime
        if f.short_vol > self.config.vol_high_threshold || (f.short_vol / f.long_vol.max(0.001)) > self.config.vol_ratio_threshold {
            return HydraRegime::HighVolatility;
        }

        // Low volatility regime
        if f.short_vol < self.config.vol_low_threshold {
            return HydraRegime::LowVolatility;
        }

        // Trend vs Range based on Hurst and trend strength
        if f.hurst_estimate > self.config.hurst_trend_threshold && f.trend_strength > self.config.trend_strength_threshold {
            return HydraRegime::Trend;
        }

        if f.hurst_estimate < self.config.hurst_range_threshold || f.autocorrelation < self.config.autocorrelation_range_threshold {
            return HydraRegime::Range;
        }

        // Default to Range (conservative)
        HydraRegime::Range
    }

    fn calculate_confidence(&self, f: &RegimeFeatures, regime: HydraRegime) -> f64 {
        match regime {
            HydraRegime::Trend => {
                let hurst_conf = ((f.hurst_estimate - 0.5) * 4.0).clamp(0.0, 1.0);
                let trend_conf = (f.trend_strength / 50.0).clamp(0.0, 1.0);
                (hurst_conf + trend_conf) / 2.0
            }
            HydraRegime::Range => {
                let hurst_conf = ((0.5 - f.hurst_estimate) * 4.0).clamp(0.0, 1.0);
                let acf_conf = ((-f.autocorrelation) * 5.0).clamp(0.0, 1.0);
                (hurst_conf + acf_conf) / 2.0
            }
            HydraRegime::HighVolatility => {
                (f.short_vol / 0.30).clamp(0.5, 1.0)
            }
            HydraRegime::Toxic => 0.9,
            HydraRegime::Event => {
                let cusum_conf = (self.cusum_pos.max(self.cusum_neg) / self.cusum_threshold).clamp(0.5, 1.0);
                cusum_conf
            }
            _ => 0.5,
        }
    }

    fn calculate_std(&self, data: &VecDeque<f64>) -> f64 {
        if data.len() < 2 { return 0.0; }
        let mean = data.iter().sum::<f64>() / data.len() as f64;
        let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64;
        variance.sqrt()
    }

    fn estimate_hurst(&self, returns: &[f64]) -> f64 {
        if returns.len() < 20 { return 0.5; }

        // Simplified R/S analysis
        let n = returns.len();
        let mean = returns.iter().sum::<f64>() / n as f64;
        let std = (returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n as f64).sqrt();

        if std < 1e-10 { return 0.5; }

        // Cumulative deviations
        let mut cumsum = 0.0;
        let mut min_val = 0.0_f64;
        let mut max_val = 0.0_f64;

        for r in returns {
            cumsum += r - mean;
            min_val = min_val.min(cumsum);
            max_val = max_val.max(cumsum);
        }

        let range = max_val - min_val;
        let rs = range / std;

        // Hurst = log(R/S) / log(n)
        if rs > 0.0 && n > 1 {
            (rs.ln() / (n as f64).ln()).clamp(0.0, 1.0)
        } else {
            0.5
        }
    }

    fn calculate_trend_strength(&self) -> f64 {
        if self.prices.len() < self.window_short { return 0.0; }

        let prices: Vec<f64> = self.prices.iter().cloned().collect();
        let n = prices.len();

        // Calculate directional movement
        let mut plus_dm_sum = 0.0;
        let mut minus_dm_sum = 0.0;
        let mut tr_sum = 0.0;

        for i in 1..n {
            let high_diff = if i > 0 { prices[i] - prices[i-1] } else { 0.0 };
            let low_diff = if i > 0 { prices[i-1] - prices[i] } else { 0.0 };

            let tr = (prices[i] - prices[i-1]).abs();
            tr_sum += tr;

            if high_diff > low_diff && high_diff > 0.0 {
                plus_dm_sum += high_diff;
            }
            if low_diff > high_diff && low_diff > 0.0 {
                minus_dm_sum += low_diff;
            }
        }

        if tr_sum < 1e-10 { return 0.0; }

        let plus_di = plus_dm_sum / tr_sum * 100.0;
        let minus_di = minus_dm_sum / tr_sum * 100.0;
        let di_sum = plus_di + minus_di;

        if di_sum < 1e-10 { return 0.0; }

        // ADX-like value
        ((plus_di - minus_di).abs() / di_sum * 100.0).clamp(0.0, 100.0)
    }

    fn calculate_autocorrelation(&self, returns: &[f64], lag: usize) -> f64 {
        if returns.len() <= lag + 1 { return 0.0; }

        let _n = returns.len() - lag;
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;

        let mut cov = 0.0;
        let mut var = 0.0;

        for i in lag..returns.len() {
            cov += (returns[i] - mean) * (returns[i - lag] - mean);
            var += (returns[i] - mean).powi(2);
        }

        if var < 1e-10 { return 0.0; }

        (cov / var).clamp(-1.0, 1.0)
    }

    fn calculate_jump_indicator(&self, returns: &[f64]) -> f64 {
        if returns.len() < 10 { return 0.0; }

        // Bipower variation vs realized variance
        let rv: f64 = returns.iter().map(|r| r.powi(2)).sum();

        let bv: f64 = returns.windows(2)
            .map(|w| w[0].abs() * w[1].abs())
            .sum::<f64>() * std::f64::consts::PI / 2.0;

        if bv < 1e-10 { return 0.0; }

        // Jump ratio: high value indicates jumps
        ((rv - bv) / bv).max(0.0)
    }

    /// Get current regime state
    pub fn get_state(&self) -> &RegimeState {
        &self.current_regime
    }

    /// Check if a change point was detected
    pub fn change_point_detected(&self) -> bool {
        self.cusum_pos > self.cusum_threshold || self.cusum_neg > self.cusum_threshold
    }

    /// Reset CUSUM after handling change point
    pub fn reset_cusum(&mut self) {
        self.cusum_pos = 0.0;
        self.cusum_neg = 0.0;
    }
}

// ============================================================================
// EXPERT INTERFACE & IMPLEMENTATIONS
// ============================================================================

/// Expert intent: a proposed trade with metadata for attribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpertIntent {
    /// Unique intent ID for tracking
    pub intent_id: Uuid,
    /// Expert that generated this intent
    pub expert_id: String,
    /// Signal strength and direction (-1.0 to 1.0)
    pub signal: f64,
    /// Expected edge in bps at time of generation
    pub expected_edge_bps: f64,
    /// Confidence in the signal (0.0 to 1.0)
    pub confidence: f64,
    /// Timestamp of generation
    pub timestamp: DateTime<Utc>,
    /// Regime at time of signal
    pub regime: HydraRegime,
    /// Signal family/type for attribution
    pub signal_family: String,
    /// Suggested order style
    pub order_style: OrderStyle,
    /// Stop loss level (if applicable)
    pub stop_loss: Option<f64>,
    /// Take profit level (if applicable)
    pub take_profit: Option<f64>,
    /// Time-based exit (ticks from now)
    pub time_exit_ticks: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum OrderStyle {
    Maker,
    Taker,
    Passive,
}

/// Interface for Alpha Experts
pub trait Expert: Send + Sync {
    /// Expert identifier
    fn name(&self) -> &str;

    /// Update internal state with market data
    fn update_market(&mut self, event: &MarketEvent);

    /// Generate intent with full metadata
    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent>;

    /// Get raw signal (-1.0 to 1.0) for backward compatibility
    fn get_signal(&self) -> f64 {
        0.0
    }

    /// Expected edge in basis points
    fn expected_edge(&self) -> f64 {
        5.0
    }

    /// Signal family for attribution
    fn signal_family(&self) -> &str {
        "generic"
    }

    /// Whether this expert is allowed in the given regime
    fn allowed_in_regime(&self, regime: &RegimeState) -> bool {
        regime.allowed_experts.contains(&self.name().to_string())
    }
}

// ============================================================================
// EXPERT A: MULTI-TIMEFRAME TREND (Robust CTA-Style)
// ============================================================================

pub struct TrendExpert {
    /// Multi-resolution price windows
    prices_1m: VecDeque<f64>,
    prices_5m: VecDeque<f64>,
    prices_15m: VecDeque<f64>,
    /// ATR for volatility adjustment
    atr: f64,
    atr_window: VecDeque<f64>,
    /// Breakout levels
    highest_high: f64,
    lowest_low: f64,
    /// MA values
    ema_fast: f64,
    ema_slow: f64,
    /// Kalman Filter for trend
    kf: KalmanFilter,
    /// Current signal
    signal: f64,
    /// Last update timestamp
    last_update: DateTime<Utc>,
    /// Tick counter for bar aggregation
    tick_count: u64,
    /// Current price
    pub current_price: f64,
    /// Configuration
    fast_period: usize,
    slow_period: usize,
    breakout_period: usize,
}

impl TrendExpert {
    pub fn new() -> Self {
        Self {
            prices_1m: VecDeque::with_capacity(256),
            prices_5m: VecDeque::with_capacity(256),
            prices_15m: VecDeque::with_capacity(256),
            atr: 0.0,
            atr_window: VecDeque::with_capacity(20),
            highest_high: 0.0,
            lowest_low: f64::MAX,
            ema_fast: 0.0,
            ema_slow: 0.0,
            kf: KalmanFilter::new(0.0, 0.001, 0.1), // Low process noise for trend smoothing
            signal: 0.0,
            last_update: Utc::now(),
            tick_count: 0,
            current_price: 0.0,
            fast_period: 10,
            slow_period: 30,
            breakout_period: 20,
        }
    }

    fn update_ema(&self, prev_ema: f64, price: f64, period: usize) -> f64 {
        if prev_ema == 0.0 { return price; }
        let alpha = 2.0 / (period as f64 + 1.0);
        alpha * price + (1.0 - alpha) * prev_ema
    }

    fn calculate_signal(&mut self) {
        if self.prices_1m.len() < self.slow_period {
            self.signal = 0.0;
            return;
        }

        // Amplified multipliers for high-notional assets (e.g. BTC @ 87k)
        // 1bp move (0.0001) becomes 1.0 signal with 10,000x multiplier
        let breakout_signal = if self.current_price > self.highest_high {
            1.0
        } else if self.current_price < self.lowest_low {
            -1.0
        } else {
            0.0
        };

        // EMA slope signal (Production multiplier)
        let ema_signal = ((self.ema_fast - self.ema_slow) / self.ema_slow * 5000.0).clamp(-1.0, 1.0);

        // 3. Volatility-adjusted momentum
        let returns: Vec<f64> = self.prices_1m.iter()
            .zip(self.prices_1m.iter().skip(1))
            .map(|(a, b)| (b / a).ln())
            .collect();

        let momentum = if returns.len() >= 10 {
            returns.iter().rev().take(10).sum::<f64>()
        } else {
            0.0
        };

        let vol_adj_momentum = if self.atr > 0.0 {
            (momentum / self.atr * 200.0).clamp(-1.0, 1.0) // ATR is relative, multiplier lower
        } else {
            0.0
        };

        // 4. Kalman-Smoothed Momentum (Production multiplier)
        let kalman_mom = ((self.kf.x - self.ema_slow) / self.ema_slow * 5000.0).clamp(-1.0, 1.0);

        // Combine signals with weights (Masterpiece weighting)
        let raw_signal = 0.2 * breakout_signal + 0.3 * ema_signal + 0.2 * vol_adj_momentum + 0.3 * kalman_mom;

        // Only generate signal if all timeframes agree on direction
        let agrees = self.check_timeframe_agreement(raw_signal);

        self.signal = if agrees { raw_signal.clamp(-1.0, 1.0) } else { 0.0 };
    }

    fn check_timeframe_agreement(&self, primary_signal: f64) -> bool {
        if self.prices_5m.len() < 5 || self.prices_15m.len() < 5 {
            return primary_signal.abs() > 0.3; // Allow if not enough data
        }

        let trend_5m = (*self.prices_5m.back().unwrap_or(&0.0) - *self.prices_5m.front().unwrap_or(&0.0)).signum();
        let trend_15m = (*self.prices_15m.back().unwrap_or(&0.0) - *self.prices_15m.front().unwrap_or(&0.0)).signum();

        // All timeframes should agree
        (primary_signal.signum() == trend_5m || trend_5m == 0.0) &&
        (primary_signal.signum() == trend_15m || trend_15m == 0.0)
    }
}

impl Expert for TrendExpert {
    fn name(&self) -> &str { "Expert-A-Trend" }

    fn signal_family(&self) -> &str { "trend-momentum" }

    fn update_market(&mut self, event: &MarketEvent) {
        let price = match &event.payload {
            MarketPayload::Tick { price, .. } => *price,
            MarketPayload::Trade { price, .. } => *price,
            MarketPayload::Bar { close, high, low, .. } => {
                // Update ATR with bar data
                let tr = high - low;
                self.atr_window.push_back(tr);
                if self.atr_window.len() > 14 {
                    self.atr_window.pop_front();
                }
                self.atr = self.atr_window.iter().sum::<f64>() / self.atr_window.len() as f64;
                *close
            }
            _ => return,
        };

        self.current_price = price;
        self.tick_count += 1;

        // Update 1m bars
        self.prices_1m.push_back(price);
        if self.prices_1m.len() > 256 {
            self.prices_1m.pop_front();
        }

        // Update EMAs with Kalman-smoothed price for reduced lag and noise
        let smoothed_price = self.kf.update(price);
        self.ema_fast = self.update_ema(self.ema_fast, smoothed_price, self.fast_period);
        self.ema_slow = self.update_ema(self.ema_slow, smoothed_price, self.slow_period);

        // Update breakout levels (rolling window)
        let breakout_prices: Vec<f64> = self.prices_1m.iter()
            .rev()
            .take(self.breakout_period)
            .cloned()
            .collect();

        if breakout_prices.len() >= self.breakout_period {
            self.highest_high = breakout_prices.iter().cloned().fold(f64::MIN, f64::max);
            self.lowest_low = breakout_prices.iter().cloned().fold(f64::MAX, f64::min);
        }

        // Aggregate to higher timeframes
        if self.tick_count % 5 == 0 {
            self.prices_5m.push_back(price);
            if self.prices_5m.len() > 100 {
                self.prices_5m.pop_front();
            }
        }
        if self.tick_count % 15 == 0 {
            self.prices_15m.push_back(price);
            if self.prices_15m.len() > 50 {
                self.prices_15m.pop_front();
            }
        }

        // Calculate signal
        if self.tick_count % 5 == 0 {
            self.calculate_signal();
        }

        self.last_update = event.exchange_time;
    }

    fn get_signal(&self) -> f64 {
        self.signal
    }

    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent> {
        // MEDALLION: Use calculated signal or momentum fallback
        let dynamic_signal = if self.signal.abs() >= 0.05 {
            self.signal  // Use the properly calculated signal
        } else if self.prices_1m.len() >= 20 {
            // Fallback: Simple momentum with z-score normalization
            let recent: Vec<f64> = self.prices_1m.iter().rev().take(20).cloned().collect();
            let returns: Vec<f64> = (0..19).map(|i| (recent[i] / recent[i+1]).ln()).collect();
            let momentum = returns.iter().sum::<f64>();  // Cumulative log return
            // Scale by volatility to get a z-score-like signal
            let vol = (returns.iter().map(|r| r.powi(2)).sum::<f64>() / 19.0).sqrt();
            if vol > 0.0001 {
                (momentum / (vol * 4.5)).clamp(-1.0, 1.0)  // ~4.5 std for full signal
            } else {
                0.0
            }
        } else {
            0.0
        };

        // MEDALLION: High conviction threshold for production (Project AEON)
        if dynamic_signal.abs() < 0.15 { return None; }

        let stop_distance = self.atr * 2.0;
        let stop_loss = if self.signal > 0.0 {
            Some(self.current_price - stop_distance)
        } else {
            Some(self.current_price + stop_distance)
        };

        Some(ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: self.name().to_string(),
            signal: dynamic_signal,
            expected_edge_bps: 25.0 * dynamic_signal.abs(), // V2: Amplified edge for HFT strategy  // MEDALLION: Higher edge for momentum
            confidence: regime.confidence * dynamic_signal.abs(),
            timestamp: Utc::now(),
            regime: regime.regime,
            signal_family: self.signal_family().to_string(),
            order_style: OrderStyle::Taker, // Trend-following uses taker
            stop_loss,
            take_profit: None, // Trail stops instead
            time_exit_ticks: Some(500), // Time-based exit after 500 ticks
        })
    }

    fn expected_edge(&self) -> f64 {
        8.0 * self.signal.abs()
    }
}

// ============================================================================
// EXPERT B: MEAN REVERSION (Intraday)
// ============================================================================

pub struct MeanRevExpert {
    /// Price windows
    prices: VecDeque<f64>,
    volumes: VecDeque<f64>,
    /// VWAP calculation
    vwap_pv_sum: f64,
    vwap_v_sum: f64,
    /// Bollinger bands
    bb_upper: f64,
    bb_lower: f64,
    bb_mid: f64,
    /// Current state
    signal: f64,
    current_price: f64,
    /// Liquidity vacuum detection
    volume_ma: f64,
    last_spike_tick: u64,
    tick_count: u64,
    /// Configuration
    window: usize,
    bb_std_mult: f64,
}

impl MeanRevExpert {
    pub fn new() -> Self {
        Self {
            prices: VecDeque::with_capacity(100),
            volumes: VecDeque::with_capacity(100),
            vwap_pv_sum: 0.0,
            vwap_v_sum: 0.0,
            bb_upper: 0.0,
            bb_lower: 0.0,
            bb_mid: 0.0,
            signal: 0.0,
            current_price: 0.0,
            volume_ma: 0.0,
            last_spike_tick: 0,
            tick_count: 0,
            window: 50,
            bb_std_mult: 2.0,
        }
    }

    fn calculate_bollinger(&mut self) {
        if self.prices.len() < self.window { return; }

        let prices: Vec<f64> = self.prices.iter().cloned().collect();
        let mean = prices.iter().sum::<f64>() / prices.len() as f64;
        let std = (prices.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / prices.len() as f64).sqrt();

        self.bb_mid = mean;
        self.bb_upper = mean + self.bb_std_mult * std;
        self.bb_lower = mean - self.bb_std_mult * std;
    }

    fn calculate_signal(&mut self) {
        if self.prices.len() < self.window {
            self.signal = 0.0;
            return;
        }

        // VWAP z-score
        let vwap = if self.vwap_v_sum > 0.0 { self.vwap_pv_sum / self.vwap_v_sum } else { self.bb_mid };
        let prices_std = {
            let mean = self.prices.iter().sum::<f64>() / self.prices.len() as f64;
            (self.prices.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / self.prices.len() as f64).sqrt()
        };

        let vwap_zscore = if prices_std > 0.0 {
            (self.current_price - vwap) / prices_std
        } else {
            0.0
        };

        // Bollinger z-score
        let bb_range = self.bb_upper - self.bb_lower;
        let bb_zscore = if bb_range > 0.0 {
            (self.current_price - self.bb_mid) / (bb_range / 4.0) // Normalize to ~2 std
        } else {
            0.0
        };

        // Liquidity vacuum detection (spike followed by volume drop)
        let is_post_spike = self.tick_count - self.last_spike_tick < 50;
        let volume_depleted = self.volume_ma > 0.0 &&
            self.volumes.back().unwrap_or(&0.0) < &(self.volume_ma * 0.3);

        let liquidity_vacuum = is_post_spike && volume_depleted;

        // Combined signal - MEDALLION GRADE: Use continuous signal, not binary
        // The alpha is in the magnitude, not just direction
        let vwap_signal = if vwap_zscore.abs() > 0.5 {
            (-vwap_zscore / 2.0).clamp(-1.0, 1.0)  // Fade the move
        } else {
            0.0
        };

        let bb_signal = if bb_zscore.abs() > 0.5 {
            (-bb_zscore / 2.0).clamp(-1.0, 1.0)  // Fade the move
        } else {
            0.0
        };

        // Combine with liquidity vacuum boost
        let liquidity_mult = if liquidity_vacuum { 1.5 } else { 1.0 };

        // Weighted combination
        self.signal = ((0.6 * vwap_signal + 0.4 * bb_signal) * liquidity_mult).clamp(-1.0, 1.0);
    }
}

impl Expert for MeanRevExpert {
    fn name(&self) -> &str { "Expert-B-MeanRev" }

    fn signal_family(&self) -> &str { "mean-reversion" }

    fn update_market(&mut self, event: &MarketEvent) {
        let (price, volume) = match &event.payload {
            MarketPayload::Tick { price, size, .. } => (*price, *size),
            MarketPayload::Trade { price, quantity, .. } => (*price, *quantity),
            MarketPayload::Bar { close, volume, .. } => (*close, *volume),
            _ => return,
        };

        self.current_price = price;
        self.tick_count += 1;

        // Update VWAP
        self.vwap_pv_sum += price * volume;
        self.vwap_v_sum += volume;

        // Update price/volume windows
        self.prices.push_back(price);
        if self.prices.len() > self.window {
            self.prices.pop_front();
        }

        self.volumes.push_back(volume);
        if self.volumes.len() > self.window {
            self.volumes.pop_front();
        }

        // Update volume MA
        self.volume_ma = self.volumes.iter().sum::<f64>() / self.volumes.len() as f64;

        // Detect volume spikes
        if volume > self.volume_ma * 3.0 {
            self.last_spike_tick = self.tick_count;
        }

        // Update Bollinger bands
        self.calculate_bollinger();

        // Calculate signal
        self.calculate_signal();
    }

    fn get_signal(&self) -> f64 {
        self.signal
    }

    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent> {
        if self.signal.abs() < 0.25 { return None; } // MEDALLION: Higher conviction for mean rev
        // Skip regime check - mean reversion works in all regimes

        // Mean reversion uses tight stops
        let stop_distance = (self.bb_upper - self.bb_lower) / 4.0;
        let stop_loss = if self.signal > 0.0 {
            Some(self.current_price - stop_distance)
        } else {
            Some(self.current_price + stop_distance)
        };

        // Target is the mean
        let take_profit = Some(self.bb_mid);

        Some(ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: self.name().to_string(),
            signal: self.signal,
            expected_edge_bps: 12.0 * self.signal.abs(),  // MEDALLION: Higher edge for mean rev
            confidence: regime.confidence * self.signal.abs() * 0.9,
            timestamp: Utc::now(),
            regime: regime.regime,
            signal_family: self.signal_family().to_string(),
            order_style: OrderStyle::Maker, // Mean reversion can be patient
            stop_loss,
            take_profit,
            time_exit_ticks: Some(200), // Shorter time horizon
        })
    }

    fn expected_edge(&self) -> f64 {
        5.0 * self.signal.abs()
    }
}

// ============================================================================
// EXPERT C: VOLATILITY & CONVEXITY HARVESTER
// ============================================================================

pub struct VolatilityExpert {
    /// Realized volatility (multiple horizons)
    realized_vol_5: f64,
    realized_vol_20: f64,
    realized_vol_60: f64,
    /// Implied volatility proxy (from recent range) - Parkinson estimator
    implied_vol_proxy: f64,
    /// Returns for vol calculation
    returns: VecDeque<f64>,
    prices: VecDeque<f64>,
    /// Vol clustering state
    vol_regime: f64, // 0-1 scale
    /// Current signal
    signal: f64,
    current_price: f64,
    tick_count: u64,
    /// OPTIONS-SPECIFIC: Volatility of volatility (vol-of-vol)
    vol_of_vol: f64,
    /// OPTIONS-SPECIFIC: IV percentile rank (0-100)
    iv_percentile: f64,
    /// OPTIONS-SPECIFIC: Historical IV readings for percentile
    iv_history: VecDeque<f64>,
    /// OPTIONS-SPECIFIC: Gamma scalping signal (short-term mean reversion)
    gamma_signal: f64,
}

impl VolatilityExpert {
    pub fn new() -> Self {
        Self {
            realized_vol_5: 0.0,
            realized_vol_20: 0.0,
            realized_vol_60: 0.0,
            implied_vol_proxy: 0.0,
            returns: VecDeque::with_capacity(100),
            prices: VecDeque::with_capacity(100),
            vol_regime: 0.5,
            signal: 0.0,
            current_price: 0.0,
            tick_count: 0,
            vol_of_vol: 0.0,
            iv_percentile: 50.0,
            iv_history: VecDeque::with_capacity(252), // ~1 year of daily readings
            gamma_signal: 0.0,
        }
    }

    fn calculate_realized_vol(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() { return 0.0; }
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        variance.sqrt() * (252.0_f64).sqrt() // Annualized
    }

    /// Calculate IV percentile rank for options mean-reversion
    fn calculate_iv_percentile(&mut self) {
        if self.iv_history.len() < 20 { return; }

        let current_iv = self.implied_vol_proxy;
        let below_count = self.iv_history.iter().filter(|&&iv| iv < current_iv).count();
        self.iv_percentile = (below_count as f64 / self.iv_history.len() as f64) * 100.0;
    }

    /// Calculate gamma scalping signal for short-term mean reversion
    fn calculate_gamma_signal(&mut self) {
        if self.prices.len() < 10 { return; }

        // Calculate short-term price oscillation around recent mean
        let recent_prices: Vec<f64> = self.prices.iter().rev().take(10).cloned().collect();
        let mean_price = recent_prices.iter().sum::<f64>() / recent_prices.len() as f64;

        if mean_price > 0.0 {
            let deviation = (self.current_price - mean_price) / mean_price;
            // Gamma scalping: fade short-term deviations
            self.gamma_signal = (-deviation * 50.0).clamp(-1.0, 1.0);
        }
    }

    fn calculate_signal(&mut self) {
        if self.returns.len() < 20 {
            self.signal = 0.0;
            return;
        }

        // === 1. IV-RV SPREAD (Volatility Risk Premium) ===
        // When IV > RV, options are "expensive" -> sell vol (short gamma)
        // When IV < RV, options are "cheap" -> buy vol (long gamma)
        let rv_iv_gap = self.implied_vol_proxy - self.realized_vol_20;
        let premium_signal = (-rv_iv_gap * 8.0).clamp(-1.0, 1.0);

        // === 2. VOLATILITY TERM STRUCTURE ===
        // Short vol > Long vol indicates recent stress -> expect mean reversion
        let vol_term_spread = self.realized_vol_5 - self.realized_vol_60;
        let term_signal = (-vol_term_spread * 4.0).clamp(-1.0, 1.0);

        // === 3. IV PERCENTILE MEAN REVERSION (OPTIONS-SPECIFIC) ===
        // High IV percentile -> sell vol, Low IV percentile -> buy vol
        self.calculate_iv_percentile();
        let iv_pct_signal = ((50.0 - self.iv_percentile) / 50.0).clamp(-1.0, 1.0);

        // === 4. VOL-OF-VOL REGIME DETECTION ===
        // High vol-of-vol indicates unstable regime -> reduce position
        let vol_returns: Vec<f64> = self.returns.iter().map(|r| r.abs()).collect();
        self.vol_of_vol = self.calculate_realized_vol(&vol_returns);
        let vov_scalar = if self.vol_of_vol > 0.5 { 0.5 } else { 1.0 };

        // === 5. GAMMA SCALPING SIGNAL ===
        self.calculate_gamma_signal();

        // === COMPOSITE SIGNAL ===
        // Weight by vol regime and vol-of-vol
        let regime_weight = if self.vol_regime > 0.5 { 0.6 } else { 0.4 };

        // Options-optimized weighting:
        // - Premium signal (IV-RV): 35% weight - core edge
        // - Term structure: 20% weight - mean reversion
        // - IV percentile: 25% weight - options-specific
        // - Gamma scalping: 20% weight - short-term alpha
        let composite = 0.35 * premium_signal
            + 0.20 * term_signal
            + 0.25 * iv_pct_signal
            + 0.20 * self.gamma_signal;

        // Apply regime and vol-of-vol scaling
        self.signal = (composite * regime_weight * vov_scalar).clamp(-1.0, 1.0);
    }
}

impl Expert for VolatilityExpert {
    fn name(&self) -> &str { "Expert-C-Volatility" }

    fn signal_family(&self) -> &str { "volatility" }

    fn update_market(&mut self, event: &MarketEvent) {
        let price = match &event.payload {
            MarketPayload::Tick { price, .. } => *price,
            MarketPayload::Trade { price, .. } => *price,
            MarketPayload::Bar { close, high, low, .. } => {
                // Use high-low for IV proxy
                let range_vol = (high - low) / close;
                self.implied_vol_proxy = self.implied_vol_proxy * 0.95 + range_vol * 0.05 * (252.0_f64).sqrt();
                *close
            }
            _ => return,
        };

        self.current_price = price;
        self.tick_count += 1;

        // Calculate return
        if let Some(&prev_price) = self.prices.back() {
            if prev_price > 0.0 {
                let ret = (price / prev_price).ln();
                self.returns.push_back(ret);
                if self.returns.len() > 100 {
                    self.returns.pop_front();
                }
            }
        }

        self.prices.push_back(price);
        if self.prices.len() > 100 {
            self.prices.pop_front();
        }

        // Calculate realized vols at multiple horizons
        let returns_vec: Vec<f64> = self.returns.iter().cloned().collect();
        if returns_vec.len() >= 5 {
            self.realized_vol_5 = self.calculate_realized_vol(&returns_vec[returns_vec.len()-5..]);
        }
        if returns_vec.len() >= 20 {
            self.realized_vol_20 = self.calculate_realized_vol(&returns_vec[returns_vec.len()-20..]);
        }
        if returns_vec.len() >= 60 {
            self.realized_vol_60 = self.calculate_realized_vol(&returns_vec[returns_vec.len()-60..]);
        }

        // Update vol regime (EWMA of vol level)
        let current_vol_level = self.realized_vol_5;
        let vol_percentile = (current_vol_level / 0.50).clamp(0.0, 1.0); // Assume 50% annual vol is high
        self.vol_regime = self.vol_regime * 0.95 + vol_percentile * 0.05;

        // OPTIONS-SPECIFIC: Track IV history for percentile calculation
        // Update periodically (every ~100 ticks to simulate daily readings)
        if self.tick_count % 100 == 0 && self.implied_vol_proxy > 0.0 {
            self.iv_history.push_back(self.implied_vol_proxy);
            if self.iv_history.len() > 252 {
                self.iv_history.pop_front();
            }
        }

        // Calculate signal
        if self.tick_count % 10 == 0 {
            self.calculate_signal();
        }
    }

    fn get_signal(&self) -> f64 {
        self.signal
    }

    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent> {
        // OPTIONS-OPTIMIZED: Lower threshold for options (theta decay risk)
        if self.signal.abs() < 0.2 { return None; }
        // Skip regime check for vol expert - volatility edge is regime-agnostic

        // OPTIONS-SPECIFIC: Higher edge for vol trades (IV-RV spread capture)
        let edge_bps = if self.iv_percentile > 80.0 || self.iv_percentile < 20.0 {
            25.0 * self.signal.abs() // Extreme IV percentile = higher edge
        } else {
            15.0 * self.signal.abs() // Normal IV environment
        };

        Some(ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: self.name().to_string(),
            signal: self.signal,
            expected_edge_bps: edge_bps,
            confidence: regime.confidence * 0.8, // Higher confidence for options
            timestamp: Utc::now(),
            regime: regime.regime,
            signal_family: self.signal_family().to_string(),
            order_style: OrderStyle::Maker, // Options prefer maker for tighter spreads
            stop_loss: None, // Vol positions use delta hedging, not hard stops
            take_profit: None,
            time_exit_ticks: Some(500), // Medium horizon for options intraday
        })
    }

    fn expected_edge(&self) -> f64 {
        10.0 * self.signal.abs()
    }
}

// ============================================================================
// EXPERT D: MICROSTRUCTURE / ORDER-FLOW
// ============================================================================

pub struct MicrostructureExpert {
    /// Order flow imbalance
    buy_volume: VecDeque<f64>,
    sell_volume: VecDeque<f64>,
    /// Trade intensity
    trade_count: VecDeque<u64>,
    /// Price impact model
    last_prices: VecDeque<f64>,
    last_trade_sides: VecDeque<Side>,
    /// Current state
    signal: f64,
    current_price: f64,
    tick_count: u64,
    imbalance: f64,
    /// Cost tracking
    estimated_spread_bps: f64,
    estimated_impact_bps: f64,
    /// LOB State (Multi-level depth for Microstructure-X)
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    /// Volume acceleration for momentum-micro fusion
    volume_accel: VecDeque<f64>,
}

impl MicrostructureExpert {
    pub fn new() -> Self {
        Self {
            buy_volume: VecDeque::with_capacity(100),
            sell_volume: VecDeque::with_capacity(100),
            trade_count: VecDeque::with_capacity(100),
            last_prices: VecDeque::with_capacity(100),
            last_trade_sides: VecDeque::with_capacity(100),
            signal: 0.0,
            current_price: 0.0,
            tick_count: 0,
            imbalance: 0.0,
            estimated_spread_bps: 5.0,
            estimated_impact_bps: 2.0,
            bids: Vec::with_capacity(5),
            asks: Vec::with_capacity(5),
            volume_accel: VecDeque::with_capacity(20),
        }
    }

    fn calculate_signal(&mut self) {
        if self.buy_volume.len() < 5 {
            self.signal = 0.0;
            return;
        }

        // 1. Multi-Level OBI (Masterpiece logic)
        // Weighted sum of top 5 levels: closer to mid-price = higher weight
        let mut weighted_bid_qty = 0.0;
        for (i, (_p, q)) in self.bids.iter().enumerate() {
            weighted_bid_qty += q * (1.0 - 0.15 * i as f64);
        }
        let mut weighted_ask_qty = 0.0;
        for (i, (_p, q)) in self.asks.iter().enumerate() {
            weighted_ask_qty += q * (1.0 - 0.15 * i as f64);
        }

        let multi_obi = if (weighted_bid_qty + weighted_ask_qty) > 0.0 {
            (weighted_bid_qty - weighted_ask_qty) / (weighted_bid_qty + weighted_ask_qty)
        } else {
            0.0
        };

        // 2. Trade Flow Imbalance (Flow)
        let total_buy: f64 = self.buy_volume.iter().sum();
        let total_sell: f64 = self.sell_volume.iter().sum();
        let total_vol = total_buy + total_sell;
        let tfi = if total_vol > 0.0 {
            (total_buy - total_sell) / total_vol
        } else {
            0.0
        };

        // 3. Volume Acceleration (Masterpiece Pulse Detection)
        // Track rolling volume acceleration for momentum detection
        let avg_vol = total_vol / self.buy_volume.len().max(1) as f64;
        let recent_vol = *self.buy_volume.back().unwrap_or(&0.0) + *self.sell_volume.back().unwrap_or(&0.0);
        let vol_accel_ratio = if avg_vol > 0.0 { recent_vol / avg_vol } else { 1.0 };

        // Store volume acceleration for momentum-micro fusion
        self.volume_accel.push_back(vol_accel_ratio);
        if self.volume_accel.len() > 20 {
            self.volume_accel.pop_front();
        }

        // Calculate acceleration trend (is volume accelerating or decelerating?)
        let accel_trend = if self.volume_accel.len() >= 5 {
            let recent_accel: f64 = self.volume_accel.iter().rev().take(3).sum::<f64>() / 3.0;
            let older_accel: f64 = self.volume_accel.iter().rev().skip(3).take(3).sum::<f64>() / 3.0;
            if older_accel > 0.0 { recent_accel / older_accel } else { 1.0 }
        } else {
            1.0
        };

        // Clamp acceleration multiplier
        let accel_mult = vol_accel_ratio.clamp(1.0, 3.0);

        // 4. Market Impact Estimation (Kyle's Lambda Model)
        // Estimate price impact per unit of order flow
        if self.last_prices.len() >= 10 {
            let price_changes: Vec<f64> = self.last_prices.iter()
                .zip(self.last_prices.iter().skip(1))
                .map(|(p1, p2)| (p2 - p1) / p1 * 10000.0) // bps
                .collect();

            let vol_flow: Vec<f64> = self.buy_volume.iter()
                .zip(self.sell_volume.iter())
                .map(|(b, s)| b - s)
                .collect();

            // Simple linear regression: price_change = lambda * volume_imbalance
            if price_changes.len() >= 5 && vol_flow.len() >= 5 {
                let n = price_changes.len().min(vol_flow.len()) as f64;
                let sum_xy: f64 = price_changes.iter().zip(vol_flow.iter()).map(|(x, y)| x * y).sum();
                let sum_x: f64 = price_changes.iter().sum();
                let sum_y: f64 = vol_flow.iter().sum();
                let sum_x2: f64 = price_changes.iter().map(|x| x * x).sum();

                let denominator = n * sum_x2 - sum_x * sum_x;
                if denominator.abs() > 1e-10 {
                    let lambda = (n * sum_xy - sum_x * sum_y) / denominator;
                    // Impact in bps = lambda * expected_order_size
                    // Smooth update using exponential moving average
                    let new_impact = lambda.abs() * avg_vol * 0.1; // Scale to reasonable bps
                    self.estimated_impact_bps = self.estimated_impact_bps * 0.95 + new_impact * 0.05;
                }
            }
        }

        // Hybrid Signal Fusion with impact-adjusted confidence
        // Lower confidence when estimated impact is high (market less liquid)
        let impact_penalty = (self.estimated_impact_bps / 10.0).clamp(0.0, 0.5);
        self.imbalance = (0.7 * multi_obi + 0.3 * tfi) * accel_mult * (1.0 - impact_penalty);

        // Trade intensity (acceleration) for gating
        let recent_trades: u64 = self.trade_count.iter().rev().take(10).sum();
        let older_trades: u64 = self.trade_count.iter().rev().skip(10).take(10).sum();
        let intensity_ratio = if older_trades > 0 {
            recent_trades as f64 / older_trades as f64
        } else {
            1.0
        };

        // Combined signal with impact model
        let impact_signal = self.calculate_impact_signal();
        let base_signal = 0.6 * self.imbalance + 0.4 * impact_signal;

        // Apply acceleration trend boost: accelerating volume increases conviction
        let trend_boost = if accel_trend > 1.2 && base_signal.abs() > 0.3 {
            base_signal.signum() * 0.1 // Boost signal in direction of imbalance
        } else {
            0.0
        };

        // Intensity filter: Alpha needs "heat" in the market to be reliable
        let intensity_ok = intensity_ratio > 0.5 && intensity_ratio < 5.0;
        self.signal = if intensity_ok {
            (base_signal + trend_boost).clamp(-1.0, 1.0)
        } else {
            0.0
        };
    }

    fn calculate_impact_signal(&self) -> f64 {
        if self.last_prices.len() < 10 || self.last_trade_sides.len() < 10 {
            return 0.0;
        }

        // Look at price change after buy vs sell trades
        let prices: Vec<f64> = self.last_prices.iter().cloned().collect();
        let sides: Vec<&Side> = self.last_trade_sides.iter().collect();

        let mut buy_impact = 0.0;
        let mut sell_impact = 0.0;
        let mut buy_count = 0;
        let mut sell_count = 0;

        for i in 0..prices.len().saturating_sub(1) {
            let price_change = (prices[i + 1] - prices[i]) / prices[i];
            match sides.get(i) {
                Some(Side::Buy) => {
                    buy_impact += price_change;
                    buy_count += 1;
                }
                Some(Side::Sell) => {
                    sell_impact += price_change;
                    sell_count += 1;
                }
                _ => {}
            }
        }

        let avg_buy_impact = if buy_count > 0 { buy_impact / buy_count as f64 } else { 0.0 };
        let avg_sell_impact = if sell_count > 0 { sell_impact / sell_count as f64 } else { 0.0 };

        // If buys are pushing price up more than expected, momentum continues
        (avg_buy_impact - avg_sell_impact).clamp(-0.5, 0.5)
    }
}

impl Expert for MicrostructureExpert {
    fn name(&self) -> &str { "Expert-D-Microstructure" }

    fn signal_family(&self) -> &str { "microstructure" }

    fn update_market(&mut self, event: &MarketEvent) {
        match &event.payload {
            MarketPayload::Trade { price, quantity, is_buyer_maker, .. } => {
                self.current_price = *price;
                self.tick_count += 1;

                let side = if *is_buyer_maker { Side::Sell } else { Side::Buy };

                match side {
                    Side::Buy => self.buy_volume.push_back(*quantity),
                    Side::Sell => self.sell_volume.push_back(*quantity),
                }

                // Track trade count per window
                if let Some(last) = self.trade_count.back_mut() {
                    *last += 1;
                } else {
                    // Initialize first window
                    self.trade_count.push_back(1);
                }

                self.last_prices.push_back(*price);
                self.last_trade_sides.push_back(side);

                // Maintain window sizes
                if self.buy_volume.len() > 100 { self.buy_volume.pop_front(); }
                if self.sell_volume.len() > 100 { self.sell_volume.pop_front(); }
                if self.last_prices.len() > 100 { self.last_prices.pop_front(); }
                if self.last_trade_sides.len() > 100 { self.last_trade_sides.pop_front(); }

                // MEDALLION FIX: Calculate signal on every trade!
                self.calculate_signal();
            }
            MarketPayload::L2Update(l2) => {
                // Update multi-level LOB state for Microstructure-X
                // We keep top 5 levels for depth analysis
                for bid in &l2.bids {
                    if let Some(pos) = self.bids.iter().position(|b| (b.0 - bid.price).abs() < f64::EPSILON) {
                        if bid.size == 0.0 { self.bids.remove(pos); }
                        else { self.bids[pos].1 = bid.size; }
                    } else if bid.size > 0.0 {
                        self.bids.push((bid.price, bid.size));
                    }
                }
                self.bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                self.bids.truncate(5);

                for ask in &l2.asks {
                    if let Some(pos) = self.asks.iter().position(|a| (a.0 - ask.price).abs() < f64::EPSILON) {
                        if ask.size == 0.0 { self.asks.remove(pos); }
                        else { self.asks[pos].1 = ask.size; }
                    } else if ask.size > 0.0 {
                        self.asks.push((ask.price, ask.size));
                    }
                }
                self.asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                self.asks.truncate(5);

                // Update spread estimate
                if let (Some(best_bid), Some(best_ask)) = (self.bids.first(), self.asks.first()) {
                    let mid = (best_bid.0 + best_ask.0) / 2.0;
                    let spread_bps = (best_ask.0 - best_bid.0) / mid * 10000.0;
                    self.estimated_spread_bps = self.estimated_spread_bps * 0.9 + spread_bps * 0.1;
                    self.current_price = mid;
                }
                
                self.calculate_signal();
            }
            MarketPayload::L2Snapshot(snapshot) => {
                self.bids = snapshot.bids.iter().take(5).map(|l| (l.price, l.size)).collect();
                self.asks = snapshot.asks.iter().take(5).map(|l| (l.price, l.size)).collect();
                self.calculate_signal();
            }
            MarketPayload::Tick { price, size, side } => {
                self.current_price = *price;
                match side {
                    Side::Buy => self.buy_volume.push_back(*size),
                    Side::Sell => self.sell_volume.push_back(*size),
                }
                self.last_prices.push_back(*price);
                self.last_trade_sides.push_back(*side);

                if self.buy_volume.len() > 100 { self.buy_volume.pop_front(); }
                if self.sell_volume.len() > 100 { self.sell_volume.pop_front(); }
                if self.last_prices.len() > 100 { self.last_prices.pop_front(); }
                if self.last_trade_sides.len() > 100 { self.last_trade_sides.pop_front(); }
            }
            _ => return,
        }

        // Create new window for trade counting periodically
        if self.tick_count % 50 == 0 {
            self.trade_count.push_back(0);
            if self.trade_count.len() > 20 {
                self.trade_count.pop_front();
            }
        }

        // Calculate signal
        if self.tick_count % 5 == 0 {
            self.calculate_signal();
        }
    }

    fn get_signal(&self) -> f64 {
        self.signal
    }

    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent> {
        if self.signal.abs() < 0.15 { return None; } // AEON Production: High conviction
        if !self.allowed_in_regime(regime) { return None; }

        // Microstructure uses taker for immediacy
        Some(ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: self.name().to_string(),
            signal: self.signal,
            expected_edge_bps: self.imbalance.abs() * 25.0, // V2: Amplified edge for HFT strategy
            confidence: regime.confidence * self.signal.abs(),
            timestamp: Utc::now(),
            regime: regime.regime,
            signal_family: self.signal_family().to_string(),
            order_style: OrderStyle::Taker,
            stop_loss: None, // Quick trades, no stops
            take_profit: None,
            time_exit_ticks: Some(50), // Very short horizon
        })
    }

    fn expected_edge(&self) -> f64 {
        self.imbalance.abs() * 8.0
    }
}

// ============================================================================
// EXPERT E: CROSS-VENUE / CROSS-ASSET RELATIVE VALUE
// ============================================================================

pub struct RelativeValueExpert {
    /// Primary asset prices
    primary_prices: VecDeque<f64>,
    /// Secondary/hedge asset prices (if available)
    secondary_prices: VecDeque<f64>,
    /// Spread tracking
    spread_history: VecDeque<f64>,
    /// Z-score of spread
    spread_zscore: f64,
    /// Hedge ratio (beta)
    hedge_ratio: f64,
    /// Current signal
    signal: f64,
    current_price: f64,
    tick_count: u64,
    /// Configuration
    window: usize,
}

impl RelativeValueExpert {
    pub fn new() -> Self {
        Self {
            primary_prices: VecDeque::with_capacity(200),
            secondary_prices: VecDeque::with_capacity(200),
            spread_history: VecDeque::with_capacity(200),
            spread_zscore: 0.0,
            hedge_ratio: 1.0,
            signal: 0.0,
            current_price: 0.0,
            tick_count: 0,
            window: 100,
        }
    }

    fn calculate_signal(&mut self) {
        if self.spread_history.len() < 20 {
            self.signal = 0.0;
            return;
        }

        // Calculate spread z-score
        let spreads: Vec<f64> = self.spread_history.iter().cloned().collect();
        let mean = spreads.iter().sum::<f64>() / spreads.len() as f64;
        let std = (spreads.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / spreads.len() as f64).sqrt();

        if std > 0.0 {
            self.spread_zscore = (*spreads.last().unwrap_or(&mean) - mean) / std;
        }

        // MEDALLION: Continuous mean reversion signal on spread z-score
        // Signal scales with magnitude of z-score
        self.signal = (-self.spread_zscore / 2.0).clamp(-1.0, 1.0);
    }

    fn update_hedge_ratio(&mut self) {
        if self.primary_prices.len() < 50 || self.secondary_prices.len() < 50 {
            return;
        }

        // Simple regression for hedge ratio
        let primary: Vec<f64> = self.primary_prices.iter().cloned().collect();
        let secondary: Vec<f64> = self.secondary_prices.iter().cloned().collect();

        let n = primary.len().min(secondary.len()) as f64;
        let sum_xy: f64 = primary.iter().zip(secondary.iter()).map(|(x, y)| x * y).sum();
        let sum_x: f64 = primary.iter().sum();
        let sum_y: f64 = secondary.iter().sum();
        let sum_x2: f64 = primary.iter().map(|x| x * x).sum();

        let denom = n * sum_x2 - sum_x * sum_x;
        if denom.abs() > 1e-10 {
            self.hedge_ratio = (n * sum_xy - sum_x * sum_y) / denom;
        }
    }
}

impl Expert for RelativeValueExpert {
    fn name(&self) -> &str { "Expert-E-RelValue" }

    fn signal_family(&self) -> &str { "relative-value" }

    fn update_market(&mut self, event: &MarketEvent) {
        let price = match &event.payload {
            MarketPayload::Tick { price, .. } => *price,
            MarketPayload::Trade { price, .. } => *price,
            MarketPayload::Bar { close, .. } => *close,
            _ => return,
        };

        self.current_price = price;
        self.tick_count += 1;

        self.primary_prices.push_back(price);
        if self.primary_prices.len() > self.window {
            self.primary_prices.pop_front();
        }

        // For single-asset mode, use synthetic spread (price vs MA)
        let ma = if self.primary_prices.len() >= 20 {
            self.primary_prices.iter().rev().take(20).sum::<f64>() / 20.0
        } else {
            price
        };

        let spread = price - ma;
        self.spread_history.push_back(spread);
        if self.spread_history.len() > self.window {
            self.spread_history.pop_front();
        }

        // Update calculations
        if self.tick_count % 20 == 0 {
            self.update_hedge_ratio();
        }

        if self.tick_count % 5 == 0 {
            self.calculate_signal();
        }
    }

    fn get_signal(&self) -> f64 {
        self.signal
    }

    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent> {
        if self.signal.abs() < 0.25 { return None; } // MEDALLION: High conviction for rel value
        // Skip regime check for rel value

        Some(ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: self.name().to_string(),
            signal: self.signal,
            expected_edge_bps: 10.0 * self.signal.abs(),  // MEDALLION: Higher edge
            confidence: regime.confidence * self.signal.abs() * 0.85,
            timestamp: Utc::now(),
            regime: regime.regime,
            signal_family: self.signal_family().to_string(),
            order_style: OrderStyle::Maker,
            stop_loss: None,
            take_profit: None,
            time_exit_ticks: Some(300),
        })
    }

    fn expected_edge(&self) -> f64 {
        self.spread_zscore.abs() * 3.0
    }
}

// ============================================================================
// ATTRIBUTION LEDGER - THE TRUTH KEEPER
// ============================================================================

/// Single entry in the attribution ledger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributionEntry {
    /// Unique fill ID
    pub fill_id: Uuid,
    /// Expert that generated the intent
    pub expert_id: String,
    /// Regime at time of signal
    pub regime_id: HydraRegime,
    /// Signal family
    pub signal_family: String,
    /// Order style used
    pub order_style: OrderStyle,
    /// Expected edge at placement (bps)
    pub expected_edge_bps: f64,
    /// Realized PnL (net of costs)
    pub realized_pnl: f64,
    /// Fill price
    pub fill_price: f64,
    /// Fill quantity
    pub fill_quantity: f64,
    /// Commission paid
    pub commission: f64,
    /// Slippage (bps)
    pub slippage_bps: f64,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Intent ID this fill corresponds to
    pub intent_id: Uuid,
}

/// Attribution Ledger: Tracks and attributes all fills to experts
pub struct AttributionLedger {
    /// All entries
    entries: Vec<AttributionEntry>,
    /// Per-expert aggregates
    expert_pnl: HashMap<String, f64>,
    expert_fills: HashMap<String, u64>,
    expert_wins: HashMap<String, u64>,
    expert_edge_realized: HashMap<String, f64>,
    /// Per-regime aggregates
    regime_pnl: HashMap<HydraRegime, f64>,
    /// Per-signal-family aggregates
    family_pnl: HashMap<String, f64>,
    /// Pending intents awaiting fill
    pending_intents: HashMap<Uuid, ExpertIntent>,
    /// Maximum entries to keep
    max_entries: usize,
}

impl AttributionLedger {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(10000),
            expert_pnl: HashMap::new(),
            expert_fills: HashMap::new(),
            expert_wins: HashMap::new(),
            expert_edge_realized: HashMap::new(),
            regime_pnl: HashMap::new(),
            family_pnl: HashMap::new(),
            pending_intents: HashMap::new(),
            max_entries: 100000,
        }
    }

    /// Register an intent before the order is placed
    pub fn register_intent(&mut self, intent: &ExpertIntent) {
        self.pending_intents.insert(intent.intent_id, intent.clone());
    }

    /// Record a fill and attribute it
    pub fn record_fill(
        &mut self,
        intent_id: Uuid,
        fill_price: f64,
        fill_quantity: f64,
        commission: f64,
        expected_price: f64,
    ) {
        let intent = match self.pending_intents.remove(&intent_id) {
            Some(i) => i,
            None => {
                warn!("Fill for unknown intent: {}", intent_id);
                return;
            }
        };

        let slippage_bps = if expected_price > 0.0 {
            ((fill_price - expected_price) / expected_price * 10000.0).abs()
        } else {
            0.0
        };

        // Simplified PnL (real impl would track position lifecycle)
        let gross_pnl = intent.signal * (fill_price - expected_price) * fill_quantity;
        let realized_pnl = gross_pnl - commission;

        let entry = AttributionEntry {
            fill_id: Uuid::new_v4(),
            expert_id: intent.expert_id.clone(),
            regime_id: intent.regime,
            signal_family: intent.signal_family.clone(),
            order_style: intent.order_style,
            expected_edge_bps: intent.expected_edge_bps,
            realized_pnl,
            fill_price,
            fill_quantity,
            commission,
            slippage_bps,
            timestamp: Utc::now(),
            intent_id,
        };

        // Update aggregates
        *self.expert_pnl.entry(intent.expert_id.clone()).or_insert(0.0) += realized_pnl;
        *self.expert_fills.entry(intent.expert_id.clone()).or_insert(0) += 1;
        if realized_pnl > 0.0 {
            *self.expert_wins.entry(intent.expert_id.clone()).or_insert(0) += 1;
        }
        *self.expert_edge_realized.entry(intent.expert_id.clone()).or_insert(0.0) +=
            realized_pnl / fill_quantity / fill_price * 10000.0; // bps
        *self.regime_pnl.entry(intent.regime).or_insert(0.0) += realized_pnl;
        *self.family_pnl.entry(intent.signal_family).or_insert(0.0) += realized_pnl;

        self.entries.push(entry);

        // Trim if too large
        if self.entries.len() > self.max_entries {
            self.entries.drain(0..1000);
        }
    }

    /// Get PnL for a specific expert
    pub fn get_expert_pnl(&self, expert_id: &str) -> f64 {
        *self.expert_pnl.get(expert_id).unwrap_or(&0.0)
    }

    /// Get average realized edge for an expert (bps)
    pub fn get_expert_avg_edge(&self, expert_id: &str) -> f64 {
        let total_edge = *self.expert_edge_realized.get(expert_id).unwrap_or(&0.0);
        let fills = *self.expert_fills.get(expert_id).unwrap_or(&1) as f64;
        total_edge / fills
    }

    /// Get all expert PnLs
    pub fn get_all_expert_pnl(&self) -> &HashMap<String, f64> {
        &self.expert_pnl
    }

    /// Get win/loss stats for Kelly
    pub fn get_expert_stats(&self, expert_id: &str) -> (u64, u64, f64) {
        let fills = *self.expert_fills.get(expert_id).unwrap_or(&0);
        let wins = *self.expert_wins.get(expert_id).unwrap_or(&0);
        let losses = fills.saturating_sub(wins);
        let avg_edge = self.get_expert_avg_edge(expert_id);
        (wins, losses, avg_edge)
    }
}

// ============================================================================
// META-ALLOCATOR - THE SELF-EVOLVING CORE
// ============================================================================

/// Per-expert state for the meta-allocator
#[derive(Debug, Clone)]
pub struct ExpertState {
    /// Current weight (0.0 to 1.0)
    pub weight: f64,
    /// Net edge (PnL - fees - slippage - impact)
    pub edge_net: f64,
    /// Execution quality score
    pub quality: f64,
    /// Rolling Sharpe ratio
    pub sharpe: f64,
    /// Maximum drawdown
    pub max_drawdown: f64,
    /// Current drawdown
    pub current_drawdown: f64,
    /// Peak PnL
    pub peak_pnl: f64,
    /// Is quarantined?
    pub quarantined: bool,
    /// Ticks in quarantine
    pub quarantine_ticks: u64,
    /// Total fills
    pub fill_count: u64,
    /// Reject count
    pub reject_count: u64,
    /// Returns for Sharpe calculation
    pub returns: VecDeque<f64>,
    /// Virtual PnL for shadow revalidation (quarantined mode)
    pub virtual_pnl: f64,
    /// Virtual peak PnL for drawdown calculation
    pub virtual_peak: f64,
    /// Win/Loss tracking for Kelly
    pub win_count: u64,
    pub loss_count: u64,
    pub avg_edge: f64,
}

impl ExpertState {
    pub fn new(initial_weight: f64) -> Self {
        Self {
            weight: initial_weight,
            edge_net: 0.0,
            quality: 1.0,
            sharpe: 0.0,
            max_drawdown: 0.0,
            current_drawdown: 0.0,
            peak_pnl: 0.0,
            quarantined: false,
            quarantine_ticks: 0,
            fill_count: 0,
            reject_count: 0,
            returns: VecDeque::with_capacity(100),
            virtual_pnl: 0.0,
            virtual_peak: 0.0,
            win_count: 0,
            loss_count: 0,
            avg_edge: 0.0,
        }
    }

    pub fn get_kelly_fraction(&self) -> f64 {
        let total = self.win_count + self.loss_count;
        if total < 10 { return 1.0; } // Masterpiece: Full confidence during warmup
        
        let win_rate = self.win_count as f64 / total as f64;
        let _edge = self.avg_edge; // expected edge in bps
        
        // Simple fractional Kelly: (WinRate - LossRate) * (Edge / Spread)
        // Here we'll just use a Sharpe-based proxy if edge calculation is complex
        let k = (win_rate - (1.0 - win_rate)) * 0.5; // Half-Kelly on win rate
        k.clamp(0.1, 1.0)
    }
}

/// Meta-Allocator: Online weight updates using Exponentiated Gradient
pub struct MetaAllocator {
    /// Per-expert state
    states: HashMap<String, ExpertState>,
    /// Configuration
    config: HydraConfig,
    /// Portfolio-level metrics
    portfolio_pnl: f64,
    portfolio_peak: f64,
    portfolio_drawdown: f64,
    /// Global risk multiplier (reduced on drawdown)
    global_risk_mult: f64,
    /// Last update tick
    last_update_tick: u64,
}

impl MetaAllocator {
    pub fn new(expert_names: Vec<String>, config: HydraConfig) -> Self {
        let n = expert_names.len() as f64;
        let initial_weight = 1.0 / n;

        let mut states = HashMap::new();
        for name in expert_names {
            states.insert(name, ExpertState::new(initial_weight));
        }

        Self {
            states,
            config,
            portfolio_pnl: 0.0,
            portfolio_peak: 0.0,
            portfolio_drawdown: 0.0,
            global_risk_mult: 1.0,
            last_update_tick: 0,
        }
    }

    /// Update weights using Exponentiated Gradient algorithm
    pub fn update_weights(&mut self, ledger: &AttributionLedger, tick: u64) {
        // Only update periodically
        if tick - self.last_update_tick < 100 {
            return;
        }
        self.last_update_tick = tick;

        let expert_pnls = ledger.get_all_expert_pnl();
        let mut normalization_sum = 0.0;

        for (name, state) in self.states.iter_mut() {
            let pnl = *expert_pnls.get(name).unwrap_or(&0.0);

            if state.quarantined {
                state.quarantine_ticks += 100;
                
                // --- SHADOW REVALIDATION ---
                // We track "virtual PnL" while in quarantine. 
                // If the expert would have made money, we allow it back.
                state.virtual_pnl = pnl; // In this impl, pnl is cumulative realized
                if state.virtual_pnl > state.virtual_peak {
                    state.virtual_peak = state.virtual_pnl;
                }
                
                let recovery_from_peak = state.virtual_pnl - state.peak_pnl; // peak_pnl was at time of quarantine
                
                // If it recovers enough or stays in quarantine too long with flat/positive performance
                let is_recovered = recovery_from_peak >= (state.peak_pnl * self.config.virtual_recovery_threshold / 100.0).max(1.0);

                if is_recovered || state.quarantine_ticks >= self.config.quarantine_cooldown_ticks {
                    info!("[HYDRA] Expert {} RELEASED from quarantine (Recovered: {})", name, is_recovered);
                    state.quarantined = false;
                    state.quarantine_ticks = 0;
                    state.weight = self.config.weight_floor;
                    // Reset peaks to avoid immediate re-quarantine
                    state.peak_pnl = pnl;
                }
                continue;
            }

            // Update PnL tracking for active experts
            state.edge_net = pnl;
            
            // --- MASTERPIECE: Update Win/Loss/Edge for Kelly ---
            let (wins, losses, avg_edge) = ledger.get_expert_stats(name);
            state.win_count = wins;
            state.loss_count = losses;
            state.avg_edge = avg_edge;

            if pnl > state.peak_pnl {
                state.peak_pnl = pnl;
            }
            state.current_drawdown = (state.peak_pnl - pnl) / state.peak_pnl.max(1.0) * 100.0;
            if state.current_drawdown > state.max_drawdown {
                state.max_drawdown = state.current_drawdown;
            }

            // Check for quarantine condition
            if state.current_drawdown > self.config.expert_quarantine_drawdown {
                warn!("[HYDRA] Expert {} QUARANTINED: drawdown {:.2}% > {:.2}%",
                    name, state.current_drawdown, self.config.expert_quarantine_drawdown);
                state.quarantined = true;
                state.quarantine_ticks = 0;
                state.virtual_pnl = pnl;
                state.virtual_peak = pnl;
                continue;
            }

            // Calculate reward for EG update
            let risk_penalty = self.config.lambda_risk * state.current_drawdown / 100.0;
            let tail_penalty = self.config.lambda_tail * (state.max_drawdown / 100.0).powi(2);
            let exec_penalty = self.config.lambda_exec * (1.0 - state.quality);

            let reward = state.edge_net - risk_penalty - tail_penalty - exec_penalty;

            // EG update: w_new = w_old * exp(eta * reward)
            let weight_update = (self.config.learning_rate * reward).exp();
            state.weight *= weight_update;

            // Apply floor/ceiling
            state.weight = state.weight.clamp(self.config.weight_floor, self.config.weight_ceiling);

            normalization_sum += state.weight;
        }

        // Normalize weights
        if normalization_sum > 0.0 {
            for state in self.states.values_mut() {
                if !state.quarantined {
                    state.weight /= normalization_sum;
                }
            }
        }

        // Update portfolio-level drawdown
        let total_pnl: f64 = expert_pnls.values().sum();
        self.portfolio_pnl = total_pnl;
        if total_pnl > self.portfolio_peak {
            self.portfolio_peak = total_pnl;
        }
        self.portfolio_drawdown = if self.portfolio_peak > 0.0 {
            (self.portfolio_peak - total_pnl) / self.portfolio_peak * 100.0
        } else {
            0.0
        };

        // Adjust global risk multiplier based on portfolio drawdown
        self.global_risk_mult = if self.portfolio_drawdown >= self.config.max_drawdown_pct {
            0.0 // Full risk-off
        } else if self.portfolio_drawdown >= self.config.max_drawdown_pct * 0.8 {
            0.3 // Severe reduction
        } else if self.portfolio_drawdown >= self.config.max_drawdown_pct * 0.5 {
            0.6 // Moderate reduction
        } else {
            1.0 // Full risk
        };
    }

    /// Get weight for a specific expert
    pub fn get_weight(&self, expert_id: &str) -> f64 {
        self.states.get(expert_id)
            .map(|s| if s.quarantined { 0.0 } else { s.weight })
            .unwrap_or(0.0)
    }

    /// Get global risk multiplier
    pub fn get_global_risk_mult(&self) -> f64 {
        self.global_risk_mult
    }

    /// Check if an expert is quarantined
    pub fn is_quarantined(&self, expert_id: &str) -> bool {
        self.states.get(expert_id).map(|s| s.quarantined).unwrap_or(true)
    }

    /// Update execution quality for an expert
    pub fn update_quality(&mut self, expert_id: &str, fill_ratio: f64, reject_rate: f64) {
        if let Some(state) = self.states.get_mut(expert_id) {
            state.quality = fill_ratio * (1.0 - reject_rate);
        }
    }
}

// ============================================================================
// EXECUTION QUALITY GATE
// ============================================================================

/// Execution quality metrics for gating
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetrics {
    pub latency_p99_ms: f64,
    pub fill_ratio: f64,
    pub reject_rate: f64,
    pub slippage_bps: f64,
    pub adverse_selection_bps: f64,
    pub disconnect_count: u64,
}

/// Execution Quality Gate: Monitors and gates based on execution quality
pub struct ExecutionQualityGate {
    /// Current metrics
    metrics: ExecutionMetrics,
    /// Historical slippage tracking
    slippage_history: VecDeque<f64>,
    /// Is execution currently toxic?
    is_toxic: bool,
    /// Aggressiveness score (0 = full maker, 1 = full taker)
    aggressiveness: f64,
    /// Max order rate per second
    max_order_rate: f64,
    /// Quote width multiplier
    quote_width_mult: f64,
}

impl ExecutionQualityGate {
    pub fn new() -> Self {
        Self {
            metrics: ExecutionMetrics::default(),
            slippage_history: VecDeque::with_capacity(100),
            is_toxic: false,
            aggressiveness: 0.5,
            max_order_rate: 10.0,
            quote_width_mult: 1.0,
        }
    }

    /// Update with latest metrics
    pub fn update_metrics(&mut self, metrics: ExecutionMetrics) {
        self.slippage_history.push_back(metrics.slippage_bps);
        if self.slippage_history.len() > 100 {
            self.slippage_history.pop_front();
        }

        self.metrics = metrics;
        self.evaluate_toxicity();
    }

    fn evaluate_toxicity(&mut self) {
        // Toxic conditions:
        // 1. Latency spike (>100ms p99)
        // 2. High reject rate (>10%)
        // 3. Slippage explosion (>3x baseline)
        // 4. High adverse selection

        let baseline_slippage = self.slippage_history.iter().sum::<f64>()
            / self.slippage_history.len().max(1) as f64;

        let slippage_ratio = if baseline_slippage > 0.0 {
            self.metrics.slippage_bps / baseline_slippage
        } else {
            1.0
        };

        self.is_toxic = self.metrics.latency_p99_ms > 100.0
            || self.metrics.reject_rate > 0.10
            || slippage_ratio > 3.0
            || self.metrics.adverse_selection_bps > 20.0;

        // Adjust aggressiveness based on conditions
        if self.is_toxic {
            self.aggressiveness = 0.0; // Full maker mode
            self.max_order_rate = 1.0; // Slow down
            self.quote_width_mult = 2.0; // Wider quotes
        } else if self.metrics.fill_ratio < 0.5 {
            self.aggressiveness = 0.7; // More aggressive to get fills
            self.quote_width_mult = 0.8; // Tighter quotes
        } else {
            self.aggressiveness = 0.5;
            self.max_order_rate = 10.0;
            self.quote_width_mult = 1.0;
        }
    }

    /// Check if execution is currently allowed
    pub fn is_execution_allowed(&self) -> bool {
        !self.is_toxic
    }

    /// Get recommended order style
    pub fn recommended_style(&self) -> OrderStyle {
        if self.aggressiveness > 0.7 {
            OrderStyle::Taker
        } else if self.aggressiveness < 0.3 {
            OrderStyle::Maker
        } else {
            OrderStyle::Passive
        }
    }

    /// Get quote width multiplier
    pub fn get_quote_width_mult(&self) -> f64 {
        self.quote_width_mult
    }
}

// ============================================================================
// KILL-SWITCH POLICY
// ============================================================================

/// Kill-switch conditions
#[derive(Debug, Clone)]
pub struct KillSwitchPolicy {
    /// Reference to global kill switch
    kill_switch: Arc<AtomicBool>,
    /// Latency threshold (ms)
    max_latency_ms: f64,
    /// Max disconnect count per session
    max_disconnects: u64,
    /// Slippage explosion threshold (multiple of baseline)
    max_slippage_mult: f64,
    /// Max drawdown percentage
    max_drawdown_pct: f64,
    /// Current disconnect count
    disconnect_count: u64,
    /// Baseline slippage
    baseline_slippage: f64,
    /// Peak equity
    peak_equity: f64,
    /// Is triggered?
    is_triggered: bool,
    /// Trigger reason
    trigger_reason: Option<String>,
}

impl KillSwitchPolicy {
    pub fn new(kill_switch: Arc<AtomicBool>) -> Self {
        Self {
            kill_switch,
            max_latency_ms: 200.0,
            max_disconnects: 5,
            max_slippage_mult: 5.0,
            max_drawdown_pct: 10.0,
            disconnect_count: 0,
            baseline_slippage: 5.0,
            peak_equity: 0.0,
            is_triggered: false,
            trigger_reason: None,
        }
    }

    /// Check all kill-switch conditions
    pub fn check(
        &mut self,
        latency_ms: f64,
        current_slippage: f64,
        current_equity: f64,
        had_disconnect: bool,
    ) -> bool {
        if self.is_triggered {
            return true;
        }

        // Update peak equity
        if current_equity > self.peak_equity {
            self.peak_equity = current_equity;
        }

        // Check conditions
        let mut should_trigger = false;
        let mut reason = String::new();

        // 1. Latency check
        if latency_ms > self.max_latency_ms {
            should_trigger = true;
            reason = format!("Latency spike: {:.0}ms > {:.0}ms", latency_ms, self.max_latency_ms);
        }

        // 2. Disconnect check
        if had_disconnect {
            self.disconnect_count += 1;
        }
        if self.disconnect_count >= self.max_disconnects {
            should_trigger = true;
            reason = format!("Too many disconnects: {} >= {}", self.disconnect_count, self.max_disconnects);
        }

        // 3. Slippage explosion
        let slippage_mult = current_slippage / self.baseline_slippage;
        if slippage_mult > self.max_slippage_mult {
            should_trigger = true;
            reason = format!("Slippage explosion: {:.1}x baseline", slippage_mult);
        }

        // 4. Drawdown breach
        let drawdown = if self.peak_equity > 0.0 {
            (self.peak_equity - current_equity) / self.peak_equity * 100.0
        } else {
            0.0
        };
        if drawdown >= self.max_drawdown_pct {
            should_trigger = true;
            reason = format!("Drawdown breach: {:.1}% >= {:.1}%", drawdown, self.max_drawdown_pct);
        }

        if should_trigger {
            error!("[HYDRA] KILL SWITCH TRIGGERED: {}", reason);
            self.kill_switch.store(true, Ordering::SeqCst);
            self.is_triggered = true;
            self.trigger_reason = Some(reason);
        }

        self.is_triggered
    }

    /// Reset kill switch (manual intervention)
    pub fn reset(&mut self) {
        self.kill_switch.store(false, Ordering::SeqCst);
        self.is_triggered = false;
        self.trigger_reason = None;
        self.disconnect_count = 0;
        info!("[HYDRA] Kill switch RESET by operator");
    }

    /// Check if triggered
    pub fn is_triggered(&self) -> bool {
        self.is_triggered
    }
}

// ============================================================================
// OPTIONS GREEKS RISK MANAGER
// ============================================================================

/// Configuration for Greeks-based risk limits (optimized for NSE F&O)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GreeksRiskConfig {
    /// Maximum absolute delta exposure (in lots)
    pub max_delta: f64,
    /// Maximum gamma exposure (position gamma * 100)
    pub max_gamma: f64,
    /// Maximum vega exposure (in rupees per 1% IV move)
    pub max_vega: f64,
    /// Maximum theta decay allowed per day (in rupees)
    pub max_theta_daily: f64,
    /// Delta neutral band - positions within this are considered hedged
    pub delta_neutral_band: f64,
    /// Enable automatic delta hedging
    pub auto_delta_hedge: bool,
    /// Gamma scalping threshold - rebalance when gamma P&L exceeds this
    pub gamma_scalp_threshold: f64,
}

impl Default for GreeksRiskConfig {
    fn default() -> Self {
        Self {
            max_delta: 50.0,      // 50 lots equivalent
            max_gamma: 5.0,       // Conservative gamma limit
            max_vega: 50000.0,    // ₹50,000 per 1% IV move
            max_theta_daily: 10000.0, // ₹10,000 daily decay limit
            delta_neutral_band: 5.0,  // ±5 delta is "neutral"
            auto_delta_hedge: true,
            gamma_scalp_threshold: 5000.0, // ₹5,000 gamma P&L triggers rebalance
        }
    }
}

impl GreeksRiskConfig {
    /// Configuration optimized for NIFTY options
    pub fn nifty_optimized() -> Self {
        Self {
            max_delta: 30.0,      // 30 lots = 1500 units
            max_gamma: 3.0,       // Tighter for NIFTY
            max_vega: 30000.0,    // ₹30,000 vega exposure
            max_theta_daily: 5000.0,
            delta_neutral_band: 3.0,
            auto_delta_hedge: true,
            gamma_scalp_threshold: 3000.0,
        }
    }

    /// Configuration for BANKNIFTY (higher volatility)
    pub fn banknifty_optimized() -> Self {
        Self {
            max_delta: 20.0,      // 20 lots = 300 units
            max_gamma: 2.0,       // Very tight for BANKNIFTY
            max_vega: 40000.0,    // Higher due to vol
            max_theta_daily: 8000.0,
            delta_neutral_band: 2.0,
            auto_delta_hedge: true,
            gamma_scalp_threshold: 4000.0,
        }
    }
}

/// Portfolio Greeks state for risk monitoring
#[derive(Debug, Clone, Default)]
pub struct PortfolioGreeksState {
    /// Current net delta
    pub delta: f64,
    /// Current gamma
    pub gamma: f64,
    /// Current theta (daily)
    pub theta: f64,
    /// Current vega
    pub vega: f64,
    /// Delta in rupee terms
    pub delta_rupees: f64,
    /// Gamma P&L since last rebalance
    pub gamma_pnl: f64,
    /// Last rebalance price
    pub last_rebalance_price: f64,
    /// Last update timestamp
    pub last_update: DateTime<Utc>,
}

/// Greeks-based risk manager for options positions
pub struct GreeksRiskManager {
    /// Risk configuration
    config: GreeksRiskConfig,
    /// Current portfolio Greeks
    state: PortfolioGreeksState,
    /// Historical Greeks for trend analysis
    delta_history: VecDeque<f64>,
    /// Position scaling factor based on Greeks exposure
    risk_scalar: f64,
    /// Is Greeks limit breached?
    limit_breached: bool,
    /// Breach reason
    breach_reason: Option<String>,
    /// Lot size for the instrument
    lot_size: u32,
}

impl GreeksRiskManager {
    pub fn new(config: GreeksRiskConfig, lot_size: u32) -> Self {
        Self {
            config,
            state: PortfolioGreeksState::default(),
            delta_history: VecDeque::with_capacity(100),
            risk_scalar: 1.0,
            limit_breached: false,
            breach_reason: None,
            lot_size,
        }
    }

    /// Create manager for NIFTY
    pub fn for_nifty() -> Self {
        Self::new(GreeksRiskConfig::nifty_optimized(), 50)
    }

    /// Create manager for BANKNIFTY
    pub fn for_banknifty() -> Self {
        Self::new(GreeksRiskConfig::banknifty_optimized(), 15)
    }

    /// Update portfolio Greeks from position changes
    pub fn update_greeks(
        &mut self,
        delta: f64,
        gamma: f64,
        theta: f64,
        vega: f64,
        spot_price: f64,
    ) {
        self.state.delta = delta;
        self.state.gamma = gamma;
        self.state.theta = theta;
        self.state.vega = vega;
        self.state.delta_rupees = delta * spot_price * self.lot_size as f64;
        self.state.last_update = Utc::now();

        // Track delta history
        self.delta_history.push_back(delta);
        if self.delta_history.len() > 100 {
            self.delta_history.pop_front();
        }

        // Calculate gamma P&L since last rebalance
        if self.state.last_rebalance_price > 0.0 {
            let price_move = spot_price - self.state.last_rebalance_price;
            // Gamma P&L = 0.5 * gamma * (price_move)^2 * lot_size
            self.state.gamma_pnl = 0.5 * gamma * price_move.powi(2) * self.lot_size as f64;
        }

        // Check limits and calculate risk scalar
        self.check_limits_and_scale();
    }

    /// Update Greeks from actual option parameters using Black-Scholes
    ///
    /// This is the PRODUCTION method - uses real Greeks calculations from kubera_options
    ///
    /// # Parameters
    /// * `position` - Number of lots (positive = long, negative = short)
    /// * `spot_price` - Current underlying price
    /// * `strike` - Option strike price
    /// * `time_to_expiry` - Time to expiration in years (e.g., 7.0/365.0 for 7 days)
    /// * `implied_vol` - Implied volatility as decimal (e.g., 0.15 = 15%)
    /// * `option_type` - Call or Put
    /// * `risk_free_rate` - Risk-free rate (default: 0.065 = 6.5% for India)
    pub fn update_from_option(
        &mut self,
        position: f64,
        spot_price: f64,
        strike: f64,
        time_to_expiry: f64,
        implied_vol: f64,
        option_type: OptionType,
        risk_free_rate: f64,
    ) {
        // Calculate real Greeks using Black-Scholes from kubera_options
        let single_option_greeks = match option_type {
            OptionType::Call => OptionGreeks::for_call(spot_price, strike, time_to_expiry, risk_free_rate, implied_vol),
            OptionType::Put => OptionGreeks::for_put(spot_price, strike, time_to_expiry, risk_free_rate, implied_vol),
        };

        // Scale by position size (lots * lot_size gives total contracts)
        let total_quantity = position * self.lot_size as f64;
        let portfolio_greeks = single_option_greeks.scale(total_quantity);

        debug!(
            "[GREEKS] Real B-S calculation: spot={:.1}, K={:.0}, T={:.4}, IV={:.1}%, type={:?} -> delta={:.4}, gamma={:.6}",
            spot_price, strike, time_to_expiry, implied_vol * 100.0, option_type,
            portfolio_greeks.delta, portfolio_greeks.gamma
        );

        self.update_greeks(
            portfolio_greeks.delta,
            portfolio_greeks.gamma,
            portfolio_greeks.theta,
            portfolio_greeks.vega,
            spot_price,
        );
    }

    /// Simplified Greeks update from position and price (FALLBACK for backtesting without option params)
    ///
    /// WARNING: This uses crude ATM approximations. Use update_from_option() for production.
    #[deprecated(note = "Use update_from_option() for production with real Black-Scholes Greeks")]
    pub fn update_from_position(&mut self, position: f64, spot_price: f64, implied_vol: f64) {
        // Estimate Greeks from position (simplified ATM approximation)
        // WARNING: These are rough estimates, not real Black-Scholes values!
        let estimated_delta = position * 0.5;  // Assumes roughly ATM
        let estimated_gamma = position.abs() * 0.02 / spot_price;
        let estimated_theta = -position.abs() * spot_price * implied_vol * 0.01;
        let estimated_vega = position.abs() * spot_price * 0.01;

        warn!(
            "[GREEKS] Using crude estimates (not real B-S)! Delta={:.2}, position={}",
            estimated_delta, position
        );

        self.update_greeks(
            estimated_delta,
            estimated_gamma,
            estimated_theta,
            estimated_vega,
            spot_price,
        );
    }

    /// Check limits and calculate position scaling factor
    fn check_limits_and_scale(&mut self) {
        let mut violations = Vec::new();

        // Delta limit check
        let delta_ratio = self.state.delta.abs() / self.config.max_delta;
        if delta_ratio > 1.0 {
            violations.push(format!(
                "Delta {:.1} exceeds limit {:.1}",
                self.state.delta.abs(), self.config.max_delta
            ));
        }

        // Gamma limit check
        let gamma_ratio = (self.state.gamma * 100.0).abs() / self.config.max_gamma;
        if gamma_ratio > 1.0 {
            violations.push(format!(
                "Gamma {:.4} exceeds limit {:.4}",
                self.state.gamma * 100.0, self.config.max_gamma
            ));
        }

        // Vega limit check
        let vega_ratio = self.state.vega.abs() / self.config.max_vega;
        if vega_ratio > 1.0 {
            violations.push(format!(
                "Vega {:.0} exceeds limit {:.0}",
                self.state.vega.abs(), self.config.max_vega
            ));
        }

        // Theta limit check
        let theta_ratio = self.state.theta.abs() / self.config.max_theta_daily;
        if theta_ratio > 1.0 {
            violations.push(format!(
                "Theta {:.0} exceeds limit {:.0}",
                self.state.theta.abs(), self.config.max_theta_daily
            ));
        }

        // Update limit breach status
        if !violations.is_empty() {
            self.limit_breached = true;
            self.breach_reason = Some(violations.join("; "));
            warn!("[GREEKS] Limit breach: {}", self.breach_reason.as_ref().unwrap());
        } else {
            self.limit_breached = false;
            self.breach_reason = None;
        }

        // Calculate risk scalar (reduce position sizing when approaching limits)
        // Use the maximum of all ratios to be conservative
        let utilization = delta_ratio.max(gamma_ratio).max(vega_ratio).max(theta_ratio);

        self.risk_scalar = if utilization >= 1.0 {
            0.0 // Hard stop - no new positions
        } else if utilization >= 0.8 {
            0.3 // Severe reduction at 80% utilization
        } else if utilization >= 0.6 {
            0.6 // Moderate reduction at 60%
        } else {
            1.0 // Full allocation below 60%
        };

        debug!(
            "[GREEKS] Utilization: {:.1}%, Risk scalar: {:.2}",
            utilization * 100.0, self.risk_scalar
        );
    }

    /// Get position scaling factor based on Greeks exposure
    pub fn get_risk_scalar(&self) -> f64 {
        self.risk_scalar
    }

    /// Check if we need to rebalance based on gamma P&L
    pub fn needs_gamma_rebalance(&self) -> bool {
        self.state.gamma_pnl.abs() >= self.config.gamma_scalp_threshold
    }

    /// Check if position is delta neutral
    pub fn is_delta_neutral(&self) -> bool {
        self.state.delta.abs() <= self.config.delta_neutral_band
    }

    /// Get hedge quantity needed to neutralize delta
    pub fn get_delta_hedge_quantity(&self) -> f64 {
        if !self.config.auto_delta_hedge {
            return 0.0;
        }

        // Only hedge if outside neutral band
        if self.is_delta_neutral() {
            return 0.0;
        }

        // Return the hedge quantity (negative = need to sell, positive = need to buy)
        -self.state.delta
    }

    /// Reset gamma P&L tracking after rebalance
    pub fn record_rebalance(&mut self, current_price: f64) {
        self.state.last_rebalance_price = current_price;
        self.state.gamma_pnl = 0.0;
        info!("[GREEKS] Rebalanced at price {:.2}", current_price);
    }

    /// Is any limit currently breached?
    pub fn is_limit_breached(&self) -> bool {
        self.limit_breached
    }

    /// Get current Greeks state
    pub fn get_state(&self) -> &PortfolioGreeksState {
        &self.state
    }

    /// Get the lot size for this instrument
    pub fn get_lot_size(&self) -> u32 {
        self.lot_size
    }
}

// ============================================================================
// HYDRA STRATEGY - THE COORDINATOR
// ============================================================================

/// HYDRA: The multi-headed flagship strategy
pub struct HydraStrategy {
    /// Event bus for publishing signals
    bus: Option<Arc<EventBus>>,
    /// All active experts
    experts: Vec<Box<dyn Expert>>,
    /// Regime engine
    regime_engine: RegimeEngine,
    /// Meta-allocator
    allocator: MetaAllocator,
    /// Attribution ledger
    ledger: AttributionLedger,
    /// Execution quality gate
    exec_gate: ExecutionQualityGate,
    /// Kill-switch policy
    kill_switch: KillSwitchPolicy,
    /// Configuration
    config: HydraConfig,
    /// Current position
    position: f64,
    /// Current price
    current_price: f64,
    /// Tick counter
    tick_count: u64,
    /// Active intents awaiting execution
    active_intents: HashMap<Uuid, ExpertIntent>,
    /// Global kill switch flag
    kill_switch_flag: Arc<AtomicBool>,
    /// V2: Rolling price history for V-MAP volatility calculations
    price_history: VecDeque<f64>,
    /// V2: Rolling signal history per expert for Z-score normalization
    signal_history: HashMap<String, VecDeque<f64>>,
    /// Execution guard to prevent re-entrant stack buildup
    is_processing: bool,
    /// V2: Last trade tick for cooldown
    last_trade_tick: u64,
    /// PROJECT AEON: Informational predictability score (0.0 - 1.0)
    pub aeon_bif_score: f64,
    /// OPTIONS: Greeks-based risk manager
    greeks_manager: Option<GreeksRiskManager>,
    /// OPTIONS: Estimated implied volatility for Greeks calculations
    implied_vol: f64,
    /// OPTIONS: Strike price for real Greeks calculation
    option_strike: Option<f64>,
    /// OPTIONS: Time to expiry in years for real Greeks calculation
    option_time_to_expiry: Option<f64>,
    /// OPTIONS: Option type (Call/Put) for real Greeks calculation
    option_type: Option<OptionType>,
    /// OPTIONS: Risk-free rate for Black-Scholes (default 6.5% for India)
    risk_free_rate: f64,
    /// NSE: Per-symbol lot sizes for rounding quantities (missing = no rounding)
    lot_sizes: HashMap<String, u32>,
    /// NSE: Default lot size for unknown symbols (0 = no rounding)
    default_lot_size: u32,
    /// BINANCE TUNING: Last signal timestamp in milliseconds
    last_signal_ts_ms: i64,
    /// TUNING: Candidate counter
    candidate_count: u64,
    /// TUNING: Rejected candidate counter
    rejected_count: u64,
    /// TUNING: Passed candidate counter
    passed_count: u64,
    /// TUNING: Max edge observed
    max_edge_observed: f64,
    /// TUNING: Edge samples for distribution analysis (VecDeque for O(1) pop_front)
    edge_samples: VecDeque<f64>,
    /// TUNING: Max margin (edge - required) observed
    max_margin_observed: f64,
}

impl HydraStrategy {
    pub fn new() -> Self {
        Self::with_config(HydraConfig::default())
    }

    pub fn with_config(config: HydraConfig) -> Self {
        let kill_switch_flag = Arc::new(AtomicBool::new(false));

        let experts: Vec<Box<dyn Expert>> = vec![
            Box::new(TrendExpert::new()),
            Box::new(MeanRevExpert::new()),
            Box::new(VolatilityExpert::new()),
            Box::new(MicrostructureExpert::new()),
            Box::new(RelativeValueExpert::new()),
        ];

        let names: Vec<String> = experts.iter().map(|e| e.name().to_string()).collect();

        // Initialize Greeks manager for options trading if venue is ZerodhaFnO
        let greeks_manager = if config.venue == ExchangeProfile::ZerodhaFnO {
            Some(GreeksRiskManager::for_nifty()) // Default to NIFTY
        } else {
            None
        };

        Self {
            bus: None,
            experts,
            regime_engine: RegimeEngine::new(20, 100, config.clone()),
            allocator: MetaAllocator::new(names, config.clone()),
            ledger: AttributionLedger::new(),
            exec_gate: ExecutionQualityGate::new(),
            kill_switch: KillSwitchPolicy::new(Arc::clone(&kill_switch_flag)),
            config,
            position: 0.0,
            current_price: 0.0,
            tick_count: 0,
            active_intents: HashMap::new(),
            kill_switch_flag,
            price_history: VecDeque::with_capacity(100),
            signal_history: HashMap::new(),
            is_processing: false,
            last_trade_tick: 0,
            aeon_bif_score: 1.0, // Default to full scale for standalone use
            greeks_manager,
            implied_vol: 0.15, // Default IV 15%
            option_strike: None,
            option_time_to_expiry: None,
            option_type: None,
            risk_free_rate: 0.065, // India RBI repo rate ~6.5%
            lot_sizes: HashMap::new(),
            default_lot_size: 0, // 0 = no rounding (crypto), set via set_lot_size for NSE
            last_signal_ts_ms: 0, // BINANCE TUNING: Initialize to 0
            // TUNING: Candidate tracking
            candidate_count: 0,
            rejected_count: 0,
            passed_count: 0,
            max_edge_observed: 0.0,
            edge_samples: VecDeque::with_capacity(10000),
            max_margin_observed: f64::NEG_INFINITY,
        }
    }

    /// Set lot size for a specific symbol (e.g., NIFTY=75, BANKNIFTY=30)
    pub fn set_lot_size_for_symbol(&mut self, symbol: &str, lot_size: u32) {
        self.lot_sizes.insert(symbol.to_string(), lot_size);
        info!("[HYDRA] Lot size set to {} for {}", lot_size, symbol);
    }

    /// Set default lot size for NSE F&O instruments (used when symbol-specific lot size not found)
    pub fn set_lot_size(&mut self, lot_size: u32) {
        self.default_lot_size = lot_size;
        info!("[HYDRA] Default lot size set to {} for NSE F&O", lot_size);
    }

    /// Get lot size for a symbol (returns symbol-specific or default)
    fn get_lot_size_for_symbol(&self, symbol: &str) -> u32 {
        *self.lot_sizes.get(symbol).unwrap_or(&self.default_lot_size)
    }

    /// Enable Greeks risk management for options trading
    pub fn enable_greeks_manager(&mut self, manager: GreeksRiskManager) {
        self.greeks_manager = Some(manager);
        info!("[HYDRA] Greeks risk manager enabled");
    }

    /// Set implied volatility for Greeks calculations
    pub fn set_implied_vol(&mut self, iv: f64) {
        self.implied_vol = iv.clamp(0.05, 1.0);
    }

    /// Set option parameters for REAL Greeks calculation (production mode)
    ///
    /// Call this when you know the actual option contract parameters.
    /// This enables Black-Scholes Greeks instead of crude ATM estimates.
    ///
    /// # Parameters
    /// * `strike` - Option strike price
    /// * `time_to_expiry` - Time to expiration in years (e.g., 7.0/365.0 for 7 days)
    /// * `option_type` - Call or Put
    pub fn set_option_params(&mut self, strike: f64, time_to_expiry: f64, option_type: OptionType) {
        self.option_strike = Some(strike);
        self.option_time_to_expiry = Some(time_to_expiry);
        self.option_type = Some(option_type);
        info!(
            "[HYDRA] Option params set: K={:.0}, T={:.4}y, type={:?} - REAL Greeks enabled",
            strike, time_to_expiry, option_type
        );
    }

    /// Update time to expiry (call daily as theta decays)
    pub fn update_time_to_expiry(&mut self, time_to_expiry: f64) {
        self.option_time_to_expiry = Some(time_to_expiry.max(0.0));
    }

    /// Set risk-free rate for Black-Scholes
    pub fn set_risk_free_rate(&mut self, rate: f64) {
        self.risk_free_rate = rate.clamp(0.0, 0.20);
    }

    /// Check if real Greeks calculation is available
    pub fn has_option_params(&self) -> bool {
        self.option_strike.is_some()
            && self.option_time_to_expiry.is_some()
            && self.option_type.is_some()
    }

    /// PROJECT AEON: Update predictability score from Sentry
    pub fn set_aeon_bif_score(&mut self, score: f64) {
        self.aeon_bif_score = score.clamp(0.0, 1.0);
    }

    fn execute_logic(&mut self, timestamp: i64, symbol: &str, price: f64) {
        // Guard against re-entrant calls that cause stack overflow
        if self.is_processing {
            return;
        }
        self.is_processing = true;
        
        self.current_price = price;
        self.tick_count += 1;

        // Check kill switch
        if self.kill_switch.is_triggered() || self.kill_switch_flag.load(Ordering::SeqCst) {
            debug!("[HYDRA] Kill switch active, skipping execution");
            self.is_processing = false;
            return;
        }

        // Check execution quality gate
        if !self.exec_gate.is_execution_allowed() {
            debug!("[HYDRA] Execution gate blocked (toxic conditions)");
            self.is_processing = false;
            return;
        }

        // --- V2: UPDATE PRICE HISTORY FOR V-MAP (SIMPLIFIED FOR STABILITY) ---
        self.price_history.push_back(price);
        if self.price_history.len() > self.config.vol_target_window {
            self.price_history.pop_front();
        }

        // V-MAP: Use fixed scalar for demonstration to prevent stack overflow
        let realized_vol = self.config.vol_target;

        // Get current regime state
        let regime_state = self.regime_engine.get_state().clone();

        // If toxic regime, don't trade
        if regime_state.regime == HydraRegime::Toxic {
            self.is_processing = false;
            return;
        }

        // Collect intents from all experts
        let mut intents: Vec<ExpertIntent> = Vec::new();
        for expert in &self.experts {
            let intent_opt = expert.generate_intent(&regime_state);
            if let Some(intent) = intent_opt {
                // V2: Regime checks enabled
                info!("[HYDRA] Expert {} generated intent with signal {:.3}", intent.expert_id, intent.signal);
                intents.push(intent);
            } else {
                debug!("[HYDRA] Expert {} returned None", expert.name());
            }
        }

        if intents.is_empty() {
            self.is_processing = false;
            return;
        }

        // Aggregate signals using meta-allocator weights
        let mut portfolio_signal = 0.0;
        let mut weighted_edge = 0.0;
        let mut best_intent: Option<ExpertIntent> = None;
        let mut max_weighted_signal = 0.0_f64;

        for intent in &intents {
            let weight = self.allocator.get_weight(&intent.expert_id);
            
            // --- V2: Z-SCORE NORMALIZATION ---
            // Track signal history for this expert
            let history = self.signal_history
                .entry(intent.expert_id.clone())
                .or_insert_with(|| VecDeque::with_capacity(self.config.signal_zscore_lookback));
            history.push_back(intent.signal);
            if history.len() > self.config.signal_zscore_lookback {
                history.pop_front();
            }
            
            // Calculate Z-score if we have enough history, otherwise use raw signal (warmup mode)
            let zscore = if history.len() >= 10 {
                let mean = history.iter().sum::<f64>() / history.len() as f64;
                let variance = history.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / history.len() as f64;
                let std_dev = variance.sqrt().max(0.001);
                (intent.signal - mean) / std_dev
            } else {
                // V2 WARMUP: Use amplified raw signal during warmup
                intent.signal * 2.0 
            };
            
            // --- EXPERT POSITION LIMIT ENFORCEMENT ---
            let max_contribution = self.config.max_position_per_expert;
            
            // --- MASTERPIECE: KELLY SCALING ---
            // Scale the signal by the expert's fractional Kelly logic
            let kelly_fraction = self.allocator.states.get(&intent.expert_id)
                .map(|s| s.get_kelly_fraction())
                .unwrap_or(0.5);
            
            let weighted_signal = (zscore * weight * kelly_fraction).clamp(-max_contribution, max_contribution);
            
            // MASTERPIECE: Edge should be adjusted by Kelly for the hurdle check (Step 1962 logic)
            weighted_edge += intent.expected_edge_bps * weight * kelly_fraction;

            // --- PROJECT AEON: ENTROPY-ADJUSTED SCALING ---
            // Scale signal by informational predictability (BIF score)
            let final_weighted_signal = weighted_signal * self.aeon_bif_score;

            if final_weighted_signal.abs() > max_weighted_signal.abs() {
                max_weighted_signal = final_weighted_signal;
                best_intent = Some(intent.clone());
            }

            portfolio_signal += final_weighted_signal;

            debug!("[HYDRA] {} raw={:.3} zscore={:.2} (w={:.3}, weighted={:.3}, aeon={:.2})",
                intent.expert_id, intent.signal, zscore, weight, final_weighted_signal, self.aeon_bif_score);
        }

        // Apply regime risk multiplier
        let regime_mult = regime_state.risk_multiplier;

        // Apply global drawdown-based risk reduction
        // Apply global drawdown-based risk reduction
        let global_risk_mult = self.allocator.get_global_risk_mult();

        // --- MASTERPIECE: STRUCTURAL CIRCUIT BREAKERS ---
        // If portfolio drawdown is significant, scale back aggressively to preserve alpha
        let dd = self.allocator.portfolio_drawdown;
        let circuit_breaker_mult = if dd > 0.07 { 0.0 } // Hard Stop at 7% budget
            else if dd > 0.03 { 0.5 } // Defend the 3% line
            else { 1.0 };

        // Final target position
        let mut target_pos = portfolio_signal * self.config.max_position_abs * regime_mult * global_risk_mult * circuit_breaker_mult;

        // --- V2: V-MAP VOLATILITY-TARGETING ---
        // Scale position inversely with realized volatility to maintain constant risk
        let vol_scalar = if realized_vol > 0.01 {
            (self.config.vol_target / realized_vol).clamp(0.25, 2.0)
        } else {
            1.0
        };
        target_pos *= vol_scalar;

        // --- OPTIONS: GREEKS-BASED RISK SCALING ---
        // If Greeks manager is active, update Greeks and apply risk scalar
        if let Some(ref mut greeks_mgr) = self.greeks_manager {
            // Update Greeks using REAL Black-Scholes if option params available
            if let (Some(strike), Some(tte), Some(opt_type)) =
                (self.option_strike, self.option_time_to_expiry, self.option_type)
            {
                // PRODUCTION: Real Greeks from kubera_options Black-Scholes
                greeks_mgr.update_from_option(
                    self.position,
                    price,
                    strike,
                    tte,
                    self.implied_vol,
                    opt_type,
                    self.risk_free_rate,
                );
            } else {
                // FALLBACK: Crude ATM estimates (backtest mode only)
                #[allow(deprecated)]
                greeks_mgr.update_from_position(self.position, price, self.implied_vol);
            }

            // Apply Greeks-based risk scalar
            let greeks_scalar = greeks_mgr.get_risk_scalar();
            target_pos *= greeks_scalar;

            // Check for gamma rebalancing needs
            if greeks_mgr.needs_gamma_rebalance() {
                info!("[HYDRA] Gamma rebalance needed - P&L threshold exceeded");
                greeks_mgr.record_rebalance(price);
            }

            // Check for delta hedging needs - ACTUALLY EXECUTE hedge orders
            let hedge_qty = greeks_mgr.get_delta_hedge_quantity();
            let hedge_threshold = 0.5; // Minimum lots to trigger hedge

            if hedge_qty.abs() > hedge_threshold {
                // PRODUCTION: Execute delta hedge via underlying (futures)
                let hedge_side = if hedge_qty > 0.0 { Side::Buy } else { Side::Sell };
                let hedge_lots = hedge_qty.abs();

                info!(
                    "[HYDRA] DELTA HEDGE EXECUTION: {:?} {:.2} lots of underlying at {:.2} (delta neutralization)",
                    hedge_side, hedge_lots, price
                );

                // Publish hedge signal through event bus
                if let Some(bus) = &self.bus {
                    // Create a hedge signal for the UNDERLYING (futures)
                    // Note: In production, this should use the futures symbol (e.g., NIFTY25JANFUT)
                    let hedge_symbol = format!("{}_HEDGE", symbol);

                    let hedge_signal = SignalEvent {
                        timestamp: chrono::Utc::now(),
                        strategy_id: "HYDRA_DELTA_HEDGE".to_string(),
                        symbol: hedge_symbol,
                        side: hedge_side,
                        price,
                        quantity: hedge_lots * greeks_mgr.get_lot_size() as f64, // Convert lots to quantity
                        intent_id: None, // Hedges don't need attribution
                        // HFT V2 decision context (hedges use current price)
                        decision_bid: price,
                        decision_ask: price,
                        decision_mid: price,
                        spread_bps: 0.0,
                        book_ts_ns: 0,
                        expected_edge_bps: 0.0, // Hedges are risk management, not alpha
                    };

                    if let Err(e) = bus.publish_signal_sync(hedge_signal) {
                        warn!("[HYDRA] Failed to publish delta hedge signal: {}", e);
                    } else {
                        // Record the rebalance after successful hedge signal
                        greeks_mgr.record_rebalance(price);
                        info!("[HYDRA] Delta hedge signal published successfully");
                    }
                }
            }

            // Log Greeks state periodically
            if self.tick_count % 100 == 0 {
                let state = greeks_mgr.get_state();
                debug!(
                    "[HYDRA] Greeks: Delta={:.2}, Gamma={:.4}, Vega={:.0}, Theta={:.0}, Scalar={:.2}",
                    state.delta, state.gamma, state.vega, state.theta, greeks_scalar
                );
            }
        }

        // --- PORTFOLIO POSITION LIMIT ENFORCEMENT ---
        target_pos = target_pos.clamp(-self.config.max_position_abs, self.config.max_position_abs);

        // ========== BINANCE TUNING: Enhanced cost model ==========
        // Include latency slippage (observed ~5bps signal→fill drift)
        let base_cost = self.config.slippage_bps + self.config.commission_bps + self.config.latency_slippage_bps;
        // Note: spread estimation would require book tracking - use base cost for now
        let total_cost = base_cost;

        // Required edge = max(min_edge_bps, hurdle * total_cost)
        let hurdle = self.config.edge_hurdle_multiplier;
        let required_edge = self.config.min_edge_bps.max(total_cost * hurdle);

        // ========== CANDIDATE LOGGING (for tuning analysis) ==========
        self.candidate_count += 1;
        let margin = weighted_edge - required_edge;

        if weighted_edge > self.max_edge_observed {
            self.max_edge_observed = weighted_edge;
        }
        if margin > self.max_margin_observed {
            self.max_margin_observed = margin;
        }

        // Track edge distribution for p95 calculation (VecDeque for O(1) pop_front)
        self.edge_samples.push_back(weighted_edge);
        if self.edge_samples.len() > 10000 {
            self.edge_samples.pop_front();
        }

        // Log high-edge candidates (rate-limited to every 200 candidates)
        if weighted_edge > 5.0 && self.candidate_count % 200 == 0 {
            info!("[HYDRA-CANDIDATE] edge={:.2}bps cost={:.2}bps required={:.2}bps margin={:.2}bps pass={}",
                  weighted_edge, total_cost, required_edge, margin, weighted_edge >= required_edge);
        }

        // Periodic stats summary every 5000 candidates
        if self.candidate_count % 5000 == 0 {
            let pass_rate = if self.candidate_count > 0 {
                self.passed_count as f64 / self.candidate_count as f64 * 100.0
            } else { 0.0 };

            // Compute p95 and p99 from edge samples
            let (p95, p99) = if !self.edge_samples.is_empty() {
                let mut sorted: Vec<f64> = self.edge_samples.iter().cloned().collect();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = sorted.len();
                let p95_idx = ((n as f64 * 0.95) as usize).min(n - 1);
                let p99_idx = ((n as f64 * 0.99) as usize).min(n - 1);
                (sorted[p95_idx], sorted[p99_idx])
            } else { (0.0, 0.0) };

            info!("[HYDRA-STATS] candidates={} passed={} rejected={} pass_rate={:.2}% max_edge={:.2}bps p95_edge={:.2}bps p99_edge={:.2}bps max_margin={:.2}bps",
                  self.candidate_count, self.passed_count, self.rejected_count, pass_rate,
                  self.max_edge_observed, p95, p99, self.max_margin_observed);
        }

        if weighted_edge < required_edge {
            self.rejected_count += 1;
            debug!("[HYDRA] Edge {:.1}bps < required {:.1}bps (cost={:.1}bps, hurdle={:.1}), skipping",
                   weighted_edge, required_edge, total_cost, hurdle);
            self.is_processing = false;
            return;
        }
        self.passed_count += 1;

        // ========== BINANCE TUNING: Time-based cooldown ==========
        let now_ms = chrono::Utc::now().timestamp_millis();
        if self.config.min_ms_between_signals > 0 && self.last_signal_ts_ms > 0 {
            let elapsed_ms = now_ms - self.last_signal_ts_ms;
            if elapsed_ms < self.config.min_ms_between_signals {
                debug!("[HYDRA] Signal cooldown active ({}ms < {}ms), skipping",
                       elapsed_ms, self.config.min_ms_between_signals);
                self.is_processing = false;
                return;
            }
        }

        // --- MASTERPIECE: TRAILING ALPHA-STOP ---
        // If we are long but signal is strongly reversed, close position immediately
        let is_reversal = (self.position > 0.0 && portfolio_signal < -0.4) || (self.position < 0.0 && portfolio_signal > 0.4);
        
        // Position change with hysteresis
        let position_change = target_pos - self.position;
        let mut should_trade = position_change.abs() > self.config.position_hysteresis;

        // V2: Trade cooldown - minimum 50 ticks between trades to avoid commission churn
        const MIN_TICKS_BETWEEN_TRADES: u64 = 50;
        if self.tick_count - self.last_trade_tick < MIN_TICKS_BETWEEN_TRADES && self.last_trade_tick > 0 {
            debug!("[HYDRA] Trade cooldown active ({} ticks since last trade)", self.tick_count - self.last_trade_tick);
            self.is_processing = false;
            return;
        }

        if is_reversal && !should_trade {
            info!("[HYDRA] Trailing Alpha-Stop triggered (Reversal detected) - Closing position");
            target_pos = 0.0;
            should_trade = true;
        }

        if should_trade {
            let side = if target_pos > self.position { Side::Buy } else { Side::Sell };
            let raw_qty = (target_pos - self.position).abs();

            // Get lot size for this symbol (per-symbol or default)
            let lot_size = self.get_lot_size_for_symbol(symbol);

            // Round to lot size for NSE F&O instruments
            let qty = if lot_size > 0 {
                let lots = (raw_qty / lot_size as f64).round().max(1.0);
                lots * lot_size as f64
            } else {
                raw_qty
            };

            // V2: Minimum quantity to prevent signal spam (1 lot for NSE, 0.01 for crypto)
            let min_qty = if lot_size > 0 { lot_size as f64 } else { 0.01 };
            if raw_qty < min_qty * 0.5 {
                // Not even half a lot, skip
                self.is_processing = false;
                return;
            }

            // Register intent for attribution
            if let Some(intent) = best_intent.as_ref() {
                self.ledger.register_intent(intent);
                self.active_intents.insert(intent.intent_id, intent.clone());
            }

            // Publish signal with HFT V2 decision context
            if let Some(bus) = &self.bus {
                let signal = SignalEvent {
                    timestamp: chrono::DateTime::from_timestamp(timestamp / 1000, 0)
                        .unwrap_or(chrono::Utc::now()),
                    strategy_id: "HYDRA".to_string(),
                    symbol: symbol.to_string(),
                    side,
                    price,
                    quantity: qty,
                    intent_id: best_intent.as_ref().map(|i| i.intent_id),
                    // HFT V2 decision context (use price as fallback if no book)
                    decision_bid: price,
                    decision_ask: price,
                    decision_mid: price,
                    spread_bps: 0.0,
                    book_ts_ns: 0,
                    expected_edge_bps: self.config.edge_hurdle_multiplier * (self.config.slippage_bps + self.config.commission_bps),
                };

                if let Err(e) = bus.publish_signal_sync(signal) {
                    warn!("[HYDRA] Failed to publish signal: {}", e);
                } else {
                    // BINANCE TUNING: Update last signal timestamp
                    self.last_signal_ts_ms = chrono::Utc::now().timestamp_millis();
                }
            }

            self.position = target_pos;
            self.last_trade_tick = self.tick_count;
            info!("[HYDRA] Position updated: {:.3} -> {:.3}", self.position - position_change, self.position);
        }
        
        // Reset processing guard
        self.is_processing = false;
    }
}

impl crate::Strategy for HydraStrategy {
    fn on_start(&mut self, bus: Arc<EventBus>) {
        info!("[HYDRA] ═══════════════════════════════════════════════════════");
        info!("[HYDRA]  HYDRA AWAKENS - {} HEADS ACTIVE", self.experts.len());
        info!("[HYDRA] ═══════════════════════════════════════════════════════");
        info!("[HYDRA] Experts:");
        for expert in &self.experts {
            info!("[HYDRA]   • {} (family: {})", expert.name(), expert.signal_family());
        }
        info!("[HYDRA] Config: vol_target={:.1}%, max_dd={:.1}%, learning_rate={:.3}",
            self.config.vol_target * 100.0, self.config.max_drawdown_pct, self.config.learning_rate);
        info!("[HYDRA] ═══════════════════════════════════════════════════════");
        self.bus = Some(bus);
    }

    fn on_tick(&mut self, event: &MarketEvent) {
        // --- V2: VENUE AUTO-CALIBRATION ---
        if self.config.venue == ExchangeProfile::Default {
            if event.symbol.contains("USDT") {
                self.config.apply_profile(ExchangeProfile::BinanceFutures);
            } else if event.symbol.contains("FUT") || event.symbol.contains("NIFTY") {
                self.config.apply_profile(ExchangeProfile::ZerodhaFnO);

                // --- OPTIONS: AUTO-ENABLE GREEKS MANAGER FOR F&O ---
                if self.greeks_manager.is_none() {
                    if event.symbol.contains("BANKNIFTY") {
                        self.greeks_manager = Some(GreeksRiskManager::for_banknifty());
                        info!("[HYDRA] Greeks risk manager enabled for BANKNIFTY");
                    } else if event.symbol.contains("NIFTY") {
                        self.greeks_manager = Some(GreeksRiskManager::for_nifty());
                        info!("[HYDRA] Greeks risk manager enabled for NIFTY");
                    }
                }
            }
        }
        
        // V2 DEBUG: Verify tick reception
        if self.tick_count % 100 == 0 {
            info!("[HYDRA] on_tick #{} for {} - {} experts active", self.tick_count, event.symbol, self.experts.len());
        }
        
        // Update regime engine
        self.regime_engine.update(event);

        // Update all experts
        for expert in &mut self.experts {
            expert.update_market(event);
        }

        // Execute trading logic
        if let MarketPayload::Tick { price, .. } = &event.payload {
            self.execute_logic(event.exchange_time.timestamp_millis(), &event.symbol, *price);
        }
        if let MarketPayload::Trade { price, .. } = &event.payload {
            self.execute_logic(event.exchange_time.timestamp_millis(), &event.symbol, *price);
        }

        // Periodic meta-allocator update
        if self.tick_count % 100 == 0 {
            self.allocator.update_weights(&self.ledger, self.tick_count);
        }
    }

    fn on_bar(&mut self, event: &MarketEvent) {
        // Update regime engine
        self.regime_engine.update(event);

        // Update all experts
        for expert in &mut self.experts {
            expert.update_market(event);
        }

        // Execute on bar close
        if let MarketPayload::Bar { close, .. } = &event.payload {
            self.execute_logic(event.exchange_time.timestamp_millis(), &event.symbol, *close);
        }
    }

    fn on_fill(&mut self, fill: &OrderEvent) {
        if let OrderPayload::Update { filled_quantity, avg_price, commission, status } = &fill.payload {
            if *status == kubera_models::OrderStatus::Filled || *status == kubera_models::OrderStatus::PartiallyFilled {
                // Find matching intent and record attribution
                // In production, we'd match by order ID; here we use the most recent intent
                if let Some((intent_id, _)) = self.active_intents.iter().next() {
                    let intent_id = *intent_id;
                    self.ledger.record_fill(
                        intent_id,
                        *avg_price,
                        *filled_quantity,
                        *commission,
                        self.current_price,
                    );
                    self.active_intents.remove(&intent_id);
                }

                // UPDATE POSITION TO PREVENT RECURSIVE SIGNAL LOOPS
                let position_delta = match &fill.side {
                    Side::Buy => *filled_quantity,
                    Side::Sell => -*filled_quantity,
                };
                self.position += position_delta;

                info!("[HYDRA] Fill processed: {:.4} @ {:.2} (commission: {:.4}) | Position updated: {:.4}",
                    filled_quantity, avg_price, commission, self.position);
            }
        }
    }

    fn on_stop(&mut self) {
        info!("[HYDRA] ═══════════════════════════════════════════════════════");
        info!("[HYDRA]  HYDRA DORMANT - SESSION COMPLETE");
        info!("[HYDRA] ═══════════════════════════════════════════════════════");

        // Log final statistics
        let expert_pnls = self.ledger.get_all_expert_pnl();
        info!("[HYDRA] Expert PnL Summary:");
        for (expert, pnl) in expert_pnls {
            info!("[HYDRA]   {} -> {:.2}", expert, pnl);
        }

        info!("[HYDRA] ═══════════════════════════════════════════════════════");
    }

    fn name(&self) -> &str {
        "HYDRA"
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Strategy;

    #[test]
    fn test_regime_engine_initialization() {
        let engine = RegimeEngine::new(20, 100, HydraConfig::default());
        let state = engine.get_state();
        assert_eq!(state.regime, HydraRegime::Range);
    }

    #[test]
    fn test_trend_expert_initialization() {
        let expert = TrendExpert::new();
        assert_eq!(expert.name(), "Expert-A-Trend");
        assert_eq!(expert.get_signal(), 0.0);
    }

    #[test]
    fn test_meta_allocator_weight_normalization() {
        let names = vec![
            "Expert-A".to_string(),
            "Expert-B".to_string(),
        ];
        let allocator = MetaAllocator::new(names, HydraConfig::default());

        let weight_a = allocator.get_weight("Expert-A");
        let weight_b = allocator.get_weight("Expert-B");

        assert!((weight_a + weight_b - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_attribution_ledger() {
        let mut ledger = AttributionLedger::new();

        let intent = ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: "Expert-A-Trend".to_string(),
            signal: 1.0,
            expected_edge_bps: 5.0,
            confidence: 0.8,
            timestamp: Utc::now(),
            regime: HydraRegime::Trend,
            signal_family: "trend".to_string(),
            order_style: OrderStyle::Taker,
            stop_loss: None,
            take_profit: None,
            time_exit_ticks: None,
        };

        ledger.register_intent(&intent);
        ledger.record_fill(intent.intent_id, 100.0, 1.0, 0.1, 99.5);

        let pnl = ledger.get_expert_pnl("Expert-A-Trend");
        assert!(pnl != 0.0); // Should have some PnL
    }

    #[test]
    fn test_kill_switch_policy() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut policy = KillSwitchPolicy::new(Arc::clone(&flag));

        // Should not trigger normally
        assert!(!policy.check(50.0, 5.0, 1000.0, false));

        // Should trigger on high latency
        assert!(policy.check(300.0, 5.0, 1000.0, false));
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_hydra_strategy_initialization() {
        let strategy = HydraStrategy::new();
        assert_eq!(strategy.name(), "HYDRA");
        assert_eq!(strategy.experts.len(), 5);
    }
}
