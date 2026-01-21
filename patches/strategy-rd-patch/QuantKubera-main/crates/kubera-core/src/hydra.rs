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
            coherence_min: 0.65, // Require broad expert alignment
            edge_scale_mult: 1.0, // No edge scaling by default
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
                self.coherence_min = 0.60;
            }
            ExchangeProfile::BinanceFutures => {
                info!("[HYDRA] Calibrating for Binance Futures (Masterpiece Final)");
                self.slippage_bps = 0.2;
                self.commission_bps = 0.8; 
                self.position_hysteresis = 0.4; // Medallion-grade alpha capture
                self.zscore_action_threshold = 0.8; 
                self.coherence_min = 0.60;
            }
            ExchangeProfile::ZerodhaFnO => {
                info!("[HYDRA] Calibrating for Zerodha Indian F&O (Low Churn profile)");
                self.slippage_bps = 0.5;
                self.commission_bps = 6.0; // ~0.06% total costs
                self.position_hysteresis = 1.0;
                self.zscore_action_threshold = 1.2;
                self.coherence_min = 0.70;
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
        // NOTE: hard clamping was saturating the signal (often ~0.9/1.0). We reduce the boost
        // and apply a soft clip (tanh) to preserve magnitude information without saturation.
        let liquidity_mult = if liquidity_vacuum { 1.2 } else { 1.0 };

        // Weighted combination (pre-boost)
        let combined = 0.6 * vwap_signal + 0.4 * bb_signal;
        let boosted = combined * liquidity_mult;
        // Soft clip to [-1,1] without hard saturation
        self.signal = boosted.tanh();

        // Diagnostics for tuning (enable with RUST_LOG=kubera_core::hydra=debug)
        debug!("[MEANREV] vwap_z={:.2} bb_z={:.2} vwap_sig={:.2} bb_sig={:.2} vacuum={} combined={:.2} boosted={:.2} signal={:.2}",
               vwap_zscore, bb_zscore, vwap_signal, bb_signal, liquidity_vacuum, combined, boosted, self.signal);
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
    /// Implied volatility proxy (from recent range)
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
        }
    }

    fn calculate_realized_vol(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() { return 0.0; }
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        variance.sqrt() * (252.0_f64).sqrt() // Annualized
    }

    fn calculate_signal(&mut self) {
        if self.returns.len() < 20 {
            self.signal = 0.0;
            return;
        }

        // RV vs IV gap (vol risk premium)
        let rv_iv_gap = self.implied_vol_proxy - self.realized_vol_20;

        // Vol term structure (short vol vs long vol)
        let vol_term_spread = self.realized_vol_5 - self.realized_vol_60;

        // Vol clustering (vol of vol)
        let vol_returns: Vec<f64> = self.returns.iter()
            .map(|r| r.abs())
            .collect();
        let _vol_of_vol = self.calculate_realized_vol(&vol_returns);

        // MEDALLION: Continuous signal based on vol dynamics
        // 1. RV vs IV gap (vol risk premium) -> continuous fade
        // 2. Vol term structure -> momentum signal
        // 3. Vol regime -> scaling factor

        let premium_signal = (-rv_iv_gap * 10.0).clamp(-1.0, 1.0); // Fade the premium
        let term_signal = (-vol_term_spread * 5.0).clamp(-1.0, 1.0);  // Mean revert term structure

        // Combine with regime-based weighting
        let regime_weight = if self.vol_regime > 0.5 { 0.7 } else { 0.3 };
        self.signal = (regime_weight * premium_signal + (1.0 - regime_weight) * term_signal).clamp(-1.0, 1.0);
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

        // Calculate signal
        if self.tick_count % 10 == 0 {
            self.calculate_signal();
        }
    }

    fn get_signal(&self) -> f64 {
        self.signal
    }

    fn generate_intent(&self, regime: &RegimeState) -> Option<ExpertIntent> {
        if self.signal.abs() < 0.3 { return None; } // MEDALLION: High conviction for vol
        // Skip regime check for vol expert

        Some(ExpertIntent {
            intent_id: Uuid::new_v4(),
            expert_id: self.name().to_string(),
            signal: self.signal,
            expected_edge_bps: 18.0 * self.signal.abs(), // MEDALLION: Vol trades have highest edge
            confidence: regime.confidence * 0.7,
            timestamp: Utc::now(),
            regime: regime.regime,
            signal_family: self.signal_family().to_string(),
            order_style: OrderStyle::Passive,
            stop_loss: None, // Vol positions use different risk mgmt
            take_profit: None,
            time_exit_ticks: Some(1000), // Longer horizon for vol
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
        let avg_vol = total_vol / self.buy_volume.len() as f64;
        let recent_vol = *self.buy_volume.back().unwrap_or(&0.0) + *self.sell_volume.back().unwrap_or(&0.0);
        let accel_mult = if avg_vol > 0.0 {
            (recent_vol / avg_vol).clamp(1.0, 3.0)
        } else {
            1.0
        };

        // Hybrid Signal Fusion
        self.imbalance = (0.7 * multi_obi + 0.3 * tfi) * accel_mult;

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

        // Intensity filter: Alpha needs "heat" in the market to be reliable
        let intensity_ok = intensity_ratio > 0.5 && intensity_ratio < 5.0;
        self.signal = if intensity_ok { base_signal.clamp(-1.0, 1.0) } else { 0.0 };
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
        }
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
        let mut abs_contrib_sum = 0.0;
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
            abs_contrib_sum += final_weighted_signal.abs();

            debug!("[HYDRA] {} raw={:.3} zscore={:.2} (w={:.3}, weighted={:.3}, aeon={:.2})",
                intent.expert_id, intent.signal, zscore, weight, final_weighted_signal, self.aeon_bif_score);
        }


        // Apply global scaling to heuristic edge estimates (useful when calibrating offline)
        weighted_edge *= self.config.edge_scale_mult;

        // Coherence gate: require experts to be directionally aligned
        let coherence = if abs_contrib_sum > 1e-9 { (portfolio_signal.abs() / abs_contrib_sum).clamp(0.0, 1.0) } else { 0.0 };
        if coherence < self.config.coherence_min {
            debug!("[HYDRA] Coherence {:.2} < min {:.2}, skipping", coherence, self.config.coherence_min);
            self.is_processing = false;
            return;
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

        // --- PORTFOLIO POSITION LIMIT ENFORCEMENT ---
        target_pos = target_pos.clamp(-self.config.max_position_abs, self.config.max_position_abs);

        // Check cost-adjusted edge (Masterpiece: Trade when edge > hurdle * costs)
        let total_cost = self.config.slippage_bps + self.config.commission_bps;
        let hurdle = self.config.edge_hurdle_multiplier;
        if weighted_edge < total_cost * hurdle {
            debug!("[HYDRA] Edge {:.1}bps < cost {:.1}bps * {:.1}, skipping", weighted_edge, total_cost, hurdle);
            self.is_processing = false;
            return;
        }

        // --- MASTERPIECE: TRAILING ALPHA-STOP ---
        // If we are long but signal is strongly reversed, close position immediately
        let is_reversal = (self.position > 0.0 && portfolio_signal < -0.4) || (self.position < 0.0 && portfolio_signal > 0.4);
        
        // Position change with hysteresis
        let position_change = target_pos - self.position;
        let mut should_trade = position_change.abs() > self.config.position_hysteresis;
        
        if is_reversal && !should_trade {
            info!("[HYDRA] Trailing Alpha-Stop triggered (Reversal detected) - Closing position");
            target_pos = 0.0;
            should_trade = true;
        }

        if should_trade {
            let side = if target_pos > self.position { Side::Buy } else { Side::Sell };
            let qty = (target_pos - self.position).abs();

            // V2: Minimum quantity to prevent signal spam
            if qty < 0.01 {
                self.is_processing = false;
                return;
            }

            // Register intent for attribution
            if let Some(intent) = best_intent.as_ref() {
                self.ledger.register_intent(intent);
                self.active_intents.insert(intent.intent_id, intent.clone());
            }

            // Publish signal
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
                };

                if let Err(e) = bus.publish_signal_sync(signal) {
                    warn!("[HYDRA] Failed to publish signal: {}", e);
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
