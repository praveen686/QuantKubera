//! # Real-Time Trading Metrics Engine
//!
//! ## P.1016 - IEEE Standard
//! **Module**: METRICS-v2.0
//! **Date**: 2024-12-19
//! **Compliance**: ISO/IEC/IEEE 12207:2017
//!
//! ## Description
//! Provides comprehensive, real-time calculation of trading performance metrics
//! for paper trading and backtesting. Designed for streaming updates with
//! constant-time metric access and periodic recalculation.
//!
//! ## Architecture
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                    TRADING METRICS ENGINE                                │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
//! │  │  Fill Stream    │  │  Price Stream   │  │  Time Stream    │          │
//! │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘          │
//! │           │                    │                    │                    │
//! │           └────────────────────┴────────────────────┘                    │
//! │                                │                                         │
//! │                       ┌────────▼────────┐                                │
//! │                       │  METRICS CORE   │                                │
//! │                       │ (Rolling State) │                                │
//! │                       └────────┬────────┘                                │
//! │                                │                                         │
//! │  ┌─────────────────────────────┼─────────────────────────────┐          │
//! │  ▼                             ▼                             ▼          │
//! │ ┌───────────────┐  ┌───────────────────┐  ┌──────────────────┐         │
//! │ │  Prometheus   │  │    WebSocket      │  │  Strategy Feed   │         │
//! │ │   Exporter    │  │     Broadcast     │  │  (Hydra Loop)    │         │
//! │ └───────────────┘  └───────────────────┘  └──────────────────┘         │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::VecDeque;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use tracing::{info, debug};

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Configuration for the trading metrics engine
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Risk-free rate for Sharpe calculation (annualized)
    pub risk_free_rate: f64,
    /// Window size for rolling calculations (in periods)
    pub rolling_window: usize,
    /// Initial capital for return calculations
    pub initial_capital: f64,
    /// Minimum trades before metrics are valid
    pub min_trades_for_metrics: usize,
    /// Trading days per year for annualization
    pub trading_days_per_year: f64,
    /// Confidence level for VaR calculation (e.g., 0.95 for 95%)
    pub var_confidence: f64,
    /// Sampling interval for returns calculation (seconds)
    pub sampling_interval_seconds: f64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            risk_free_rate: 0.05, // 5% annual
            rolling_window: 252,  // 1 year of daily data
            initial_capital: 100_000.0,
            min_trades_for_metrics: 10,
            trading_days_per_year: 252.0,
            var_confidence: 0.95,
            sampling_interval_seconds: 10.0, // 10-second samples for better Sharpe accuracy
        }
    }
}

// ============================================================================
// TRADE RECORD
// ============================================================================

/// Record of a completed trade for metrics calculation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// Timestamp of trade completion
    pub timestamp: DateTime<Utc>,
    /// Symbol traded
    pub symbol: String,
    /// Trade direction (true = long, false = short)
    pub is_long: bool,
    /// Entry price
    pub entry_price: f64,
    /// Exit price (if closed)
    pub exit_price: Option<f64>,
    /// Quantity
    pub quantity: f64,
    /// Realized PnL (net of costs)
    pub pnl: f64,
    /// Commission paid
    pub commission: f64,
    /// Slippage experienced (bps)
    pub slippage_bps: f64,
    /// Duration held (seconds)
    pub duration_secs: u64,
    /// Strategy/expert that generated the trade
    pub source: String,
}

// ============================================================================
// SNAPSHOT - POINT-IN-TIME METRICS
// ============================================================================

/// Point-in-time snapshot of all trading metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Timestamp of snapshot
    pub timestamp: DateTime<Utc>,

    // === RETURN METRICS ===
    /// Total realized PnL
    pub total_pnl: f64,
    /// Total return percentage
    pub total_return_pct: f64,
    /// Annualized return
    pub annualized_return: f64,

    // === RISK METRICS ===
    /// Sharpe ratio (annualized)
    pub sharpe_ratio: f64,
    /// Sortino ratio (annualized)
    pub sortino_ratio: f64,
    /// Calmar ratio (return / max drawdown)
    pub calmar_ratio: f64,
    /// Maximum drawdown (percentage)
    pub max_drawdown_pct: f64,
    /// Current drawdown (percentage)
    pub current_drawdown_pct: f64,
    /// Value at Risk (95% confidence)
    pub var_95: f64,
    /// Expected Shortfall / CVaR
    pub cvar_95: f64,
    /// Volatility (annualized)
    pub volatility: f64,
    /// Downside deviation
    pub downside_deviation: f64,

    // === TRADE METRICS ===
    /// Total number of trades
    pub total_trades: u64,
    /// Winning trades
    pub winning_trades: u64,
    /// Losing trades
    pub losing_trades: u64,
    /// Win rate (0.0 to 1.0)
    pub win_rate: f64,
    /// Profit factor (gross profit / gross loss)
    pub profit_factor: f64,
    /// Average winning trade
    pub avg_win: f64,
    /// Average losing trade
    pub avg_loss: f64,
    /// Expectancy (expected PnL per trade)
    pub expectancy: f64,
    /// Largest winning trade
    pub largest_win: f64,
    /// Largest losing trade
    pub largest_loss: f64,
    /// Average trade duration (seconds)
    pub avg_trade_duration_secs: f64,

    // === EFFICIENCY METRICS ===
    /// Recovery factor (total return / max drawdown)
    pub recovery_factor: f64,
    /// Payoff ratio (avg win / avg loss)
    pub payoff_ratio: f64,
    /// Kelly criterion optimal fraction
    pub kelly_fraction: f64,
    /// Ulcer index (quadratic drawdown measure)
    pub ulcer_index: f64,

    // === EXECUTION METRICS ===
    /// Average slippage (bps)
    pub avg_slippage_bps: f64,
    /// Total commission paid
    pub total_commission: f64,
    /// Commission as % of PnL
    pub commission_drag_pct: f64,

    // === EQUITY STATE ===
    /// Current equity
    pub equity: f64,
    /// Peak equity
    pub peak_equity: f64,
    /// Number of equity data points
    pub equity_points: usize,
}

impl MetricsSnapshot {
    /// Export to Prometheus gauge metrics
    pub fn export_prometheus(&self) {
        use metrics::gauge;

        // Return metrics
        gauge!("kubera_trading_total_pnl").set(self.total_pnl);
        gauge!("kubera_trading_total_return_pct").set(self.total_return_pct);
        gauge!("kubera_trading_annualized_return").set(self.annualized_return);

        // Risk metrics
        gauge!("kubera_trading_sharpe_ratio").set(self.sharpe_ratio);
        gauge!("kubera_trading_sortino_ratio").set(self.sortino_ratio);
        gauge!("kubera_trading_calmar_ratio").set(self.calmar_ratio);
        gauge!("kubera_trading_max_drawdown_pct").set(self.max_drawdown_pct);
        gauge!("kubera_trading_current_drawdown_pct").set(self.current_drawdown_pct);
        gauge!("kubera_trading_var_95").set(self.var_95);
        gauge!("kubera_trading_volatility").set(self.volatility);

        // Trade metrics
        gauge!("kubera_trading_total_trades").set(self.total_trades as f64);
        gauge!("kubera_trading_win_rate").set(self.win_rate);
        gauge!("kubera_trading_profit_factor").set(self.profit_factor);
        gauge!("kubera_trading_expectancy").set(self.expectancy);
        gauge!("kubera_trading_kelly_fraction").set(self.kelly_fraction);

        // Execution metrics
        gauge!("kubera_trading_avg_slippage_bps").set(self.avg_slippage_bps);
        gauge!("kubera_trading_total_commission").set(self.total_commission);

        // Equity state
        gauge!("kubera_trading_equity").set(self.equity);
        gauge!("kubera_trading_peak_equity").set(self.peak_equity);
    }

    /// Format as a compact single-line summary
    pub fn summary_line(&self) -> String {
        format!(
            "PnL: {:.2} | Sharpe: {:.2} | DD: {:.1}% | Win: {:.1}% | PF: {:.2} | Trades: {}",
            self.total_pnl,
            self.sharpe_ratio,
            self.max_drawdown_pct,
            self.win_rate * 100.0,
            self.profit_factor,
            self.total_trades
        )
    }

    /// Generate a detailed report
    pub fn detailed_report(&self) -> String {
        format!(
r#"
═══════════════════════════════════════════════════════════════
                    TRADING METRICS REPORT
═══════════════════════════════════════════════════════════════
Timestamp: {}

─── RETURN METRICS ───────────────────────────────────────────
  Total PnL:          {:>12.2}
  Total Return:       {:>12.2}%
  Annualized Return:  {:>12.2}%

─── RISK METRICS ─────────────────────────────────────────────
  Sharpe Ratio:       {:>12.2}
  Sortino Ratio:      {:>12.2}
  Calmar Ratio:       {:>12.2}
  Max Drawdown:       {:>12.2}%
  Current Drawdown:   {:>12.2}%
  VaR (95%):          {:>12.2}
  Volatility (Ann.):  {:>12.2}%

─── TRADE METRICS ────────────────────────────────────────────
  Total Trades:       {:>12}
  Win Rate:           {:>12.2}%
  Profit Factor:      {:>12.2}
  Expectancy:         {:>12.2}
  Avg Win:            {:>12.2}
  Avg Loss:           {:>12.2}
  Largest Win:        {:>12.2}
  Largest Loss:       {:>12.2}
  Payoff Ratio:       {:>12.2}

─── EFFICIENCY METRICS ───────────────────────────────────────
  Recovery Factor:    {:>12.2}
  Kelly Fraction:     {:>12.2}%
  Ulcer Index:        {:>12.4}

─── EXECUTION METRICS ────────────────────────────────────────
  Avg Slippage:       {:>12.2} bps
  Total Commission:   {:>12.2}
  Commission Drag:    {:>12.2}%

─── EQUITY STATE ─────────────────────────────────────────────
  Current Equity:     {:>12.2}
  Peak Equity:        {:>12.2}
  Data Points:        {:>12}
═══════════════════════════════════════════════════════════════
"#,
            self.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            self.total_pnl,
            self.total_return_pct,
            self.annualized_return,
            self.sharpe_ratio,
            self.sortino_ratio,
            self.calmar_ratio,
            self.max_drawdown_pct,
            self.current_drawdown_pct,
            self.var_95,
            self.volatility * 100.0,
            self.total_trades,
            self.win_rate * 100.0,
            self.profit_factor,
            self.expectancy,
            self.avg_win,
            self.avg_loss,
            self.largest_win,
            self.largest_loss,
            self.payoff_ratio,
            self.recovery_factor,
            self.kelly_fraction * 100.0,
            self.ulcer_index,
            self.avg_slippage_bps,
            self.total_commission,
            self.commission_drag_pct,
            self.equity,
            self.peak_equity,
            self.equity_points
        )
    }
}

// ============================================================================
// TRADING METRICS ENGINE
// ============================================================================

/// Core metrics engine for real-time performance tracking
pub struct TradingMetrics {
    /// Configuration
    config: MetricsConfig,

    /// All recorded trades
    trades: Vec<TradeRecord>,

    /// Equity curve (timestamp, equity)
    equity_curve: Vec<(DateTime<Utc>, f64)>,

    /// Rolling returns for Sharpe/Sortino calculation
    returns: VecDeque<f64>,

    /// Current equity
    current_equity: f64,

    /// Peak equity
    peak_equity: f64,
    /// Last sampling timestamp for returns
    last_sample_time: DateTime<Utc>,
    /// Last sampled equity
    last_sample_equity: f64,
    /// Session start time
    session_start: DateTime<Utc>,

    /// Cached snapshot (updated periodically)
    cached_snapshot: MetricsSnapshot,

    /// Ticks since last recalculation
    ticks_since_recalc: u64,

    /// Recalculation interval (ticks)
    recalc_interval: u64,

    // === Running aggregates for O(1) updates ===
    gross_profit: f64,
    gross_loss: f64,
    total_slippage_bps: f64,
    total_commission: f64,
    total_duration_secs: u64,
    largest_win: f64,
    largest_loss: f64,

    /// Sum of squared drawdowns for Ulcer Index
    sum_squared_drawdowns: f64,
    drawdown_count: u64,
}

impl TradingMetrics {
    /// Create a new trading metrics engine
    pub fn new(config: MetricsConfig) -> Self {
        let now = Utc::now();
        let initial_equity = config.initial_capital;
        Self {
            config,
            trades: Vec::with_capacity(10000),
            equity_curve: Vec::with_capacity(100000),
            returns: VecDeque::with_capacity(1000),
            current_equity: initial_equity,
            peak_equity: initial_equity,
            cached_snapshot: MetricsSnapshot {
                timestamp: now,
                equity: initial_equity,
                peak_equity: initial_equity,
                ..Default::default()
            },
            last_sample_time: now,
            last_sample_equity: initial_equity,
            session_start: now,
            ticks_since_recalc: 0,
            recalc_interval: 100, // Recalculate every 100 ticks
            gross_profit: 0.0,
            gross_loss: 0.0,
            total_slippage_bps: 0.0,
            total_commission: 0.0,
            total_duration_secs: 0,
            largest_win: 0.0,
            largest_loss: 0.0,
            sum_squared_drawdowns: 0.0,
            drawdown_count: 0,
        }
    }

    /// Create with default configuration
    pub fn default_config() -> Self {
        Self::new(MetricsConfig::default())
    }

    /// Record a completed trade
    pub fn record_trade(&mut self, trade: TradeRecord) {
        // Update running aggregates
        if trade.pnl > 0.0 {
            self.gross_profit += trade.pnl;
            if trade.pnl > self.largest_win {
                self.largest_win = trade.pnl;
            }
        } else if trade.pnl < 0.0 {
            self.gross_loss += trade.pnl.abs();
            if trade.pnl.abs() > self.largest_loss {
                self.largest_loss = trade.pnl.abs();
            }
        }

        self.total_slippage_bps += trade.slippage_bps;
        self.total_commission += trade.commission;
        self.total_duration_secs += trade.duration_secs;

        self.trades.push(trade);

        debug!("[METRICS] Trade recorded: total={}, gross_profit={:.2}, gross_loss={:.2}",
            self.trades.len(), self.gross_profit, self.gross_loss);
    }

    /// Update equity with a new mark-to-market value
    pub fn update_equity(&mut self, equity: f64) {
        self.update_equity_with_timestamp(equity, Utc::now());
    }

    /// Update equity with a specific timestamp (for backtesting with market time)
    pub fn update_equity_with_timestamp(&mut self, equity: f64, timestamp: DateTime<Utc>) {
        self.current_equity = equity;

        // Track peak for drawdown
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }

        // --- PERIODIC SAMPLING FOR RETURNS ---
        let elapsed = timestamp.signed_duration_since(self.last_sample_time).num_milliseconds() as f64 / 1000.0;

        // If timestamp is in the past (backtest mode), reset sample time to sync with market time
        if elapsed < 0.0 {
            self.last_sample_time = timestamp;
            self.last_sample_equity = equity;
            return;
        }

        if elapsed >= self.config.sampling_interval_seconds {
            if self.last_sample_equity > 0.0 {
                let sampled_return = (equity - self.last_sample_equity) / self.last_sample_equity;
                self.returns.push_back(sampled_return);

                // Maintain rolling window
                if self.returns.len() > self.config.rolling_window {
                    self.returns.pop_front();
                }
            }
            self.last_sample_time = timestamp;
            self.last_sample_equity = equity;
        }

        // Record equity point
        let now = Utc::now();
        self.equity_curve.push((now, equity));

        // Update drawdown tracking for Ulcer Index
        if self.peak_equity > 0.0 {
            let drawdown_pct = (self.peak_equity - equity) / self.peak_equity * 100.0;
            self.sum_squared_drawdowns += drawdown_pct.powi(2);
            self.drawdown_count += 1;
        }

        // Periodic recalculation
        self.ticks_since_recalc += 1;
        if self.ticks_since_recalc >= self.recalc_interval {
            self.recalculate();
            self.ticks_since_recalc = 0;
        }
    }

    /// Force a full metrics recalculation
    pub fn recalculate(&mut self) {
        let now = Utc::now();
        let trades = &self.trades;
        let num_trades = trades.len() as u64;

        // Trade counts
        let winning_trades = trades.iter().filter(|t| t.pnl > 0.0).count() as u64;
        let losing_trades = trades.iter().filter(|t| t.pnl < 0.0).count() as u64;

        // Win rate
        let win_rate = if num_trades > 0 {
            winning_trades as f64 / num_trades as f64
        } else {
            0.0
        };

        // Profit factor
        let profit_factor = if self.gross_loss > 0.0 {
            self.gross_profit / self.gross_loss
        } else if self.gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Average win/loss
        let avg_win = if winning_trades > 0 {
            self.gross_profit / winning_trades as f64
        } else {
            0.0
        };

        let avg_loss = if losing_trades > 0 {
            self.gross_loss / losing_trades as f64
        } else {
            0.0
        };

        // Expectancy
        let expectancy = if num_trades > 0 {
            (self.gross_profit - self.gross_loss) / num_trades as f64
        } else {
            0.0
        };

        // Payoff ratio
        let payoff_ratio = if avg_loss > 0.0 {
            avg_win / avg_loss
        } else {
            0.0
        };

        // Total PnL and returns
        let total_pnl = self.gross_profit - self.gross_loss;
        let total_return_pct = if self.config.initial_capital > 0.0 {
            total_pnl / self.config.initial_capital * 100.0
        } else {
            0.0
        };

        // Annualized return
        let session_duration = now.signed_duration_since(self.session_start);
        let years = session_duration.num_seconds() as f64 / (365.25 * 24.0 * 3600.0);
        let annualized_return = if years > 0.0 && total_return_pct > -100.0 {
            ((1.0 + total_return_pct / 100.0).powf(1.0 / years) - 1.0) * 100.0
        } else {
            0.0
        };

        // Volatility calculations
        let (volatility, downside_deviation, sharpe, sortino) = self.calculate_volatility_metrics();

        // Drawdown
        let max_drawdown_pct = self.calculate_max_drawdown();
        let current_drawdown_pct = if self.peak_equity > 0.0 {
            (self.peak_equity - self.current_equity) / self.peak_equity * 100.0
        } else {
            0.0
        };

        // VaR and CVaR
        let (var_95, cvar_95) = self.calculate_var_cvar();

        // Calmar ratio
        let calmar_ratio = if max_drawdown_pct > 0.0 {
            annualized_return / max_drawdown_pct
        } else {
            0.0
        };

        // Recovery factor
        let recovery_factor = if max_drawdown_pct > 0.0 {
            total_return_pct / max_drawdown_pct
        } else {
            0.0
        };

        // Kelly fraction
        let kelly_fraction = if payoff_ratio > 0.0 {
            let q = 1.0 - win_rate;
            win_rate - (q / payoff_ratio)
        } else {
            0.0
        };

        // Ulcer index
        let ulcer_index = if self.drawdown_count > 0 {
            (self.sum_squared_drawdowns / self.drawdown_count as f64).sqrt()
        } else {
            0.0
        };

        // Execution metrics
        let avg_slippage_bps = if num_trades > 0 {
            self.total_slippage_bps / num_trades as f64
        } else {
            0.0
        };

        let commission_drag_pct = if total_pnl.abs() > 0.0 {
            self.total_commission / total_pnl.abs() * 100.0
        } else {
            0.0
        };

        let avg_trade_duration = if num_trades > 0 {
            self.total_duration_secs as f64 / num_trades as f64
        } else {
            0.0
        };

        self.cached_snapshot = MetricsSnapshot {
            timestamp: now,
            total_pnl,
            total_return_pct,
            annualized_return,
            sharpe_ratio: sharpe,
            sortino_ratio: sortino,
            calmar_ratio,
            max_drawdown_pct,
            current_drawdown_pct,
            var_95,
            cvar_95,
            volatility,
            downside_deviation,
            total_trades: num_trades,
            winning_trades,
            losing_trades,
            win_rate,
            profit_factor,
            avg_win,
            avg_loss,
            expectancy,
            largest_win: self.largest_win,
            largest_loss: self.largest_loss,
            avg_trade_duration_secs: avg_trade_duration,
            recovery_factor,
            payoff_ratio,
            kelly_fraction: kelly_fraction.max(0.0),
            ulcer_index,
            avg_slippage_bps,
            total_commission: self.total_commission,
            commission_drag_pct,
            equity: self.current_equity,
            peak_equity: self.peak_equity,
            equity_points: self.equity_curve.len(),
        };
    }

    fn calculate_volatility_metrics(&self) -> (f64, f64, f64, f64) {
        if self.returns.len() < 2 {
            return (0.0, 0.0, 0.0, 0.0);
        }

        let returns: Vec<f64> = self.returns.iter().cloned().collect();
        let n = returns.len() as f64;

        // Mean return
        let mean_return = returns.iter().sum::<f64>() / n;

        // Variance and standard deviation
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / (n - 1.0);
        let std_dev = variance.sqrt();

        // Annualized volatility
        // annualization_factor = sqrt(T / t) where T is a year and t is the sampling interval
        let seconds_per_year = self.config.trading_days_per_year * 24.0 * 3600.0;
        let annualization_factor = (seconds_per_year / self.config.sampling_interval_seconds).sqrt();
        let volatility = std_dev * annualization_factor;

        // Downside deviation (only negative returns)
        let target_return = self.config.risk_free_rate / self.config.trading_days_per_year;
        let downside_sq: f64 = returns.iter()
            .map(|r| if *r < target_return { (r - target_return).powi(2) } else { 0.0 })
            .sum();
        let downside_deviation = (downside_sq / n).sqrt() * annualization_factor;

        // Sharpe ratio (daily_rf adjusted to sampling interval)
        let sample_rf = self.config.risk_free_rate * (self.config.sampling_interval_seconds / seconds_per_year);

        // Use trade-based Sharpe if we have enough trades (more stable for backtest)
        let sharpe = if self.trades.len() >= self.config.min_trades_for_metrics {
            self.calculate_trade_sharpe()
        } else if std_dev > 0.0 {
            ((mean_return - sample_rf) / std_dev) * annualization_factor
        } else {
            0.0
        };

        // Sortino ratio
        let sortino = if downside_deviation > 0.0 {
            (mean_return - sample_rf) * annualization_factor / downside_deviation
        } else {
            sharpe
        };

        (volatility, downside_deviation, sharpe, sortino)
    }

    /// Calculate Sharpe ratio from trade PnL (more stable for backtesting)
    fn calculate_trade_sharpe(&self) -> f64 {
        if self.trades.len() < 2 {
            return 0.0;
        }

        let pnls: Vec<f64> = self.trades.iter().map(|t| t.pnl).collect();
        let n = pnls.len() as f64;

        let mean_pnl = pnls.iter().sum::<f64>() / n;
        let variance = pnls.iter()
            .map(|p| (p - mean_pnl).powi(2))
            .sum::<f64>() / (n - 1.0);
        let std_dev = variance.sqrt();

        if std_dev > 0.0 {
            // Trade-based Sharpe: mean / std_dev, annualized assuming ~250 trades/year
            let trades_per_year: f64 = 250.0;
            (mean_pnl / std_dev) * trades_per_year.sqrt()
        } else {
            0.0
        }
    }

    fn calculate_max_drawdown(&self) -> f64 {
        if self.equity_curve.len() < 2 {
            return 0.0;
        }

        let mut peak = self.config.initial_capital;
        let mut max_dd = 0.0;

        for (_, equity) in &self.equity_curve {
            if *equity > peak {
                peak = *equity;
            }
            let dd = (peak - equity) / peak * 100.0;
            if dd > max_dd {
                max_dd = dd;
            }
        }

        max_dd
    }

    fn calculate_var_cvar(&self) -> (f64, f64) {
        if self.returns.len() < 10 {
            return (0.0, 0.0);
        }

        let mut sorted_returns: Vec<f64> = self.returns.iter().cloned().collect();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // VaR at confidence level
        let var_index = ((1.0 - self.config.var_confidence) * sorted_returns.len() as f64) as usize;
        let var = -sorted_returns.get(var_index).unwrap_or(&0.0) * self.current_equity;

        // CVaR (Expected Shortfall) - average of returns below VaR
        let cvar = if var_index > 0 {
            let tail_sum: f64 = sorted_returns[..=var_index].iter().sum();
            -(tail_sum / (var_index + 1) as f64) * self.current_equity
        } else {
            var
        };

        (var, cvar)
    }

    /// Get the latest metrics snapshot
    pub fn snapshot(&self) -> &MetricsSnapshot {
        &self.cached_snapshot
    }

    /// Get a fresh snapshot (forces recalculation)
    pub fn fresh_snapshot(&mut self) -> MetricsSnapshot {
        self.recalculate();
        self.cached_snapshot.clone()
    }

    /// Export metrics to Prometheus
    pub fn export_prometheus(&self) {
        self.cached_snapshot.export_prometheus();
    }

    /// Get the equity curve
    pub fn equity_curve(&self) -> &[(DateTime<Utc>, f64)] {
        &self.equity_curve
    }

    /// Get all trades
    pub fn trades(&self) -> &[TradeRecord] {
        &self.trades
    }

    /// Log a summary to tracing
    pub fn log_summary(&self) {
        info!("[METRICS] {}", self.cached_snapshot.summary_line());
    }

    /// Reset metrics for a new session
    pub fn reset(&mut self) {
        let initial = self.config.initial_capital;
        self.trades.clear();
        self.equity_curve.clear();
        self.returns.clear();
        self.current_equity = initial;
        self.peak_equity = initial;
        self.session_start = Utc::now();
        self.cached_snapshot = MetricsSnapshot {
            timestamp: Utc::now(),
            equity: initial,
            peak_equity: initial,
            ..Default::default()
        };
        self.gross_profit = 0.0;
        self.gross_loss = 0.0;
        self.total_slippage_bps = 0.0;
        self.total_commission = 0.0;
        self.total_duration_secs = 0;
        self.largest_win = 0.0;
        self.largest_loss = 0.0;
        self.sum_squared_drawdowns = 0.0;
        self.drawdown_count = 0;
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        let metrics = TradingMetrics::default_config();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_trades, 0);
        assert_eq!(snapshot.equity, 100_000.0);
    }

    #[test]
    fn test_trade_recording() {
        let mut metrics = TradingMetrics::default_config();

        metrics.record_trade(TradeRecord {
            timestamp: Utc::now(),
            symbol: "TEST".to_string(),
            is_long: true,
            entry_price: 100.0,
            exit_price: Some(110.0),
            quantity: 10.0,
            pnl: 100.0,
            commission: 1.0,
            slippage_bps: 2.0,
            duration_secs: 3600,
            source: "test".to_string(),
        });

        metrics.recalculate();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.total_trades, 1);
        assert_eq!(snapshot.winning_trades, 1);
        assert!(snapshot.win_rate > 0.99);
    }

    #[test]
    fn test_equity_update_and_drawdown() {
        let mut metrics = TradingMetrics::default_config();

        metrics.update_equity(100_000.0);
        metrics.update_equity(110_000.0);  // Peak
        metrics.update_equity(99_000.0);   // Drawdown

        metrics.recalculate();
        let snapshot = metrics.snapshot();

        assert!(snapshot.max_drawdown_pct > 9.0);  // 10% drawdown from peak
    }

    #[test]
    fn test_profit_factor() {
        let mut metrics = TradingMetrics::default_config();

        // 3 winning trades of $100 each
        for _ in 0..3 {
            metrics.record_trade(TradeRecord {
                timestamp: Utc::now(),
                symbol: "TEST".to_string(),
                is_long: true,
                entry_price: 100.0,
                exit_price: Some(110.0),
                quantity: 10.0,
                pnl: 100.0,
                commission: 0.0,
                slippage_bps: 0.0,
                duration_secs: 3600,
                source: "test".to_string(),
            });
        }

        // 1 losing trade of $150
        metrics.record_trade(TradeRecord {
            timestamp: Utc::now(),
            symbol: "TEST".to_string(),
            is_long: true,
            entry_price: 100.0,
            exit_price: Some(85.0),
            quantity: 10.0,
            pnl: -150.0,
            commission: 0.0,
            slippage_bps: 0.0,
            duration_secs: 3600,
            source: "test".to_string(),
        });

        metrics.recalculate();
        let snapshot = metrics.snapshot();

        // Profit factor = 300 / 150 = 2.0
        assert!((snapshot.profit_factor - 2.0).abs() < 0.01);
        // Win rate = 3/4 = 75%
        assert!((snapshot.win_rate - 0.75).abs() < 0.01);
    }
}
