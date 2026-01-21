//! # Options Backtesting Engine Module
//!
//! Simulation environment for evaluating FnO strategies against historical data.
//!
//! ## Description
//! Implements a high-fidelity options backtester with:
//! - Mark-to-Market (MTM) accounting
//! - Realistic slippage and commission modeling
//! - Multi-leg strategy execution
//! - Performance metrics (Sharpe, Profit Factor, Drawdown)
//!
//! ## Execution Logic
//! Matches strategy intents against historical option OHLCV bars, accounting for
//! bid-ask spreads via slippage parameters.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use crate::contract::{OptionContract, OptionType};
use crate::pricing::implied_volatility;
use crate::strategy::{OptionsStrategy, StrategyType, StrategyLeg};
use crate::execution::{PositionManager, Position};
use chrono::{NaiveDate, DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Historical OHLCV bar for an individual option contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionBar {
    pub timestamp: DateTime<Utc>,
    pub underlying_price: f64,
    pub strike: f64,
    pub expiry: NaiveDate,
    pub option_type: OptionType,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
    pub oi: u64,
}

impl OptionBar {
    /// Extracts implied volatility from the bar's closing price.
    ///
    /// # Parameters
    /// * `rate` - Annualized risk-free rate.
    pub fn implied_vol(&self, rate: f64) -> Option<f64> {
        let time = (self.expiry - self.timestamp.date_naive()).num_days() as f64 / 365.0;
        if time <= 0.0 {
            return None;
        }
        
        let is_call = matches!(self.option_type, OptionType::Call);
        implied_volatility(self.close, self.underlying_price, self.strike, time, rate, is_call)
    }
}

/// Operational parameters for the backtesting engine.
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub initial_capital: f64,
    /// Number of shares per contract (e.g., 50 for Nifty).
    pub lot_size: u32,
    pub commission_per_lot: f64,
    /// Price impact simulation in basis points.
    pub slippage_bps: f64,
    pub risk_free_rate: f64,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            start_date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            initial_capital: 1_000_000.0,
            lot_size: 50,
            commission_per_lot: 20.0,
            slippage_bps: 5.0,
            risk_free_rate: 0.065,
        }
    }
}

/// Comprehensive performance report generated after a run.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BacktestResult {
    pub total_trades: u32,
    pub winning_trades: u32,
    pub losing_trades: u32,
    pub gross_pnl: f64,
    pub net_pnl: f64,
    pub total_commission: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub avg_trade_pnl: f64,
    /// Cumulative equity over time.
    pub equity_curve: Vec<(DateTime<Utc>, f64)>,
}

/// Discrete record of a trade execution for auditing.
#[derive(Debug, Clone, Serialize)]
pub struct TradeRecord {
    pub timestamp: DateTime<Utc>,
    pub strategy_name: String,
    pub symbol: String,
    pub side: String,
    pub quantity: i32,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub pnl: f64,
    pub commission: f64,
}

/// Core engine for executing FnO backtests.
pub struct FnOBacktester {
    config: BacktestConfig,
    position_manager: PositionManager,
    trades: Vec<TradeRecord>,
    equity_curve: Vec<(DateTime<Utc>, f64)>,
    current_equity: f64,
    peak_equity: f64,
    max_drawdown: f64,
}

impl FnOBacktester {
    /// Initializes the backtester with capital and limits.
    pub fn new(config: BacktestConfig) -> Self {
        let initial_capital = config.initial_capital;
        Self {
            config,
            position_manager: PositionManager::new(),
            trades: Vec::new(),
            equity_curve: Vec::new(),
            current_equity: initial_capital,
            peak_equity: initial_capital,
            max_drawdown: 0.0,
        }
    }

    /// Ingests a strategy intent and executes it against current market prices.
    pub fn execute_strategy(
        &mut self,
        timestamp: DateTime<Utc>,
        strategy: &OptionsStrategy,
        prices: &HashMap<String, f64>,
    ) {
        for leg in &strategy.legs {
            let price = prices.get(&leg.contract.tradingsymbol).copied().unwrap_or(leg.entry_price);
            let slippage = price * (self.config.slippage_bps / 10000.0);
            
            let fill_price = if leg.quantity > 0 {
                price + slippage 
            } else {
                price - slippage 
            };
            
            let total_quantity = leg.quantity * self.config.lot_size as i32;
            let commission = self.config.commission_per_lot * leg.quantity.abs() as f64;
            
            self.position_manager.on_fill(
                &leg.contract.tradingsymbol,
                total_quantity,
                fill_price,
                "NRML",
            );
            
            self.current_equity -= commission;
            
            self.trades.push(TradeRecord {
                timestamp,
                strategy_name: strategy.name.clone(),
                symbol: leg.contract.tradingsymbol.clone(),
                side: if leg.quantity > 0 { "BUY".to_string() } else { "SELL".to_string() },
                quantity: total_quantity,
                entry_price: fill_price,
                exit_price: None,
                pnl: 0.0,
                commission,
            });
        }
    }

    /// Periodically updates account equity based on unrealized PnL.
    pub fn mark_to_market(&mut self, timestamp: DateTime<Utc>, prices: &HashMap<String, f64>) {
        self.position_manager.update_prices(prices);
        let mtm_pnl = self.position_manager.total_pnl();
        
        self.current_equity = self.config.initial_capital + mtm_pnl;
        self.equity_curve.push((timestamp, self.current_equity));
        
        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }
        let drawdown = (self.peak_equity - self.current_equity) / self.peak_equity;
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
    }

    /// Liquidates all open positions at current market prices.
    pub fn close_all_positions(&mut self, timestamp: DateTime<Utc>, prices: &HashMap<String, f64>) {
        let positions: Vec<Position> = self.position_manager.all_positions()
            .iter()
            .map(|p| (*p).clone())
            .collect();
        
        for pos in positions {
            let price = prices.get(&pos.tradingsymbol).copied().unwrap_or(pos.last_price);
            let slippage = price * (self.config.slippage_bps / 10000.0);
            
            let fill_price = if pos.quantity > 0 {
                price - slippage 
            } else {
                price + slippage 
            };
            
            let pnl = (fill_price - pos.average_price) * pos.quantity as f64;
            let commission = self.config.commission_per_lot * (pos.quantity.abs() as f64 / self.config.lot_size as f64);
            
            self.current_equity += pnl - commission;
            
            self.position_manager.on_fill(
                &pos.tradingsymbol,
                -pos.quantity,
                fill_price,
                "NRML",
            );
            
            self.trades.push(TradeRecord {
                timestamp,
                strategy_name: "CLOSE".to_string(),
                symbol: pos.tradingsymbol.clone(),
                side: if pos.quantity > 0 { "SELL".to_string() } else { "BUY".to_string() },
                quantity: -pos.quantity,
                entry_price: fill_price,
                exit_price: Some(fill_price),
                pnl,
                commission,
            });
        }
    }

    /// Aggregates trades and equity history into a performance summary.
    pub fn finalize(&self) -> BacktestResult {
        let total_trades = self.trades.len() as u32;
        let winning_trades = self.trades.iter().filter(|t| t.pnl > 0.0).count() as u32;
        let losing_trades = self.trades.iter().filter(|t| t.pnl < 0.0).count() as u32;
        let gross_pnl = self.trades.iter().map(|t| t.pnl).sum();
        let total_commission: f64 = self.trades.iter().map(|t| t.commission).sum();
        let net_pnl = gross_pnl - total_commission;
        
        let gross_profit: f64 = self.trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = self.trades.iter().filter(|t| t.pnl < 0.0).map(|t| t.pnl.abs()).sum();
        let profit_factor = if gross_loss > 0.0 { gross_profit / gross_loss } else { 0.0 };
        
        let win_rate = if total_trades > 0 { winning_trades as f64 / total_trades as f64 } else { 0.0 };
        let avg_trade_pnl = if total_trades > 0 { net_pnl / total_trades as f64 } else { 0.0 };
        
        // Sharpe calculation
        let returns: Vec<f64> = self.equity_curve.windows(2)
            .map(|w| (w[1].1 - w[0].1) / w[0].1)
            .collect();
        
        let mean_return = if !returns.is_empty() { returns.iter().sum::<f64>() / returns.len() as f64 } else { 0.0 };
        let std_dev = if returns.len() > 1 {
            let variance = returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / (returns.len() - 1) as f64;
            variance.sqrt()
        } else {
            0.0
        };
        
        let sharpe_ratio = if std_dev > 0.0 { (mean_return * 252.0_f64.sqrt()) / std_dev } else { 0.0 };
        
        BacktestResult {
            total_trades,
            winning_trades,
            losing_trades,
            gross_pnl,
            net_pnl,
            total_commission,
            max_drawdown: self.max_drawdown,
            sharpe_ratio,
            win_rate,
            profit_factor,
            avg_trade_pnl,
            equity_curve: self.equity_curve.clone(),
        }
    }
}

/// Orchestrator for mapping analytical signals to execution intents.
pub struct StrategyRunner {
    /// Strategy closure defining signal-to-intent logic.
    strategy_fn: Box<dyn Fn(f64, f64, NaiveDate) -> Option<OptionsStrategy>>,
}

impl StrategyRunner {
    /// Helper to create a runner for volatility mean-reversion (ATM Straddles).
    pub fn atm_straddle(iv_threshold_low: f64, iv_threshold_high: f64) -> Self {
        Self {
            strategy_fn: Box::new(move |spot, iv, expiry| {
                if iv < iv_threshold_low {
                    Some(Self::build_straddle(spot, expiry, true))
                } else if iv > iv_threshold_high {
                    Some(Self::build_straddle(spot, expiry, false))
                } else {
                    None
                }
            }),
        }
    }

    fn build_straddle(spot: f64, expiry: NaiveDate, is_long: bool) -> OptionsStrategy {
        let atm_strike = (spot / 50.0).round() * 50.0;
        let qty = if is_long { 1 } else { -1 };
        
        OptionsStrategy {
            name: format!("{} Straddle {}", if is_long { "Long" } else { "Short" }, atm_strike),
            strategy_type: StrategyType::Straddle,
            legs: vec![
                StrategyLeg {
                    contract: OptionContract {
                        underlying: "NIFTY".to_string(),
                        expiry,
                        strike: atm_strike,
                        option_type: OptionType::Call,
                        instrument_token: 0,
                        tradingsymbol: format!("NIFTY{}CE", atm_strike as u32),
                        lot_size: 50,
                    },
                    quantity: qty,
                    entry_price: 0.0,
                },
                StrategyLeg {
                    contract: OptionContract {
                        underlying: "NIFTY".to_string(),
                        expiry,
                        strike: atm_strike,
                        option_type: OptionType::Put,
                        instrument_token: 0,
                        tradingsymbol: format!("NIFTY{}PE", atm_strike as u32),
                        lot_size: 50,
                    },
                    quantity: qty,
                    entry_price: 0.0,
                },
            ],
            underlying_price: spot,
        }
    }

    /// Evaluates input data and potentially generates a strategy intent.
    pub fn evaluate(&self, spot: f64, iv: f64, expiry: NaiveDate) -> Option<OptionsStrategy> {
        (self.strategy_fn)(spot, iv, expiry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backtest_config_default() {
        let config = BacktestConfig::default();
        assert_eq!(config.lot_size, 50);
        assert_eq!(config.initial_capital, 1_000_000.0);
    }

    #[test]
    fn test_backtester_creation() {
        let config = BacktestConfig::default();
        let bt = FnOBacktester::new(config);
        assert_eq!(bt.current_equity, 1_000_000.0);
    }

    #[test]
    fn test_strategy_runner_atm_straddle() {
        let runner = StrategyRunner::atm_straddle(0.12, 0.25);
        
        // Low IV should generate long straddle
        let expiry = NaiveDate::from_ymd_opt(2024, 12, 26).unwrap();
        let strategy = runner.evaluate(25800.0, 0.10, expiry);
        assert!(strategy.is_some());
        assert_eq!(strategy.unwrap().legs.len(), 2);
        
        // Normal IV should not generate signal
        let strategy = runner.evaluate(25800.0, 0.18, expiry);
        assert!(strategy.is_none());
    }
}
