//! # Backtesting Execution Module
//!
//! Provides the primary engine for historical strategy simulation.
//!
//! ## Description
//! Implements the `BacktestEngine` which replays Write-Ahead Log (WAL) events 
//! through strategy logic and a `SimulatedExchange`. Responsible for 
//! performance accounting, risk metric calculation, and equity curve generation.
//!
//! ## Performance Requirements
//! - Replay Speed: > 1M events/sec (estimated)
//! - Determinism: 100% reproducible results for identical WAL + Seed
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - MasterPrompt 5.1: Performance Reporting Standards

pub mod optimize;

use kubera_core::{EventBus, Strategy, wal::WalReader};
use kubera_executor::{SimulatedExchange, CommissionModel};
use kubera_models::{MarketPayload, OrderEvent, OrderPayload, Side, OrderType, OrderStatus};
use std::sync::Arc;
use tracing::info;

/// Aggregated performance and risk metrics for a strategy run.
///
/// # Metrics
/// - **Sharpe Ratio**: Annualized risk-adjusted return
/// - **Sortino Ratio**: Downside deviation-adjusted return
/// - **Max Drawdown**: peak-to-trough decline as fraction
/// - **Profit Factor**: gross_profit / ABS(gross_loss)
#[derive(Debug, Default, Clone)]
pub struct BacktestResults {
    /// Total count of closed trading cycles.
    pub total_trades: u64,
    /// Count of trades with PnL > 0.
    pub winning_trades: u64,
    /// Count of trades with PnL <= 0.
    pub losing_trades: u64,
    
    /// Sum of all realized PnL.
    pub total_pnl: f64,
    /// Sum of profits from winning trades.
    pub gross_profit: f64,
    /// Absolute sum of losses from losing trades.
    pub gross_loss: f64,
    
    /// Maximum observed peak-to-trough decline (0.0 to 1.0).
    pub max_drawdown: f64,
    /// Risk-adjusted return metric.
    pub sharpe_ratio: f64,
    /// Downside-risk-adjusted return metric.
    pub sortino_ratio: f64,
    
    /// Efficiency of profit generation.
    pub profit_factor: f64,
    /// Percentage of successful trades (0.0 to 1.0).
    pub win_rate: f64,
    /// Mean dollar PnL per trade.
    pub avg_trade_pnl: f64,
    
    /// Sequence of equity values at each evaluation interval.
    pub equity_curve: Vec<f64>,
    /// Sequence of period-over-period fractional returns.
    pub returns: Vec<f64>,
}

impl BacktestResults {
    /// Emits a structured performance report to the log.
    pub fn report(&self) {
        info!("═══════════════════════════════════════════");
        info!("           BACKTEST RESULTS                ");
        info!("═══════════════════════════════════════════");
        info!("Total Trades:   {}", self.total_trades);
        info!("Win Rate:       {:.1}%", self.win_rate * 100.0);
        info!("Winning Trades: {}", self.winning_trades);
        info!("Losing Trades:  {}", self.losing_trades);
        info!("───────────────────────────────────────────");
        info!("Total PnL:      ${:.2}", self.total_pnl);
        info!("Gross Profit:   ${:.2}", self.gross_profit);
        info!("Gross Loss:     ${:.2}", self.gross_loss);
        info!("Avg Trade PnL:  ${:.2}", self.avg_trade_pnl);
        info!("Profit Factor:  {:.2}", self.profit_factor);
        info!("───────────────────────────────────────────");
        info!("Max Drawdown:   {:.2}%", self.max_drawdown * 100.0);
        info!("Sharpe Ratio:   {:.2}", self.sharpe_ratio);
        info!("Sortino Ratio:  {:.2}", self.sortino_ratio);
        info!("═══════════════════════════════════════════");
    }

    /// Logs the results to an MLflow tracking server.
    pub fn log_to_mlflow(&self, tracker_url: &str, experiment_name: &str) -> anyhow::Result<()> {
        let client = kubera_mlflow::MlflowClient::new(tracker_url, experiment_name)?;
        let run = client.start_run()?;
        
        run.log_param("total_trades", &self.total_trades.to_string())?;
        run.log_param("win_rate", &self.win_rate.to_string())?;
        
        run.log_metric("total_pnl", self.total_pnl, 0)?;
        run.log_metric("sharpe_ratio", self.sharpe_ratio, 0)?;
        run.log_metric("max_drawdown", self.max_drawdown, 0)?;
        
        run.finish()?;
        info!("Successfully logged results to MLflow at {}", tracker_url);
        Ok(())
    }
}

/// Core simulation engine for historical data replay.
///
/// # Architecture
/// - **Event Replay**: Sequential processing of WAL records.
/// - **Simulated Exchange**: Latency and slippage modeling for fills.
/// - **Portfolio Tracking**: Real-time evaluation of realized and unrealized PnL.
pub struct BacktestEngine {
    bus: Arc<EventBus>,
    exchange: SimulatedExchange,
    wal_path: String,
    results: BacktestResults,
    portfolio: kubera_core::Portfolio,
    equity: f64,
    peak_equity: f64,
    last_price: f64,
    initial_equity: f64,
    _seed: Option<u64>,
}

impl BacktestEngine {
    /// Constructs a new backtesting instance.
    ///
    /// # Parameters
    /// * `wal_path` - Location of historical event log.
    /// * `initial_equity` - Opening balance for the simulation.
    /// * `seed` - Random seed for deterministic matching.
    /// * `slippage_bps` - Constant slippage model in basis points.
    pub fn new(wal_path: &str, initial_equity: f64, seed: Option<u64>, slippage_bps: f64) -> Self {
        let bus = EventBus::new(10000);
        let exchange = SimulatedExchange::new(bus.clone(), slippage_bps, CommissionModel::None, seed);
        
        Self {
            bus,
            exchange,
            wal_path: wal_path.to_string(),
            results: BacktestResults::default(),
            portfolio: kubera_core::Portfolio::new(),
            equity: initial_equity,
            initial_equity,
            peak_equity: initial_equity,
            last_price: 0.0,
            _seed: seed,
        }
    }
    
    /// Executes the simulation for a given strategy over a specific range of events.
    pub async fn run_range(
        &mut self,
        mut strategy: Box<dyn Strategy>,
        start_idx: usize,
        end_idx: usize,
    ) -> anyhow::Result<BacktestResults> {
        info!("Starting Ranged Backtest: {} ({} to {})", strategy.name(), start_idx, end_idx);
        
        strategy.on_start(self.bus.clone());
        let mut signal_rx = self.bus.subscribe_signal();
        let mut order_rx = self.bus.subscribe_order_update();
        
        let mut wal_reader = WalReader::new(&self.wal_path)?;
        wal_reader.seek_event(start_idx)?;
        
        let mut event_count = 0usize;
        let n_events = end_idx - start_idx;
        
        while event_count < n_events {
            let event = match wal_reader.next_event()? {
                Some(e) => e,
                None => break,
            };
            
            event_count += 1;
            let mut last_tick_price = self.last_price;
            
            match &event.payload {
                MarketPayload::Tick { price, .. } => {
                    last_tick_price = *price;
                    self.last_price = *price;
                    strategy.on_tick(&event);
                    self.exchange.on_market_data(event.clone()).await?;
                }
                MarketPayload::Bar { close, .. } => {
                    last_tick_price = *close;
                    self.last_price = *close;
                    strategy.on_bar(&event);
                }
                _ => {}
            }
            
            while let Ok(signal) = signal_rx.try_recv() {
                let order = OrderEvent {
                    order_id: self.exchange.next_deterministic_id(),
                    intent_id: signal.intent_id,
                    timestamp: signal.timestamp,
                    symbol: signal.symbol.clone(),
                    side: signal.side.clone(),
                    payload: OrderPayload::New {
                        symbol: signal.symbol.clone(),
                        side: signal.side.clone(),
                        price: None,
                        quantity: signal.quantity,
                        order_type: OrderType::Market,
                    },
                };
                self.exchange.handle_order(order).await?;
            }

            while let Ok(order_event) = order_rx.try_recv() {
                if let OrderPayload::Update { status, filled_quantity, avg_price, .. } = order_event.payload {
                    if status == OrderStatus::Filled {
                        self.process_fill(&order_event.symbol, order_event.side, filled_quantity, avg_price);
                    }
                }
            }
            
            self.update_equity(last_tick_price);
        }
        
        strategy.on_stop();
        
        // Final drain
        while let Ok(signal) = signal_rx.try_recv() {
            let order = OrderEvent {
                order_id: self.exchange.next_deterministic_id(),
                intent_id: signal.intent_id,
                timestamp: signal.timestamp,
                symbol: signal.symbol.clone(),
                side: signal.side.clone(),
                payload: OrderPayload::New {
                    symbol: signal.symbol.clone(),
                    side: signal.side.clone(),
                    price: None,
                    quantity: signal.quantity,
                    order_type: OrderType::Market,
                },
            };
            self.exchange.handle_order(order).await?;
        }

        while let Ok(order_event) = order_rx.try_recv() {
            if let OrderPayload::Update { status, filled_quantity, avg_price, .. } = order_event.payload {
                if status == OrderStatus::Filled {
                    self.process_fill(&order_event.symbol, order_event.side, filled_quantity, avg_price);
                }
            }
        }
        
        info!("Replayed {} events", event_count);
        self.calculate_metrics();
        
        Ok(std::mem::take(&mut self.results))
    }

    /// Executes the simulation for a given strategy.
    ///
    /// # Parameters
    /// * `strategy` - Boxed implementation of the Strategy trait.
    ///
    /// # Returns
    /// Population of `BacktestResults` metrics upon completion.
    pub async fn run(&mut self, mut strategy: Box<dyn Strategy>) -> anyhow::Result<BacktestResults> {
        info!("Starting Backtest: {}", strategy.name());
        info!("WAL Path: {}", self.wal_path);
        
        strategy.on_start(self.bus.clone());
        
        // Subscribe to signal and order update channels
        let mut signal_rx = self.bus.subscribe_signal();
        let mut order_rx = self.bus.subscribe_order_update();
        
        let mut wal_reader = WalReader::new(&self.wal_path)?;
        let mut event_count = 0u64;
        
        while let Some(event) = wal_reader.next_event()? {
            event_count += 1;
            let mut last_tick_price = self.last_price;
            
            // Feed to strategy
            match &event.payload {
                MarketPayload::Tick { price, .. } => {
                    last_tick_price = *price;
                    self.last_price = *price;
                    strategy.on_tick(&event);
                    // Feed to exchange for order matching
                    self.exchange.on_market_data(event.clone()).await?;
                }
                MarketPayload::Bar { close, .. } => {
                    last_tick_price = *close;
                    self.last_price = *close;
                    strategy.on_bar(&event);
                }
                _ => {}
            }
            
            // Process any signals generated
            while let Ok(signal) = signal_rx.try_recv() {
                let order = OrderEvent {
                    order_id: self.exchange.next_deterministic_id(),
                    intent_id: signal.intent_id,
                    timestamp: signal.timestamp,
                    symbol: signal.symbol.clone(),
                    side: signal.side.clone(),
                    payload: OrderPayload::New {
                        symbol: signal.symbol.clone(),
                        side: signal.side.clone(),
                        price: None,
                        quantity: signal.quantity,
                        order_type: OrderType::Market,
                    },
                };
                self.exchange.handle_order(order).await?;
            }

            // Drain order events from exchange
            while let Ok(order_event) = order_rx.try_recv() {
                if let OrderPayload::Update { status, filled_quantity, avg_price, .. } = order_event.payload {
                    if status == OrderStatus::Filled {
                        self.process_fill(&order_event.symbol, order_event.side, filled_quantity, avg_price);
                    }
                }
            }
            
            // Update equity AFTER fills
            self.update_equity(last_tick_price);
        }
        
        strategy.on_stop();
        
        // Drain remaining signals/orders
        while let Ok(signal) = signal_rx.try_recv() {
            let order = OrderEvent {
                order_id: self.exchange.next_deterministic_id(),
                intent_id: signal.intent_id,
                timestamp: signal.timestamp,
                symbol: signal.symbol.clone(),
                side: signal.side.clone(),
                payload: OrderPayload::New {
                    symbol: signal.symbol.clone(),
                    side: signal.side.clone(),
                    price: None,
                    quantity: signal.quantity,
                    order_type: OrderType::Market,
                },
            };
            self.exchange.handle_order(order).await?;
        }

        while let Ok(order_event) = order_rx.try_recv() {
            if let OrderPayload::Update { status, filled_quantity, avg_price, .. } = order_event.payload {
                if status == OrderStatus::Filled {
                    self.process_fill(&order_event.symbol, order_event.side, filled_quantity, avg_price);
                }
            }
        }
        
        info!("Replayed {} events", event_count);
        self.calculate_metrics();
        
        Ok(std::mem::take(&mut self.results))
    }
    
    /// Updates the current equity based on market price and portfolio.
    ///
    /// # Parameters
    /// * `current_price` - The latest market price for marking to market.
    fn update_equity(&mut self, current_price: f64) {
        self.last_price = current_price;
        self.portfolio.mark_to_market("BTCUSDT", current_price);
        
        let realized = self.portfolio.total_realized_pnl();
        let unrealized = self.portfolio.total_unrealized_pnl();
        self.equity = self.initial_equity + realized + unrealized;
        
        self.results.equity_curve.push(self.equity);
        self.results.total_pnl = realized;
        
        if self.equity > self.peak_equity {
            self.peak_equity = self.equity;
        }
        let drawdown = (self.peak_equity - self.equity) / self.peak_equity;
        if drawdown > self.results.max_drawdown {
            self.results.max_drawdown = drawdown;
        }
    }
    
    /// Processes a filled order, updating portfolio and trade statistics.
    ///
    /// # Parameters
    /// * `symbol` - The trading pair symbol.
    /// * `side` - The side of the trade (Buy/Sell).
    /// * `quantity` - The filled quantity.
    /// * `price` - The average fill price.
    fn process_fill(&mut self, symbol: &str, side: Side, quantity: f64, price: f64) {
        // No commission in backtest for now
        let pnl = self.portfolio.apply_fill(symbol, side, quantity, price, 0.0);
        
        self.results.total_trades += 1;
        
        if pnl > 0.0 {
            self.results.winning_trades += 1;
            self.results.gross_profit += pnl;
        } else if pnl < 0.0 {
            self.results.losing_trades += 1;
            self.results.gross_loss += pnl.abs();
        }
    }
    
    /// Computes final performance metrics based on the equity curve and trade data.
    fn calculate_metrics(&mut self) {
        if self.results.equity_curve.len() < 2 {
            return;
        }
        
        let returns: Vec<f64> = self.results.equity_curve
            .windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        
        self.results.returns = returns.clone();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        
        // Sharpe Ratio
        let variance = if returns.len() > 1 {
            returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / (returns.len() - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();
        
        self.results.sharpe_ratio = if std_dev > 0.0 {
            (mean_return / std_dev) * (252.0_f64).sqrt()
        } else {
            0.0
        };
        
        // Sortino Ratio
        let downside_sum_sq: f64 = returns.iter()
            .map(|&r| if r < 0.0 { r.powi(2) } else { 0.0 })
            .sum();
        
        let downside_dev = (downside_sum_sq / returns.len() as f64).sqrt();
        
        self.results.sortino_ratio = if downside_dev > 0.0 {
            (mean_return / downside_dev) * (252.0_f64).sqrt()
        } else {
            self.results.sharpe_ratio
        };
        
        // Profit Factor
        self.results.profit_factor = if self.results.gross_loss.abs() > 0.0 {
            self.results.gross_profit / self.results.gross_loss.abs()
        } else if self.results.gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        
        // Win Rate
        self.results.win_rate = if self.results.total_trades > 0 {
            self.results.winning_trades as f64 / self.results.total_trades as f64
        } else {
            0.0
        };
        
        // Average Trade PnL
        self.results.avg_trade_pnl = if self.results.total_trades > 0 {
            self.results.total_pnl / self.results.total_trades as f64
        } else {
            0.0
        };
    }
}

// Re-export optimization types
pub use optimize::{
    ParamRange, ParamValue, ParamSet, 
    GridSearchOptimizer, RandomSearchOptimizer, TpeOptimizer,
    WalkForwardConfig, WalkForwardSplitter, WalkForwardResults,
    TrialResult, OptimizationResults,
    StrategyFactory, ParameterSweepRunner,
};
