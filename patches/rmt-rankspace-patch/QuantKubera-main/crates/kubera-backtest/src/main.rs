use kubera_core::{EventBus, Strategy, wal::WalReader};
use kubera_executor::{SimulatedExchange, CommissionModel};
use kubera_models::{MarketPayload, OrderEvent, OrderPayload, Side, OrderType, OrderStatus};
use std::sync::Arc;
use tracing::{info, Level};
use serde::Deserialize;
use std::fs;
// use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct Config {
    backtest: BacktestConfig,
}

#[derive(Debug, Deserialize)]
struct BacktestConfig {
    wal_path: String,
    initial_equity: f64,
    seed: Option<u64>,
    slippage_bps: f64,
}

/// Backtest results with key metrics per MasterPrompt 5.1
#[derive(Debug, Default)]
pub struct BacktestResults {
    // Trade counts
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    
    // PnL metrics
    pub total_pnl: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    
    // Risk metrics
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    
    // Derived metrics
    pub profit_factor: f64,
    pub win_rate: f64,
    pub avg_trade_pnl: f64,
    
    // Equity tracking
    pub equity_curve: Vec<f64>,
    pub returns: Vec<f64>,
}

impl BacktestResults {
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
}

/// Backtest engine that replays WAL through a strategy
pub struct BacktestEngine {
    bus: Arc<EventBus>,
    exchange: SimulatedExchange,
    wal_path: String,
    results: BacktestResults,
    portfolio: kubera_core::Portfolio,
    equity: f64,
    initial_equity: f64,
    peak_equity: f64,
    last_price: f64,
    current_symbol: String,
    _seed: Option<u64>,
}

impl BacktestEngine {
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
            current_symbol: String::new(),
            _seed: seed,
        }
    }
    
    /// Runs the backtest with the given strategy
    pub async fn run(&mut self, mut strategy: Box<dyn Strategy>) -> anyhow::Result<BacktestResults> {
        info!("Starting Backtest: {}", strategy.name());
        info!("WAL Path: {}", self.wal_path);
        
        strategy.on_start(self.bus.clone());
        
        // Subscribe to channels
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
                    self.current_symbol = event.symbol.clone();
                    strategy.on_tick(&event);
                    // Feed to exchange for order matching
                    self.exchange.on_market_data(event.clone()).await?;
                }
                MarketPayload::Bar { close, .. } => {
                    last_tick_price = *close;
                    self.last_price = *close;
                    self.current_symbol = event.symbol.clone();
                    strategy.on_bar(&event);
                }
                _ => {}
            }
            
            // Process any signals generated BEFORE updating equity
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
                        price: None, // Market order
                        quantity: signal.quantity,
                        order_type: OrderType::Market,
                    },
                };
                self.exchange.handle_order(order).await?;
            }

            // Drain order events from exchange (fills, accepts)
            while let Ok(order_event) = order_rx.try_recv() {
                if let OrderPayload::Update { status, filled_quantity, avg_price, .. } = order_event.payload {
                    if status == OrderStatus::Filled {
                        self.process_fill(&order_event.symbol, order_event.side, filled_quantity, avg_price);
                    }
                }
            }
            
            // Update equity AFTER fills/updates are processed
            self.update_equity(last_tick_price);
        }
        
        strategy.on_stop();
        
        // CRITICAL: Drain any remaining signals and orders after strategy stops
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

        // Final drain of order events
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
    
    fn update_equity(&mut self, current_price: f64) {
        self.last_price = current_price;

        // Mark portfolio to market using the actual symbol being traded
        if !self.current_symbol.is_empty() {
            self.portfolio.mark_to_market(&self.current_symbol, current_price);
        }

        // Equity = initial capital + realized PnL + unrealized PnL
        let realized = self.portfolio.total_realized_pnl();
        let unrealized = self.portfolio.total_unrealized_pnl();
        self.equity = self.initial_equity + realized + unrealized;

        self.results.equity_curve.push(self.equity);
        self.results.total_pnl = realized;

        // Update peak and drawdown
        if self.equity > self.peak_equity {
            self.peak_equity = self.equity;
        }
        let drawdown = (self.peak_equity - self.equity) / self.peak_equity;
        if drawdown > self.results.max_drawdown {
            self.results.max_drawdown = drawdown;
        }
    }
    
    /// Process a fill and update portfolio
    fn process_fill(&mut self, symbol: &str, side: Side, quantity: f64, price: f64) {
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
    
    fn calculate_metrics(&mut self) {
        // Calculate returns from equity curve
        if self.results.equity_curve.len() < 2 {
            return;
        }
        
        let returns: Vec<f64> = self.results.equity_curve
            .windows(2)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();
        
        self.results.returns = returns.clone();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        
        // Sharpe Ratio - Use sample variance (N-1) as per audit
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
        
        // Sortino Ratio - Correct downside deviation (denominator = total N, not negative count)
        let downside_sum_sq: f64 = returns.iter()
            .map(|&r| if r < 0.0 { r.powi(2) } else { 0.0 })
            .sum();
        
        let downside_dev = (downside_sum_sq / returns.len() as f64).sqrt();
        
        self.results.sortino_ratio = if downside_dev > 0.0 {
            (mean_return / downside_dev) * (252.0_f64).sqrt()
        } else {
            self.results.sharpe_ratio // No downside = use Sharpe
        };
        
        // Profit Factor (gross profit / gross loss)
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("═══════════════════════════════════════════");
    info!("    QuantKubera Backtest Engine v0.2      ");
    info!("═══════════════════════════════════════════");

    // Load config
    let config_str = fs::read_to_string("configs/backtest.toml").unwrap_or_else(|_| {
        fs::read_to_string("../../configs/backtest.toml").expect("Could not find configs/backtest.toml")
    });
    let config: Config = toml::from_str(&config_str).expect("Failed to parse backtest.toml");
    let b_config = config.backtest;

    // Use MomentumStrategy from kubera-core
    let strategy = Box::new(kubera_core::MomentumStrategy::new(5));
    info!("Starting Backtest: {}", strategy.name());
    info!("WAL Path: {}", b_config.wal_path);
    
    let mut engine = BacktestEngine::new(
        &b_config.wal_path, 
        b_config.initial_equity, 
        b_config.seed, 
        b_config.slippage_bps
    );
    let results = engine.run(strategy).await?;
    
    results.report();

    Ok(())
}
