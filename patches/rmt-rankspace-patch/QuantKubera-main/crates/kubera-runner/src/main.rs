//! # QuantKubera Trading Runner
//!
//! The primary application entry point for the QuantKubera trading system.
//!
//! ## Description
//! Orchestrates the integration of all system components including:
//! - **Event Bus**: Low-latency communication channel.
//! - **Market Connectors**: Real-time data ingestion for multiple assets.
//! - **Risk Engine**: Pre-trade validation and global circuit breakers.
//! - **Execution Engine**: Simulated or live order fulfillment.
//! - **Trading Metrics**: Real-time Sharpe, Profit Factor, Drawdown tracking.
//! - **Interactive TUI**: Real-time monitoring and control interface.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use kubera_core::{EventBus, ExecutionMode, Strategy, wal::WalWriter, connector::MarketConnector};
use kubera_core::{TradingMetrics, MetricsConfig, TradeRecord};
use kubera_data::Level2Book;
use kubera_connectors::{BinanceConnector, ZerodhaConnector};
use kubera_executor::{SimulatedExchange, CommissionModel};
use kubera_risk::{RiskEngine, RiskConfig};
use kubera_options::nse_specs::{NseIndex, LotSizeValidator};
use kubera_options::chain::{OptionChainFetcher, IVSurface};
use kubera_models::{MarketPayload, OrderEvent, OrderPayload, Side, OrderType};
use std::sync::{Arc, Mutex};
use tracing::{info, error, warn};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
mod web_server;
mod circuit_breakers;
use web_server::{WebMessage, ServerState};
use circuit_breakers::{TradingCircuitBreakers, CircuitBreakerStatus};
use tokio::time::Duration;
use uuid::Uuid;
use chrono::Utc;
use clap::Parser;
use std::fs;
use tokio::process::Command;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::Deserialize;
use kubera_options::OptionType;

/// Parse NSE option symbol to extract strike, expiry, and option type
/// Example: "NIFTY2612025450CE" -> (25450.0, 2026-01-20, Call)
fn parse_nse_option_symbol(symbol: &str) -> Option<(f64, chrono::NaiveDate, OptionType)> {
    // Pattern: {INDEX}{YY}{MM}{DD}{STRIKE}{CE/PE}
    // Example: NIFTY2612025450CE

    let symbol = symbol.to_uppercase();

    // Extract option type (last 2 chars)
    if symbol.len() < 10 {
        return None;
    }

    let opt_type_str = &symbol[symbol.len()-2..];
    let option_type = match opt_type_str {
        "CE" => OptionType::Call,
        "PE" => OptionType::Put,
        _ => return None,
    };

    // Find where the index name ends (first digit)
    let digit_start = symbol.chars().position(|c| c.is_ascii_digit())?;
    let _index_name = &symbol[..digit_start];
    let rest = &symbol[digit_start..symbol.len()-2]; // Remove CE/PE

    // Parse YYMMDDSSSSS (year, month, day, strike)
    if rest.len() < 7 {
        return None;
    }

    let yy: i32 = rest[0..2].parse().ok()?;
    let mm: u32 = rest[2..4].parse().ok()?;
    let dd: u32 = rest[4..6].parse().ok()?;
    let strike: f64 = rest[6..].parse().ok()?;

    let year = 2000 + yy;
    let expiry = chrono::NaiveDate::from_ymd_opt(year, mm, dd)?;

    Some((strike, expiry, option_type))
}

/// Calculate time to expiry in years
fn time_to_expiry_years(expiry: chrono::NaiveDate) -> f64 {
    let today = chrono::Utc::now().date_naive();
    let days = (expiry - today).num_days();
    (days.max(0) as f64) / 365.0
}

/// Get NSE lot size for a given symbol
/// Returns 0 for non-NSE symbols (crypto, etc.)
///
/// NSE F&O lot sizes (as of Jan 2025):
/// - NIFTY: 65 units/lot (options)
/// - BANKNIFTY: 30 units/lot
/// - FINNIFTY: 25 units/lot
/// - MIDCPNIFTY: 50 units/lot
/// - SENSEX (BSE): 10 units/lot
/// - BANKEX (BSE): 15 units/lot
fn get_nse_lot_size(symbol: &str) -> u32 {
    let symbol_upper = symbol.to_uppercase();

    // NIFTY variants (options and futures) - NOT NIFTYBANK/NIFTYIT
    if symbol_upper.starts_with("NIFTY") && !symbol_upper.starts_with("NIFTYBANK") && !symbol_upper.starts_with("NIFTYIT") {
        // NIFTY lot size: 65 units
        return 65;
    }

    // BANKNIFTY variants
    if symbol_upper.starts_with("BANKNIFTY") {
        // BANKNIFTY lot size: 30 units
        return 30;
    }

    // FINNIFTY
    if symbol_upper.starts_with("FINNIFTY") {
        return 25;
    }

    // MIDCPNIFTY
    if symbol_upper.starts_with("MIDCPNIFTY") {
        return 50;
    }

    // SENSEX (BSE)
    if symbol_upper.starts_with("SENSEX") {
        return 10;
    }

    // BANKEX (BSE)
    if symbol_upper.starts_with("BANKEX") {
        return 15;
    }

    // Default: 0 means no lot rounding (crypto, forex, etc.)
    0
}

/// Heuristic check for NSE/BSE derivative symbols that require lot size rounding.
fn is_nse_like_symbol(symbol: &str) -> bool {
    let u = symbol.to_uppercase();
    // NSE/BSE derivatives naming: FUT/CE/PE suffixes, or index names
    u.ends_with("FUT") || u.ends_with("CE") || u.ends_with("PE")
        || u.contains("NIFTY") || u.contains("BANKNIFTY") || u.contains("FINNIFTY")
        || u.contains("MIDCPNIFTY") || u.contains("SENSEX") || u.contains("BANKEX")
}

#[derive(Debug, Deserialize, Clone)]
struct StrategyConfig {
    pub hydra: Option<kubera_core::hydra::HydraConfig>,
    pub aeon: Option<kubera_core::aeon::AeonConfig>,
}

/// Root configuration schema for the trading runner.
#[derive(Debug, Deserialize, Clone)]
struct RunnerConfig {
    mode: ModeInfo,
    risk: RiskInfo,
    execution: ExecutionInfo,
    strategy: Option<StrategyConfig>,
}

/// Information regarding the execution target and symbols.
#[derive(Debug, Deserialize, Clone)]
struct ModeInfo {
    symbols: Vec<String>,
}

/// Static risk constraints defined in configuration.
#[derive(Debug, Deserialize, Clone)]
struct RiskInfo {
    max_order_value_usd: f64,
    max_notional_per_symbol_usd: f64,
}

/// Operational settings for the execution layer.
#[derive(Debug, Deserialize, Clone)]
struct ExecutionInfo {
    slippage_bps: Option<f64>,
    commission_model: Option<String>,
    /// Per-symbol lot sizes for live/paper trading (NSE F&O).
    /// For backtests, lot sizes must come from WAL metadata (META:lot_sizes).
    lot_sizes: Option<std::collections::HashMap<String, u32>>,
}

use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, List, ListItem, Paragraph, Table, Row, Cell},
    layout::{Layout, Constraint, Direction},
    Terminal,
    style::{Style, Color, Modifier},
};
use crossterm::{
    event::{self, Event as CEvent, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

/// QuantKubera Trading Runner Command Line Interface
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Execution mode: paper, backtest, or live
    #[arg(short, long, default_value = "paper")]
    mode: String,

    /// Trading symbol
    #[arg(short, long, default_value = "BTCUSDT")]
    symbol: String,

    /// Enable AEON flagship strategy
    #[arg(long, default_value = "false")]
    aeon: bool,

    /// Enable strategy sandboxing (process isolation)
    #[arg(long, default_value = "false")]
    sandbox: bool,

    /// Use HYDRA strategy instead of ORAS
    #[arg(long, default_value = "false")]
    hydra: bool,

    /// Run in headless mode (no TUI, logs to stdout)
    #[arg(long, default_value = "false")]
    headless: bool,

    /// Initial capital for metrics calculation
    #[arg(long, default_value = "100000.0")]
    initial_capital: f64,

    /// Enable real-time market data recording to WAL
    #[arg(long, default_value = "false")]
    record: bool,

    /// Specify the WAL file path for recording or backtesting
    #[arg(long, default_value = "trading.wal")]
    wal_file: String,

    /// Override default config file path (default: configs/{mode}.toml)
    #[arg(long)]
    config: Option<String>,
}

/// Volatile state for a single instrument tracking.
struct SymbolState {
    last_price: f64,
    position: f64,
    book: Level2Book,
    /// Whether the automated strategy is currently making decisions.
    strategy_active: bool,
}

/// Global application state for the TUI and orchestration.
struct AppState {
    symbols: std::collections::HashMap<String, SymbolState>,
    equity: f64,
    realized_pnl: f64,
    order_log: Vec<String>,
    mode: ExecutionMode,
    /// Trading metrics engine
    metrics: TradingMetrics,
    /// Session start time
    session_start: std::time::Instant,
    /// Tick counter
    tick_count: u64,
    /// Circuit breakers (production safety)
    circuit_breakers: Option<TradingCircuitBreakers>,
    /// IV surface for options analytics (updated periodically in live mode)
    iv_surface: Option<IVSurface>,
    /// Option chain fetcher for live data
    #[allow(dead_code)]
    chain_fetcher: Option<OptionChainFetcher>,
}

/// Entry point for the trading application.
///
/// Initializes all subsystems, connects to market data, and starts the TUI event loop.
fn main() -> anyhow::Result<()> {
    // Use larger stack size (16MB) to handle deep call stacks
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024) // 16MB stack
        .build()?;

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let args = Args::parse();

    let mode = match args.mode.to_lowercase().as_str() {
        "backtest" => ExecutionMode::Backtest,
        "live" => ExecutionMode::Live,
        _ => ExecutionMode::Paper,
    };

    let config_file = args.config.clone().unwrap_or_else(|| {
        match mode {
            ExecutionMode::Backtest => "configs/backtest.toml".to_string(),
            ExecutionMode::Live => "configs/live.toml".to_string(),
            ExecutionMode::Paper => "configs/paper.toml".to_string(),
        }
    });

    let config_str = fs::read_to_string(&config_file).unwrap_or_else(|_| {
        fs::read_to_string(format!("../../{}", &config_file)).expect("Could not find config file")
    });
    let config: RunnerConfig = toml::from_str(&config_str).expect("Failed to parse config");

    let mut terminal = if !args.headless {
        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        Some(Terminal::new(backend)?)
    } else {
        println!("═══════════════════════════════════════════════════════════════");
        println!("        KUBERA HYDRA Paper Trading - Headless Mode");
        println!("═══════════════════════════════════════════════════════════════");
        println!("[HEADLESS] Initial Capital: ${:.2}", args.initial_capital);
        println!("[HEADLESS] Press Ctrl+C to stop");
        println!("═══════════════════════════════════════════════════════════════");
        None
    };

    dotenv::dotenv().ok();

    let metrics_port = std::env::var("METRICS_PORT").unwrap_or_else(|_| "9000".to_string());
    let metrics_addr = format!("0.0.0.0:{}", metrics_port).parse().expect("Invalid metrics address");
    kubera_core::observability::init_metrics(metrics_addr);
    kubera_core::observability::init_tracing("kubera-runner");

    let (web_tx, _) = tokio::sync::broadcast::channel(100);
    let web_state = Arc::new(ServerState { tx: web_tx.clone() });
    let web_state_server = web_state.clone();
    tokio::spawn(async move {
        web_server::start_server(web_state_server, 8080).await;
    });

    let mut symbols_state = std::collections::HashMap::new();
    for s in &config.mode.symbols {
        symbols_state.insert(s.clone(), SymbolState {
            last_price: 0.0,
            position: 0.0,
            book: Level2Book::new(s.clone()),
            strategy_active: args.headless, // Auto-enable in headless mode
        });
    }

    // Initialize trading metrics
    let metrics_config = MetricsConfig {
        initial_capital: args.initial_capital,
        risk_free_rate: 0.05,
        rolling_window: 252,
        min_trades_for_metrics: 5,
        trading_days_per_year: 252.0,
        var_confidence: 0.95,
        sampling_interval_seconds: 60.0,
    };
    let trading_metrics = TradingMetrics::new(metrics_config);

    // Initialize kill switch early for circuit breakers
    let kill_switch = Arc::new(AtomicBool::new(false));

    // Determine if we're trading Indian F&O
    let is_indian_fno = config.mode.symbols.iter()
        .any(|s| s.contains("NIFTY") || s.contains("BANKNIFTY") || s.contains("FINNIFTY"));

    // Initialize circuit breakers for production safety
    let circuit_breakers = if mode != ExecutionMode::Backtest {
        let cb = if is_indian_fno {
            TradingCircuitBreakers::for_indian_fno(args.initial_capital, kill_switch.clone())
        } else {
            TradingCircuitBreakers::new(args.initial_capital, kill_switch.clone())
        };
        info!("[RUNNER] Circuit breakers enabled (Indian F&O: {})", is_indian_fno);
        Some(cb)
    } else {
        None // No circuit breakers in backtest mode
    };

    let app_state = Arc::new(Mutex::new(AppState {
        symbols: symbols_state,
        equity: args.initial_capital,
        realized_pnl: 0.0,
        order_log: Vec::new(),
        mode,
        metrics: trading_metrics,
        session_start: std::time::Instant::now(),
        tick_count: 0,
        circuit_breakers,
        iv_surface: None, // Will be populated by option chain fetcher in live mode
        chain_fetcher: None, // Initialize with Zerodha auth when available
    }));

    let global_tick_count = Arc::new(AtomicU64::new(0));

    let risk_config = RiskConfig {
        max_order_value_usd: config.risk.max_order_value_usd,
        max_notional_per_symbol_usd: config.risk.max_notional_per_symbol_usd,
    };
    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(risk_config, kill_switch.clone())));

    let bus = EventBus::new(1000);
    let commission_model = match config.execution.commission_model.as_deref() {
        Some("ZerodhaFnO") => CommissionModel::ZerodhaFnO,
        Some("Linear") => CommissionModel::Linear(2.0),
        _ => CommissionModel::None,
    };

    let slippage_bps = config.execution.slippage_bps.unwrap_or(0.0);

    let mut exchange = SimulatedExchange::new(
        bus.clone(),
        slippage_bps,
        commission_model,
        None
    );
    info!("SimulatedExchange initialized with model: {:?}, slippage: {} bps", commission_model, slippage_bps);
    let mut wal_writer = WalWriter::new(&args.wal_file)?;
    let mut portfolio = kubera_core::Portfolio::new();

    if mode != ExecutionMode::Backtest {
        let mut connectors: Vec<Box<dyn MarketConnector>> = Vec::new();

        let binance_symbols: Vec<String> = config.mode.symbols.iter()
            .filter(|s| !s.contains("NIFTY") && !s.contains("BANKNIFTY") && !s.contains("FINNIFTY"))
            .cloned()
            .collect();

        if !binance_symbols.is_empty() {
            if let Ok(api_key) = std::env::var("BINANCE_API_KEY_ED25519") {
                connectors.push(Box::new(BinanceConnector::new(bus.clone(), binance_symbols, api_key)));
            } else {
                tracing::warn!("BINANCE_API_KEY_ED25519 not set, skipping Binance connector");
            }
        }

        let zerodha_symbols: Vec<String> = config.mode.symbols.iter()
            .filter(|s| s.contains("NIFTY") || s.contains("BANKNIFTY") || s.contains("FINNIFTY"))
            .cloned()
            .collect();

        if !zerodha_symbols.is_empty() {
            connectors.push(Box::new(ZerodhaConnector::new(bus.clone(), zerodha_symbols)));
        }

        for connector in connectors {
            tokio::spawn(async move {
                tracing::info!("Starting {} connector", connector.name());
                if let Err(e) = connector.run().await {
                    tracing::error!("{} connector error: {}", connector.name(), e);
                }
            });
        }
    } else {
        let bus_replay = bus.clone();
        let wal_file = args.wal_file.clone();
        let is_headless = args.headless;
        let state_report = app_state.clone();

        // Read lot sizes from WAL metadata (if available)
        let wal_lot_sizes: std::collections::HashMap<String, u32> = {
            use kubera_core::wal::WalReader;
            if let Ok(mut reader) = WalReader::new(&wal_file) {
                match reader.read_lot_sizes() {
                    Ok(lot_sizes) => {
                        if !lot_sizes.is_empty() {
                            info!("[BACKTEST] Loaded lot sizes from WAL metadata: {:?}", lot_sizes);
                        }
                        lot_sizes
                    }
                    Err(e) => {
                        warn!("[BACKTEST] Failed to read lot sizes from WAL: {}", e);
                        std::collections::HashMap::new()
                    }
                }
            } else {
                std::collections::HashMap::new()
            }
        };

        tokio::spawn(async move {
            use kubera_core::wal::WalReader;
            info!("Starting backtest replay from {}", wal_file);
            if let Ok(mut reader) = WalReader::new(&wal_file) {
                let mut count = 0;
                while let Ok(Some(event)) = reader.next_event() {
                    let _ = bus_replay.publish_market(event).await;
                    count += 1;
                }
                info!("Backtest replay complete. Processed {} events.", count);

                if is_headless {
                    // Give some time for the last trades to be processed
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let mut state = state_report.lock().unwrap();
                    state.metrics.recalculate();
                    let report = state.metrics.snapshot().detailed_report();
                    println!("\n{}", report);
                    println!("Backtest finished. Exiting.");
                    std::process::exit(0);
                }
            } else {
                error!("Failed to open WAL file: {}", wal_file);
            }
        });

        // BACKTEST: Spawn dedicated HYDRA strategy runner
        let bus_backtest_strat = bus.clone();
        let mut backtest_market_rx = bus_backtest_strat.subscribe_market();
        let hydra_config = config.strategy.as_ref()
            .and_then(|s| s.hydra.clone());
        let config_backtest = config.clone();
        let backtest_symbols = config.mode.symbols.clone();
        let backtest_lot_sizes = wal_lot_sizes; // Move into async block
        tokio::spawn(async move {
            let mut strategy: Box<dyn Strategy> = if std::env::args().any(|a| a == "--aeon") {
                let aeon_cfg = if let Some(sc) = &config_backtest.strategy { sc.aeon.clone().unwrap_or_default() } else { kubera_core::aeon::AeonConfig::default() };
                let hydra_cfg = if let Some(sc) = &config_backtest.strategy { sc.hydra.clone().unwrap_or_default() } else { kubera_core::hydra::HydraConfig::default() };
                info!("[BACKTEST] Initializing AEON FLAGSHIP strategy");
                Box::new(kubera_core::AeonStrategy::new(aeon_cfg, hydra_cfg))
            } else if std::env::args().any(|a| a == "--hydra") {
                let mut hydra = if let Some(cfg) = hydra_config {
                    info!("[BACKTEST] Initializing HYDRA with custom config from TOML");
                    kubera_core::HydraStrategy::with_config(cfg)
                } else {
                    kubera_core::HydraStrategy::new()
                };

                // STRICT MODE: lot sizes MUST come from WAL metadata (no fallbacks)
                if backtest_lot_sizes.is_empty() {
                    error!("[BACKTEST] WAL missing required lot size metadata (META:lot_sizes). Refusing to run with implicit fallbacks.");
                    error!("[BACKTEST] Re-record the WAL with the updated zerodha_ticker_stream.py to include lot sizes.");
                    std::process::exit(2);
                }
                for symbol in &backtest_symbols {
                    match backtest_lot_sizes.get(symbol).copied() {
                        Some(lot) if lot > 0 => hydra.set_lot_size_for_symbol(symbol, lot),
                        _ => {
                            error!("[BACKTEST] Missing lot size for symbol {} in WAL metadata; refusing to run.", symbol);
                            std::process::exit(2);
                        }
                    }
                }

                // Configure option params for first symbol (if it's an option)
                if let Some(first_symbol) = backtest_symbols.first() {
                    if let Some((strike, expiry, opt_type)) = parse_nse_option_symbol(first_symbol) {
                        let tte = time_to_expiry_years(expiry);
                        info!(
                            symbol = %first_symbol, strike = strike, expiry = %expiry,
                            tte_days = %(tte * 365.0), opt_type = ?opt_type,
                            "[BACKTEST] HYDRA configured with real option params for B-S Greeks"
                        );
                        hydra.set_option_params(strike, tte, opt_type);
                    }
                }

                Box::new(hydra)
            } else if let Some(cfg) = hydra_config {
                info!("[BACKTEST] Initializing HYDRA with custom config from TOML");
                Box::new(kubera_core::HydraStrategy::with_config(cfg))
            } else {
                Box::new(kubera_core::HydraStrategy::new())
            };
            strategy.on_start(bus_backtest_strat.clone());
            let strat_name = strategy.name().to_string();
            info!("[BACKTEST] {} strategy started for backtest replay", strat_name);
            
            while let Ok(event) = backtest_market_rx.recv().await {
                match &event.payload {
                    MarketPayload::Tick { .. } | MarketPayload::Trade { .. } |
                    MarketPayload::L2Update(_) | MarketPayload::L2Snapshot(_) => {
                        strategy.on_tick(&event);
                    }
                    MarketPayload::Bar { .. } => {
                        strategy.on_bar(&event);
                    }
                }
            }
        });
    }

    let bus_signal = bus.clone();
    let mut signal_rx = bus_signal.subscribe_signal();
    let state_signal = app_state.clone();
    tokio::spawn(async move {
        while let Ok(signal) = signal_rx.recv().await {
            let span = tracing::info_span!(
                "signal_to_order",
                strategy_id = %signal.strategy_id,
                symbol = %signal.symbol,
                side = ?signal.side
            );
            let _enter = span.enter();

            // === CIRCUIT BREAKER CHECK FOR SIGNALS ===
            let signal_allowed = {
                let mut state = state_signal.lock().unwrap();
                if let Some(ref mut cb) = state.circuit_breakers {
                    cb.allow_signal()
                } else {
                    true // No circuit breakers in backtest mode
                }
            };

            if !signal_allowed {
                warn!("Signal BLOCKED by circuit breaker: {} {} {:.4}",
                    signal.symbol, format!("{:?}", signal.side), signal.quantity);
                continue;
            }

            tracing::info!(
                quantity = %signal.quantity,
                "Signal received, converting to order"
            );

            // === NSE LOT SIZE VALIDATION ===
            // Detect index from symbol and validate/round lot size
            let validated_quantity = if let Some(index) = NseIndex::from_symbol(&signal.symbol) {
                let validator = LotSizeValidator::for_index(index);
                let lot_size = index.lot_size();

                // Round quantity to valid lot size
                let rounded_qty = validator.round_down(signal.quantity) as f64;

                if rounded_qty < lot_size as f64 {
                    // Quantity too small - round up to minimum lot
                    warn!(
                        "[LOT SIZE] Signal quantity {:.2} < lot size {} for {} - rounding UP to {}",
                        signal.quantity, lot_size, signal.symbol, lot_size
                    );
                    lot_size as f64
                } else if (rounded_qty - signal.quantity).abs() > 0.01 {
                    tracing::info!(
                        "[LOT SIZE] Rounded {} -> {} for {} (lot size: {})",
                        signal.quantity, rounded_qty, signal.symbol, lot_size
                    );
                    rounded_qty
                } else {
                    signal.quantity
                }
            } else {
                // Non-NSE symbol (crypto, etc.) - pass through as-is
                signal.quantity
            };

            // Skip if validated quantity is effectively zero
            if validated_quantity < 0.01 {
                warn!(
                    "[LOT SIZE] Skipping order with zero quantity after validation: {}",
                    signal.symbol
                );
                continue;
            }

            let order = OrderEvent {
                order_id: Uuid::new_v4(),
                intent_id: signal.intent_id,
                timestamp: signal.timestamp,
                symbol: signal.symbol.clone(),
                side: signal.side.clone(),
                payload: OrderPayload::New {
                    symbol: signal.symbol.clone(),
                    side: signal.side.clone(),
                    price: None,
                    quantity: validated_quantity,
                    order_type: OrderType::Market,
                },
            };

            tracing::info!(
                order_id = %order.order_id,
                symbol = %order.symbol,
                "Order generated from signal"
            );

            if let Err(e) = bus_signal.publish_order(order).await {
                tracing::error!("Failed to publish order from signal: {}", e);
            }
        }
        tracing::warn!("Signal channel closed, signal processor task exiting");
    });

    // Skip per-symbol strategy loop in backtest mode (dedicated HYDRA runs above)
    if mode == ExecutionMode::Backtest {
        // Backtest uses the dedicated HYDRA spawned above, skip per-symbol strategies
    } else {
    for (i, symbol) in config.mode.symbols.iter().enumerate() {
        let bus_strat = bus.clone();
        let state_strat = app_state.clone();
        let symbol_name = symbol.clone();
        let mut market_rx = bus_strat.subscribe_market();
        let is_sandbox = args.sandbox;
        let port = 9091 + i;
        let hydra_config = config.strategy.as_ref()
            .and_then(|s| s.hydra.clone());

        let runner_config = config.clone();
        tokio::spawn(async move {
            if is_sandbox {
                let addr = format!("127.0.0.1:{}", port);
                let listener = TcpListener::bind(&addr).await.expect("Failed to bind sandbox port");

                tracing::info!(symbol = %symbol_name, "Spawning sandboxed strategy on {}", addr);
                let _child = Command::new("cargo")
                    .arg("run")
                    .arg("-p")
                    .arg("kubera-strategy-host")
                    .arg("--")
                    .arg("--addr")
                    .arg(&addr)
                    .arg("--strategy")
                    .arg("ORAS")
                    .spawn()
                    .expect("Failed to spawn sandbox host");

                let (stream, _) = listener.accept().await.expect("Failed to accept sandbox connection");
                let (mut rd, mut wr) = stream.into_split();

                let len = rd.read_u32().await.unwrap() as usize;
                let mut buf = vec![0u8; len];
                rd.read_exact(&mut buf).await.unwrap();

                let bus_for_signals = bus_strat.clone();
                tokio::spawn(async move {
                    let mut s_buf = vec![0u8; 65536];
                    while let Ok(s_len) = rd.read_u32().await {
                        let s_len = s_len as usize;
                        if s_len > s_buf.len() { s_buf.resize(s_len, 0); }
                        rd.read_exact(&mut s_buf[..s_len]).await.unwrap();
                        let resp: kubera_models::HostResponse = serde_json::from_slice(&s_buf[..s_len]).unwrap();
                        if let kubera_models::HostResponse::Signal(sig) = resp {
                            let _ = bus_for_signals.publish_signal(sig).await;
                        }
                    }
                });

                loop {
                    if let Ok(event) = market_rx.recv().await {
                        if event.symbol != symbol_name { continue; }

                        let strategy_active = state_strat.lock().unwrap()
                            .symbols.get(&symbol_name).map(|x| x.strategy_active).unwrap_or(false);

                        if strategy_active {
                            let host_evt = match &event.payload {
                                kubera_models::MarketPayload::Tick { .. } => Some(kubera_models::HostEvent::Tick(event.clone())),
                                kubera_models::MarketPayload::Bar { .. } => Some(kubera_models::HostEvent::Bar(event.clone())),
                                _ => None,
                            };

                            if let Some(evt) = host_evt {
                                let buf = serde_json::to_vec(&evt).unwrap();
                                let _ = wr.write_u32(buf.len() as u32).await;
                                let _ = wr.write_all(&buf).await;
                            }
                        }
                    }
                }
            } else {
                let mut strategy: Box<dyn Strategy> = if std::env::args().any(|a| a == "--aeon") {
                    let aeon_cfg = if let Some(sc) = &runner_config.strategy { sc.aeon.clone().unwrap_or_default() } else { kubera_core::aeon::AeonConfig::default() };
                    let hydra_cfg = if let Some(sc) = &runner_config.strategy { sc.hydra.clone().unwrap_or_default() } else { kubera_core::hydra::HydraConfig::default() };
                    info!(symbol = %symbol_name, "Initializing AEON FLAGSHIP strategy");
                    Box::new(kubera_core::AeonStrategy::new(aeon_cfg, hydra_cfg))
                } else if std::env::args().any(|a| a == "--hydra") {
                    let mut hydra = if let Some(cfg) = hydra_config {
                        info!(symbol = %symbol_name, "Initializing HYDRA with custom config from TOML");
                        kubera_core::HydraStrategy::with_config(cfg)
                    } else {
                        kubera_core::HydraStrategy::new()
                    };

                    // Configure option params for real Black-Scholes Greeks
                    if let Some((strike, expiry, opt_type)) = parse_nse_option_symbol(&symbol_name) {
                        let tte = time_to_expiry_years(expiry);
                        info!(
                            symbol = %symbol_name,
                            strike = strike,
                            expiry = %expiry,
                            tte_days = %(tte * 365.0),
                            opt_type = ?opt_type,
                            "Configured HYDRA with real option params for B-S Greeks"
                        );
                        hydra.set_option_params(strike, tte, opt_type);
                    } else {
                        warn!(symbol = %symbol_name, "Could not parse option params - using crude Greeks estimates");
                    }

                    // Lot-size rounding is mandatory for NSE/BSE derivative-style symbols.
                    // Must be explicitly configured in [execution.lot_sizes] for live/paper.
                    if is_nse_like_symbol(&symbol_name) {
                        let lot = runner_config
                            .execution
                            .lot_sizes
                            .as_ref()
                            .and_then(|m| m.get(&symbol_name).copied());
                        match lot {
                            Some(v) if v > 0 => hydra.set_lot_size_for_symbol(&symbol_name, v),
                            _ => {
                                error!(
                                    symbol = %symbol_name,
                                    "Missing lot size in config [execution.lot_sizes] for NSE/BSE symbol; refusing to run."
                                );
                                error!("Add to config: [execution.lot_sizes]\n{}={}", symbol_name, get_nse_lot_size(&symbol_name));
                                return;
                            }
                        }
                    }

                    Box::new(hydra)
                } else if symbol_name == "BTCUSDT" || symbol_name == "ETHUSDT" {
                    Box::new(kubera_core::OrasStrategy::new())
                } else {
                    Box::new(kubera_core::MomentumStrategy::new(5))
                };

                let start_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    strategy.on_start(bus_strat.clone());
                }));

                if let Err(e) = start_result {
                    tracing::error!(symbol = %symbol_name, "Strategy on_start panic: {:?}", e);
                    return;
                }

                let mut panic_count: u32 = 0;
                const MAX_PANICS: u32 = 3;

                loop {
                    if let Ok(event) = market_rx.recv().await {
                        if event.symbol != symbol_name {
                            continue;
                        }

                        let strategy_active = state_strat
                            .lock()
                            .map(|s| s.symbols.get(&symbol_name).map(|x| x.strategy_active).unwrap_or(false))
                            .unwrap_or_else(|_| {
                                tracing::error!(symbol = %symbol_name, "State mutex poisoned, deactivating strategy");
                                false
                            });

                        if strategy_active {
                            match &event.payload {
                                MarketPayload::Tick { .. } => {
                                    let span = tracing::info_span!(
                                        "strategy_tick",
                                        symbol = %symbol_name,
                                        exchange_time = ?event.exchange_time
                                    );
                                    let _enter = span.enter();

                                    let symbol_for_panic = symbol_name.clone();
                                    let start = std::time::Instant::now();
                                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        strategy.on_tick(&event);
                                    }));
                                    metrics::histogram!("kubera_strategy_decision_latency_ms", "symbol" => symbol_name.clone(), "hook" => "on_tick").record(start.elapsed().as_secs_f64() * 1000.0);
                                    if let Err(e) = result {
                                        panic_count += 1;
                                        tracing::error!(
                                            symbol = %symbol_for_panic,
                                            panic_count = %panic_count,
                                            max_panics = %MAX_PANICS,
                                            "Strategy on_tick panic: {:?}", e
                                        );
                                    }
                                }
                                MarketPayload::Bar { .. } => {
                                    let span = tracing::info_span!(
                                        "strategy_bar",
                                        symbol = %symbol_name,
                                        exchange_time = ?event.exchange_time
                                    );
                                    let _enter = span.enter();

                                    let symbol_for_panic = symbol_name.clone();
                                    let start = std::time::Instant::now();
                                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        strategy.on_bar(&event);
                                    }));
                                    metrics::histogram!("kubera_strategy_decision_latency_ms", "symbol" => symbol_name.clone(), "hook" => "on_bar").record(start.elapsed().as_secs_f64() * 1000.0);
                                    if let Err(e) = result {
                                        panic_count += 1;
                                        tracing::error!(
                                            symbol = %symbol_for_panic,
                                            panic_count = %panic_count,
                                            max_panics = %MAX_PANICS,
                                            "Strategy on_bar panic: {:?}", e
                                        );
                                    }
                                }
                                // HFT V2: Route L2 events to strategy for book state tracking
                                MarketPayload::L2Update(_) | MarketPayload::L2Snapshot(_) | MarketPayload::Trade { .. } => {
                                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        strategy.on_tick(&event);
                                    }));
                                    if let Err(e) = result {
                                        panic_count += 1;
                                        tracing::error!(
                                            symbol = %symbol_name,
                                            "Strategy on_tick (L2) panic: {:?}", e
                                        );
                                    }
                                }
                            }

                            if panic_count >= MAX_PANICS {
                                tracing::error!(symbol = %symbol_name, "Strategy exceeded max panics, circuit breaker triggered. Stopping task.");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
    } // End of else block for non-backtest mode

    let bus_market = bus.clone();
    let state_market = app_state.clone();
    let mut market_rx = bus_market.subscribe_market();
    let global_tick_clone = global_tick_count.clone();

    let web_tx_market = web_tx.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(event) = market_rx.recv().await {
                global_tick_clone.fetch_add(1, Ordering::Relaxed);

                let mut state = state_market.lock().unwrap();
                state.tick_count += 1;

                if let Some(symbol_state) = state.symbols.get_mut(&event.symbol) {
                    match &event.payload {
                        MarketPayload::Tick { price, .. } => {
                            symbol_state.last_price = *price;
                        }
                        MarketPayload::Bar { close, .. } => {
                            symbol_state.last_price = *close;
                        }
                        MarketPayload::L2Update(update) => {
                            symbol_state.book.apply_update(&update);
                        }
                        _ => {}
                    }
                    let _ = web_tx_market.send(WebMessage::SymbolUpdate {
                        symbol: event.symbol.clone(),
                        last_price: symbol_state.last_price,
                        position: symbol_state.position,
                    });
                }
            }
        }
    });

    let bus_exec = bus.clone();
    let state_exec = app_state.clone();
    let mut order_rx = bus_exec.subscribe_order();
    let mut market_rx_exec = bus_exec.subscribe_market();
    let bus_update = bus_exec.clone();
    let mut order_update_rx = bus_update.subscribe_order_update();
    let web_tx_exec = web_tx.clone();
    let initial_capital = args.initial_capital;

    tokio::spawn(async move {
        let mut last_metrics_broadcast = std::time::Instant::now();

        loop {
            tokio::select! {
                Ok(order) = order_rx.recv() => {
                    // === CIRCUIT BREAKER CHECK FOR ORDERS ===
                    let order_allowed = {
                        let mut state = state_exec.lock().unwrap();
                        if let Some(ref mut cb) = state.circuit_breakers {
                            cb.allow_order()
                        } else {
                            true // No circuit breakers in backtest mode
                        }
                    };

                    if !order_allowed {
                        let mut state = state_exec.lock().unwrap();
                        let msg = format!("[{}] ORDER BLOCKED by circuit breaker: {} {:?}",
                            Utc::now().format("%H:%M:%S"), order.symbol, order.side);
                        state.order_log.push(msg.clone());
                        if state.order_log.len() > 10 { state.order_log.remove(0); }
                        let _ = web_tx_exec.send(WebMessage::OrderLog { message: msg });
                        continue;
                    }

                    let violation = {
                        let risk = risk_engine.lock().unwrap();
                        let symbol = &order.symbol;
                        let price = {
                            let state = state_exec.lock().unwrap();
                            state.symbols.get(symbol).map(|s| s.last_price).unwrap_or(0.0)
                        };

                        if let Err(violation) = risk.check_order(&order, price) {
                            Some(violation)
                        } else {
                            let _ = wal_writer.log_order(&order);
                            let _ = wal_writer.flush(); // Flush orders immediately
                            None
                        }
                    };

                    if let Some(violation) = violation {
                        let mut state = state_exec.lock().unwrap();
                        let msg = format!("[{}] RISK REJECT: {}", Utc::now().format("%H:%M:%S"), violation);
                        state.order_log.push(msg.clone());
                        if state.order_log.len() > 10 { state.order_log.remove(0); }
                        let _ = web_tx_exec.send(WebMessage::OrderLog { message: msg });
                        continue;
                    }

                    let _ = exchange.handle_order(order).await;
                }
                Ok(update) = order_update_rx.recv() => {
                    if let OrderPayload::Update { status, .. } = &update.payload {
                        if *status == kubera_models::OrderStatus::Filled {
                            let _ = wal_writer.log_order(&update);
                            let _ = wal_writer.flush();
                        }
                    }

                    if let OrderPayload::Update { status, filled_quantity, avg_price, commission, .. } = &update.payload {
                        if *status == kubera_models::OrderStatus::Filled {
                            let symbol = &update.symbol;
                            let side = update.side;

                            let pnl = portfolio.apply_fill(symbol, side, *filled_quantity, *avg_price, *commission);

                            // Record trade in metrics
                            {
                                let mut state = state_exec.lock().unwrap();
                                state.metrics.record_trade(TradeRecord {
                                    timestamp: Utc::now(),
                                    symbol: symbol.clone(),
                                    is_long: side == Side::Buy,
                                    entry_price: *avg_price,
                                    exit_price: Some(*avg_price),
                                    quantity: *filled_quantity,
                                    pnl,
                                    commission: *commission,
                                    slippage_bps: 0.5, // V2: Updated for demo
                                    duration_secs: 0,
                                    source: "HYDRA".to_string(),
                                });
                            }

                            {
                                let mut risk = risk_engine.lock().unwrap();
                                if let Some(pos) = portfolio.get_position(symbol) {
                                    risk.update_position(symbol.to_string(), pos.quantity);
                                }
                            }

                            let mut state = state_exec.lock().unwrap();
                            if let Some(symbol_state) = state.symbols.get_mut(symbol) {
                                symbol_state.position = portfolio.get_position(symbol).map(|p| p.quantity).unwrap_or(0.0);
                            }
                            state.realized_pnl = portfolio.total_realized_pnl();
                            let equity = initial_capital + state.realized_pnl + portfolio.total_unrealized_pnl();
                            state.equity = equity;

                            // Update metrics with new equity
                            state.metrics.update_equity(equity);

                            // === UPDATE CIRCUIT BREAKER DRAWDOWN MONITOR ===
                            if let Some(ref mut cb) = state.circuit_breakers {
                                cb.update_equity(equity);
                            }

                            let msg = format!("[{}] FILL: {} {} @ {:.2} (PnL: {:.2}, Comm: {:.2})",
                                Utc::now().format("%H:%M:%S"),
                                format!("{:?}", side).to_uppercase(),
                                filled_quantity, avg_price, pnl, commission);
                            state.order_log.push(msg.clone());
                            if state.order_log.len() > 15 { state.order_log.remove(0); }

                            let _ = web_tx_exec.send(WebMessage::Status { equity: state.equity, realized_pnl: state.realized_pnl });
                            let _ = web_tx_exec.send(WebMessage::OrderLog { message: msg });

                            // Broadcast trade execution
                            let _ = web_tx_exec.send(WebMessage::TradeExecution {
                                symbol: symbol.clone(),
                                side: format!("{:?}", side),
                                quantity: *filled_quantity,
                                price: *avg_price,
                                pnl,
                                commission: *commission,
                                strategy: "HYDRA".to_string(),
                            });
                        }
                    }
                }
                Ok(market) = market_rx_exec.recv() => {
                    // Measure processing latency (exchange_time -> now)
                    let processing_start = std::time::Instant::now();
                    let exchange_latency_ms = (Utc::now() - market.exchange_time).num_milliseconds() as f64;

                    // Handle both Tick and Trade events for mark-to-market
                    let price = match &market.payload {
                        MarketPayload::Tick { price, .. } => Some(*price),
                        MarketPayload::Trade { price, .. } => Some(*price),
                        _ => None,
                    };
                    if let Some(price) = price {
                        let mut state = state_exec.lock().unwrap();
                        portfolio.mark_to_market(&market.symbol, price);
                        let equity = initial_capital + portfolio.total_realized_pnl() + portfolio.total_unrealized_pnl();
                        state.equity = equity;

                        // Update metrics equity with market timestamp for proper Sharpe calculation in backtest
                        state.metrics.update_equity_with_timestamp(equity, market.exchange_time);

                        // === UPDATE CIRCUIT BREAKER MONITORS ===
                        if let Some(ref mut cb) = state.circuit_breakers {
                            cb.update_equity(equity);
                            // Record end-to-end latency for circuit breaker monitoring
                            let total_latency_ms = processing_start.elapsed().as_secs_f64() * 1000.0 + exchange_latency_ms.max(0.0);
                            cb.record_latency(total_latency_ms);
                        }
                    }

                    // Record market data if enabled
                    if args.record {
                        if let Err(e) = wal_writer.log_event(&market) {
                            warn!("Failed to record market event to WAL: {}", e);
                        }
                        let _ = wal_writer.flush();
                    }

                    let _ = exchange.on_market_data(market).await;

                    // Broadcast metrics periodically (every 5 seconds)
                    if last_metrics_broadcast.elapsed() > Duration::from_secs(5) {
                        let state = state_exec.lock().unwrap();
                        let snapshot = state.metrics.snapshot();

                        // Export to Prometheus
                        snapshot.export_prometheus();

                        // Broadcast to WebSocket clients
                        let _ = web_tx_exec.send(WebMessage::from_metrics_snapshot(snapshot));

                        last_metrics_broadcast = std::time::Instant::now();
                    }
                }
            }
        }
    });

    // Headless mode: simple loop that logs status with metrics
    if args.headless {
        let state_headless = app_state.clone();
        let mut market_rx_headless = bus.subscribe_market();
        let mut signal_rx_headless = bus.subscribe_signal();
        let mut order_rx_headless = bus.subscribe_order_update();
        let mut tick_count: u64 = 0;
        let mut last_metrics_log = std::time::Instant::now();

        // Auto-enable strategies in headless mode
        {
            let mut state = state_headless.lock().unwrap();
            for (_, sym_state) in state.symbols.iter_mut() {
                sym_state.strategy_active = true;
            }
        }
        println!("[HEADLESS] Strategies AUTO-ENABLED for all symbols");
        println!("═══════════════════════════════════════════════════════════════\n");

        loop {
            tokio::select! {
                Ok(event) = market_rx_headless.recv() => {
                    tick_count += 1;

                    if let MarketPayload::Tick { price, .. } = &event.payload {
                        // Log every 100 ticks
                        if tick_count % 100 == 0 {
                            let state = state_headless.lock().unwrap();
                            println!("[TICK {:>6}] {} @ {:.2} | Equity: {:.2} | PnL: {:.2}",
                                tick_count, event.symbol, price, state.equity, state.realized_pnl);
                        }
                    }

                    // Log detailed metrics every 30 seconds
                    if last_metrics_log.elapsed() > Duration::from_secs(30) {
                        let mut state = state_headless.lock().unwrap();
                        state.metrics.recalculate();
                        let snapshot = state.metrics.snapshot();

                        println!("\n───────────────────────────────────────────────────────────────");
                        println!("                    LIVE METRICS UPDATE");
                        println!("───────────────────────────────────────────────────────────────");
                        println!("  Equity:       {:>12.2}  |  Peak:         {:>12.2}", snapshot.equity, snapshot.peak_equity);
                        println!("  Total PnL:    {:>12.2}  |  Return:       {:>11.2}%", snapshot.total_pnl, snapshot.total_return_pct);
                        println!("  ─────────────────────────────────────────────────────────────");
                        println!("  Sharpe:       {:>12.2}  |  Sortino:      {:>12.2}", snapshot.sharpe_ratio, snapshot.sortino_ratio);
                        println!("  Max DD:       {:>11.2}%  |  Current DD:   {:>11.2}%", snapshot.max_drawdown_pct, snapshot.current_drawdown_pct);
                        println!("  Profit Factor:{:>12.2}  |  Win Rate:     {:>11.2}%", snapshot.profit_factor, snapshot.win_rate * 100.0);
                        println!("  ─────────────────────────────────────────────────────────────");
                        println!("  Trades:       {:>12}  |  Expectancy:   {:>12.2}", snapshot.total_trades, snapshot.expectancy);
                        println!("  Avg Win:      {:>12.2}  |  Avg Loss:     {:>12.2}", snapshot.avg_win, snapshot.avg_loss);
                        println!("  Kelly %:      {:>11.2}%  |  VaR (95%):    {:>12.2}", snapshot.kelly_fraction * 100.0, snapshot.var_95);

                        // IV Surface info (if options data available)
                        if let Some(ref surface) = state.iv_surface {
                            let term_structure = surface.term_structure();
                            if !term_structure.is_empty() {
                                println!("  ─────────────────────────────────────────────────────────────");
                                println!("  IV SURFACE ({}) | Spot: {:.2}", surface.underlying, surface.spot_price);
                                let contango = if surface.is_contango() { "Contango" } else { "Backwardation" };
                                println!("  Term Structure: {} | Expiries: {}", contango, term_structure.len());
                                for (expiry, iv) in term_structure.iter().take(3) {
                                    println!("    {} ATM IV: {:.2}%", expiry, iv * 100.0);
                                }
                            }
                        }

                        // Circuit Breaker status summary
                        if let Some(ref cb) = state.circuit_breakers {
                            let cb_summary = cb.status_summary(state.equity);
                            let latency_info = cb.current_latency_p99()
                                .map(|l| format!("{:.1}ms", l))
                                .unwrap_or_else(|| "N/A".to_string());
                            println!("  ─────────────────────────────────────────────────────────────");
                            println!("  CIRCUIT BREAKERS: {} | p99 Latency: {}", cb_summary, latency_info);
                            println!("  Rate Limits: Sig={:.0} Ord={:.0} | Trips: {}",
                                cb.signal_rate_available(), cb.order_rate_available(), cb.trip_count());
                        }
                        println!("───────────────────────────────────────────────────────────────\n");

                        // Export to Prometheus
                        snapshot.export_prometheus();

                        last_metrics_log = std::time::Instant::now();
                    }
                }
                Ok(signal) = signal_rx_headless.recv() => {
                    // HFT V2: Enhanced signal logging with book context
                    // Show spread in $ (more useful for crypto) and bps with more precision
                    let spread_usd = signal.decision_ask - signal.decision_bid;
                    println!("[SIGNAL] {:>4} {} @ {:.2} | Qty: {:.4} | Bid: {:.2} | Ask: {:.2} | Spread: ${:.2} ({:.4}bps) | Edge: {:.2}bps | {}",
                        format!("{:?}", signal.side).to_uppercase(),
                        signal.symbol, signal.price, signal.quantity,
                        signal.decision_bid, signal.decision_ask, spread_usd, signal.spread_bps,
                        signal.expected_edge_bps, signal.strategy_id);
                }
                Ok(order) = order_rx_headless.recv() => {
                    if let OrderPayload::Update { status, filled_quantity, avg_price, commission, .. } = &order.payload {
                        if *status == kubera_models::OrderStatus::Filled {
                            // HFT V2: Enhanced fill logging with slippage calculation
                            // Note: We track slippage relative to mid price when signal was generated
                            println!("[FILL]   {} {} | {:.4} @ {:.2} | Commission: {:.4}",
                                order.symbol, format!("{:?}", order.side).to_uppercase(),
                                filled_quantity, avg_price, commission);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\n═══════════════════════════════════════════════════════════════");
                    println!("                    SESSION COMPLETE");
                    println!("═══════════════════════════════════════════════════════════════");

                    let mut state = state_headless.lock().unwrap();
                    state.metrics.recalculate();
                    println!("{}", state.metrics.snapshot().detailed_report());

                    break;
                }
            }
        }
        return Ok(());
    }

    // TUI Mode with Enhanced Metrics Display
    let terminal = terminal.as_mut().unwrap();
    let mut last_metrics_update = std::time::Instant::now();

    loop {
        // Update metrics periodically
        if last_metrics_update.elapsed() > Duration::from_secs(1) {
            let mut state = app_state.lock().unwrap();
            state.metrics.recalculate();
            state.metrics.export_prometheus();
            last_metrics_update = std::time::Instant::now();
        }

        terminal.draw(|f| {
            let size = f.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),   // Header
                    Constraint::Length(8),   // Metrics panel
                    Constraint::Min(5),      // Main content
                    Constraint::Length(8),   // Order log
                ].as_ref())
                .split(size);

            let state = app_state.lock().unwrap();
            let is_killed = kill_switch.load(Ordering::SeqCst);
            let status_color = if is_killed { Color::Red } else { Color::Green };
            let status_text = if is_killed { " [LOCKED - KILL SWITCH ACTIVE]" } else { " [ACTIVE]" };

            let uptime = state.session_start.elapsed().as_secs();
            let header = Paragraph::new(format!(
                " QuantKubera {} | Status:{} | Uptime: {}s | Ticks: {}",
                state.mode, status_text, uptime, state.tick_count
            ))
            .style(Style::default().fg(status_color).add_modifier(Modifier::BOLD))
            .block(Block::default().borders(Borders::ALL).title("KUBERA HYDRA"));
            f.render_widget(header, chunks[0]);

            // Metrics Panel
            let snapshot = state.metrics.snapshot();
            let metrics_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                    Constraint::Percentage(25),
                ].as_ref())
                .split(chunks[1]);

            // Return Metrics
            let return_rows = vec![
                Row::new(vec![Cell::from("Total PnL"), Cell::from(format!("{:.2}", snapshot.total_pnl))]),
                Row::new(vec![Cell::from("Return %"), Cell::from(format!("{:.2}%", snapshot.total_return_pct))]),
                Row::new(vec![Cell::from("Ann. Return"), Cell::from(format!("{:.2}%", snapshot.annualized_return))]),
            ];
            let return_table = Table::new(return_rows, [Constraint::Percentage(50), Constraint::Percentage(50)])
                .block(Block::default().borders(Borders::ALL).title("Returns"));
            f.render_widget(return_table, metrics_chunks[0]);

            // Risk Metrics
            let sharpe_color = if snapshot.sharpe_ratio >= 1.0 { Color::Green } else if snapshot.sharpe_ratio >= 0.5 { Color::Yellow } else { Color::Red };
            let risk_rows = vec![
                Row::new(vec![
                    Cell::from("Sharpe"),
                    Cell::from(format!("{:.2}", snapshot.sharpe_ratio)).style(Style::default().fg(sharpe_color))
                ]),
                Row::new(vec![Cell::from("Max DD"), Cell::from(format!("{:.2}%", snapshot.max_drawdown_pct))]),
                Row::new(vec![Cell::from("VaR 95%"), Cell::from(format!("{:.2}", snapshot.var_95))]),
            ];
            let risk_table = Table::new(risk_rows, [Constraint::Percentage(50), Constraint::Percentage(50)])
                .block(Block::default().borders(Borders::ALL).title("Risk"));
            f.render_widget(risk_table, metrics_chunks[1]);

            // Trade Metrics
            let pf_color = if snapshot.profit_factor >= 2.0 { Color::Green } else if snapshot.profit_factor >= 1.5 { Color::Yellow } else { Color::Red };
            let trade_rows = vec![
                Row::new(vec![Cell::from("Trades"), Cell::from(format!("{}", snapshot.total_trades))]),
                Row::new(vec![Cell::from("Win Rate"), Cell::from(format!("{:.1}%", snapshot.win_rate * 100.0))]),
                Row::new(vec![
                    Cell::from("Profit Factor"),
                    Cell::from(format!("{:.2}", snapshot.profit_factor)).style(Style::default().fg(pf_color))
                ]),
            ];
            let trade_table = Table::new(trade_rows, [Constraint::Percentage(50), Constraint::Percentage(50)])
                .block(Block::default().borders(Borders::ALL).title("Trades"));
            f.render_widget(trade_table, metrics_chunks[2]);

            // Efficiency Metrics
            let eff_rows = vec![
                Row::new(vec![Cell::from("Expectancy"), Cell::from(format!("{:.2}", snapshot.expectancy))]),
                Row::new(vec![Cell::from("Kelly %"), Cell::from(format!("{:.2}%", snapshot.kelly_fraction * 100.0))]),
                Row::new(vec![Cell::from("Recovery"), Cell::from(format!("{:.2}", snapshot.recovery_factor))]),
            ];
            let eff_table = Table::new(eff_rows, [Constraint::Percentage(50), Constraint::Percentage(50)])
                .block(Block::default().borders(Borders::ALL).title("Efficiency"));
            f.render_widget(eff_table, metrics_chunks[3]);

            // Main content: Symbols + Actions
            let mid_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(60), Constraint::Percentage(40)].as_ref())
                .split(chunks[2]);

            let mut symbol_rows = Vec::new();
            let mut symbols_list: Vec<_> = state.symbols.keys().collect();
            symbols_list.sort();

            for sym in symbols_list {
                if let Some(sym_state) = state.symbols.get(sym) {
                    let strat_status = if sym_state.strategy_active { "ON" } else { "OFF" };
                    let strat_color = if sym_state.strategy_active { Color::Cyan } else { Color::Gray };
                    let pos_color = if sym_state.position > 0.0 { Color::Green } else if sym_state.position < 0.0 { Color::Red } else { Color::White };

                    symbol_rows.push(Row::new(vec![
                        Cell::from(sym.clone()),
                        Cell::from(format!("{:.2}", sym_state.last_price)),
                        Cell::from(format!("{:.4}", sym_state.position)).style(Style::default().fg(pos_color)),
                        Cell::from(strat_status).style(Style::default().fg(strat_color)),
                    ]));
                }
            }

            let symbol_table = Table::new(symbol_rows, [
                Constraint::Percentage(35),
                Constraint::Percentage(25),
                Constraint::Percentage(20),
                Constraint::Percentage(20)
            ])
                .header(Row::new(vec!["Symbol", "Price", "Position", "Strategy"]).style(Style::default().fg(Color::Yellow)))
                .block(Block::default().borders(Borders::ALL).title("Assets"));
            f.render_widget(symbol_table, mid_chunks[0]);

            let right_mid_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(35), Constraint::Percentage(35), Constraint::Percentage(30)].as_ref())
                .split(mid_chunks[1]);

            let mut portfolio_rows = vec![
                Row::new(vec![Cell::from("Equity"), Cell::from(format!("{:.2}", state.equity))]),
                Row::new(vec![Cell::from("Realized PnL"), Cell::from(format!("{:.2}", state.realized_pnl))]),
                Row::new(vec![Cell::from("Unrealized"), Cell::from(format!("{:.2}", state.equity - state.realized_pnl - args.initial_capital))]),
            ];

            // Add IV Surface info if available (for options trading)
            if let Some(ref surface) = state.iv_surface {
                let term_structure = surface.term_structure();
                if let Some((expiry, atm_iv)) = term_structure.first() {
                    let iv_color = if *atm_iv > 0.25 { Color::Red } else if *atm_iv > 0.15 { Color::Yellow } else { Color::Green };
                    portfolio_rows.push(Row::new(vec![
                        Cell::from(format!("ATM IV {}", expiry.format("%d%b"))),
                        Cell::from(format!("{:.1}%", atm_iv * 100.0)).style(Style::default().fg(iv_color))
                    ]));
                }
            } else {
                portfolio_rows.push(Row::new(vec![Cell::from("Peak Equity"), Cell::from(format!("{:.2}", snapshot.peak_equity))]));
            }

            let portfolio_table = Table::new(portfolio_rows, [Constraint::Percentage(50), Constraint::Percentage(50)])
                .block(Block::default().borders(Borders::ALL).title("Portfolio"))
                .header(Row::new(vec!["Metric", "Value"]).style(Style::default().add_modifier(Modifier::BOLD)));
            f.render_widget(portfolio_table, right_mid_chunks[0]);

            // Circuit Breaker Status Panel
            let cb_status: Option<CircuitBreakerStatus> = state.circuit_breakers.as_ref().map(|cb| cb.detailed_status(state.equity));
            let cb_rows = if let Some(ref cbs) = cb_status {
                // Build status indicators
                let mut trip_reasons: Vec<&str> = Vec::new();
                if cbs.kill_switch_active { trip_reasons.push("KILL"); }
                if cbs.latency_tripped { trip_reasons.push("LAT"); }
                if cbs.order_flow_tripped { trip_reasons.push("FLOW"); }
                if cbs.drawdown_tripped { trip_reasons.push("DD"); }

                let status_color = if cbs.is_tripped { Color::Red } else { Color::Green };
                let status_text = if cbs.is_tripped {
                    format!("TRIPPED: {}", trip_reasons.join(","))
                } else {
                    "OK".to_string()
                };
                let latency_text = cbs.p99_latency_ms.map(|l| format!("{:.1}ms", l)).unwrap_or_else(|| "N/A".to_string());
                vec![
                    Row::new(vec![
                        Cell::from("Status"),
                        Cell::from(status_text).style(Style::default().fg(status_color))
                    ]),
                    Row::new(vec![Cell::from("p99 Latency"), Cell::from(latency_text)]),
                    Row::new(vec![Cell::from("Drawdown"), Cell::from(format!("{:.2}%", cbs.current_drawdown_pct))]),
                    Row::new(vec![Cell::from("Sig/Ord Tokens"), Cell::from(format!("{:.0}/{:.0}", cbs.signal_tokens, cbs.order_tokens))]),
                    Row::new(vec![Cell::from("Trip Count"), Cell::from(format!("{}", cbs.trip_count))]),
                ]
            } else {
                vec![Row::new(vec![Cell::from("Disabled"), Cell::from("Backtest mode")])]
            };
            let cb_table = Table::new(cb_rows, [Constraint::Percentage(50), Constraint::Percentage(50)])
                .block(Block::default().borders(Borders::ALL).title("Circuit Breakers"));
            f.render_widget(cb_table, right_mid_chunks[1]);

            let actions_text = vec![
                " 's' BTC | 't' ETH | 'n' NIFTY",
                " 'k' Kill | 'r' Reset CB",
                " 'm' Log | 'q' Quit",
            ];
            let actions_items: Vec<ListItem> = actions_text.iter().map(|s| ListItem::new(*s)).collect();
            let actions = List::new(actions_items)
                .block(Block::default().borders(Borders::ALL).title("Hotkeys"));
            f.render_widget(actions, right_mid_chunks[2]);

            // Order Log
            let log_items: Vec<ListItem> = state.order_log.iter().rev()
                .map(|l| ListItem::new(l.as_str())).collect();
            let log_list = List::new(log_items)
                .block(Block::default().borders(Borders::ALL).title("Order Log"));
            f.render_widget(log_list, chunks[3]);
        })?;

        if event::poll(Duration::from_millis(100))? {
            if let CEvent::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('s') => {
                        let mut state = app_state.lock().unwrap();
                        if let Some(s) = state.symbols.get_mut("BTCUSDT") {
                            s.strategy_active = !s.strategy_active;
                        }
                    }
                    KeyCode::Char('t') => {
                        let mut state = app_state.lock().unwrap();
                        if let Some(s) = state.symbols.get_mut("ETHUSDT") {
                            s.strategy_active = !s.strategy_active;
                        }
                    }
                    KeyCode::Char('n') => {
                        let mut state = app_state.lock().unwrap();
                        // Toggle any NIFTY symbol
                        for (sym, sym_state) in state.symbols.iter_mut() {
                            if sym.contains("NIFTY") {
                                sym_state.strategy_active = !sym_state.strategy_active;
                            }
                        }
                    }
                    KeyCode::Char('k') => {
                        let current = kill_switch.load(Ordering::SeqCst);
                        kill_switch.store(!current, Ordering::SeqCst);
                        let _ = web_tx.send(WebMessage::KillSwitch {
                            triggered: !current,
                            reason: if !current { Some("Manual trigger".to_string()) } else { None }
                        });
                    }
                    KeyCode::Char('m') => {
                        // Log detailed metrics report
                        let state = app_state.lock().unwrap();
                        let report = state.metrics.snapshot().detailed_report();
                        tracing::info!("{}", report);
                    }
                    KeyCode::Char('r') => {
                        // Reset all circuit breakers
                        let mut state = app_state.lock().unwrap();
                        let current_equity = state.equity;
                        let has_cb = state.circuit_breakers.is_some();
                        if let Some(ref mut cb) = state.circuit_breakers {
                            cb.reset_all(current_equity);
                        }
                        if has_cb {
                            state.order_log.push(format!("[{}] Circuit breakers RESET", Utc::now().format("%H:%M:%S")));
                            if state.order_log.len() > 10 { state.order_log.remove(0); }
                        }
                    }
                    KeyCode::Char('b') => {
                        let bus_pub = bus.clone();
                        tokio::spawn(async move {
                            let _ = bus_pub.publish_order(OrderEvent {
                                order_id: Uuid::new_v4(),
                                intent_id: None,
                                timestamp: Utc::now(),
                                symbol: "BTCUSDT".to_string(),
                                side: Side::Buy,
                                payload: OrderPayload::New {
                                    symbol: "BTCUSDT".to_string(),
                                    side: Side::Buy,
                                    price: None,
                                    quantity: 0.1,
                                    order_type: OrderType::Market,
                                }
                            }).await;
                        });
                    }
                    KeyCode::Char('e') => {
                        let bus_pub = bus.clone();
                        tokio::spawn(async move {
                            let _ = bus_pub.publish_order(OrderEvent {
                                order_id: Uuid::new_v4(),
                                intent_id: None,
                                timestamp: Utc::now(),
                                symbol: "ETHUSDT".to_string(),
                                side: Side::Buy,
                                payload: OrderPayload::New {
                                    symbol: "ETHUSDT".to_string(),
                                    side: Side::Buy,
                                    price: None,
                                    quantity: 1.0,
                                    order_type: OrderType::Market,
                                }
                            }).await;
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    // Final metrics report on exit
    {
        let mut state = app_state.lock().unwrap();
        state.metrics.recalculate();
        info!("{}", state.metrics.snapshot().detailed_report());
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
