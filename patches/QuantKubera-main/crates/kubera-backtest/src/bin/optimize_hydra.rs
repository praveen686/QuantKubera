use kubera_backtest::{
    ParameterSweepRunner, TpeOptimizer, WalkForwardConfig, 
    StrategyFactory, ParamSet, ParamRange,
};
use kubera_core::{HydraStrategy, hydra::{HydraConfig, ExchangeProfile}};
use kubera_core::Strategy;

struct HydraFactory {
    venue: ExchangeProfile,
}

impl StrategyFactory for HydraFactory {
    fn create(&self, params: &ParamSet) -> Box<dyn Strategy> {
        let mut config = HydraConfig::default();
        config.apply_profile(self.venue);
        
        if let Some(v) = params.get("learning_rate") {
            if let Some(f) = v.as_float() { config.learning_rate = f; }
        }
        if let Some(v) = params.get("zscore_action_threshold") {
            if let Some(f) = v.as_float() { config.zscore_action_threshold = f; }
        }
        if let Some(v) = params.get("position_hysteresis") {
            if let Some(f) = v.as_float() { config.position_hysteresis = f; }
        }
        if let Some(v) = params.get("lambda_risk") {
            if let Some(f) = v.as_float() { config.lambda_risk = f; }
        }
        
        Box::new(HydraStrategy::with_config(config))
    }
    
    fn name(&self) -> &str {
        "Hydra"
    }
}

#[tokio::main]
async fn run_optimization() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    // 1. Setup Runner
    let runner = ParameterSweepRunner::new(
        "../../binance_btcusdt.wal",
        10000.0,
        0.5,
        Some(42),
    );
    
    // 2. Define Param Space
    let mut optimizer = TpeOptimizer::new(20); // Reduced for faster verification in this session
    optimizer.add_param("learning_rate", ParamRange::Float { min: 0.01, max: 0.2, step: 0.01 });
    optimizer.add_param("zscore_action_threshold", ParamRange::Float { min: 0.5, max: 1.5, step: 0.1 });
    optimizer.add_param("position_hysteresis", ParamRange::Float { min: 0.1, max: 0.8, step: 0.1 });
    optimizer.add_param("lambda_risk", ParamRange::Float { min: 0.1, max: 0.8, step: 0.1 });
    
    let factory = HydraFactory { venue: ExchangeProfile::BinanceFutures };
    
    // 3. TPE Optimization (Global Sweep)
    println!("Starting Global TPE Optimization...");
    let results = runner.run_tpe(&factory, &optimizer).await;
    results.report();
    
    // 4. Walk-Forward Analysis (Robustness Check)
    println!("\nStarting 3-Fold Walk-Forward Analysis...");
    let wfa_config = WalkForwardConfig {
        n_folds: 3,
        train_ratio: 0.7,
        anchored: false,
    };
    
    // Clone optimizer for WFA template but reduce trials per fold
    let mut wfa_template = TpeOptimizer::new(10); 
    wfa_template.param_space = optimizer.param_space.clone();
    
    let wfa_results = runner.run_walk_forward(&factory, wfa_config, wfa_template).await?;
    wfa_results.report();
    
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // Increase stack size to 64MB for heavy HYDRA calculations
    let child = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            run_optimization()
        })?;

    child.join().expect("Failed to join thread")
}
