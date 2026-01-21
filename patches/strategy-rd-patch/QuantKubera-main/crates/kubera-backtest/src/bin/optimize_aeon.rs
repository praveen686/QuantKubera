use kubera_backtest::{
    ParameterSweepRunner, TpeOptimizer, WalkForwardConfig, 
    StrategyFactory, ParamSet, ParamRange,
};
use kubera_core::{
    AeonStrategy, 
    aeon::AeonConfig,
    hydra::HydraConfig,
};
use kubera_core::Strategy;
use std::sync::Arc;

struct AeonFactory;

impl StrategyFactory for AeonFactory {
    fn create(&self, params: &ParamSet) -> Box<dyn Strategy> {
        let mut aeon_cfg = AeonConfig::default();
        let mut hydra_cfg = HydraConfig::default();
        
        // Map optimization parameters to AeonConfig
        if let Some(v) = params.get("bif_predictability_threshold") {
            if let Some(f) = v.as_float() { aeon_cfg.bif_predictability_threshold = f; }
        }
        if let Some(v) = params.get("fti_trend_threshold") {
            if let Some(f) = v.as_float() { aeon_cfg.fti_trend_threshold = f; }
        }
        if let Some(v) = params.get("fti_mean_rev_threshold") {
            if let Some(f) = v.as_float() { aeon_cfg.fti_mean_rev_threshold = f; }
        }
        
        // Map optimization parameters to HydraConfig
        if let Some(v) = params.get("learning_rate") {
            if let Some(f) = v.as_float() { hydra_cfg.learning_rate = f; }
        }
        if let Some(v) = params.get("zscore_action_threshold") {
            if let Some(f) = v.as_float() { hydra_cfg.zscore_action_threshold = f; }
        }

        Box::new(AeonStrategy::new(aeon_cfg, hydra_cfg))
    }
    
    fn name(&self) -> &str {
        "Aeon"
    }
}

#[tokio::main]
async fn run_optimization() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    // 1. Setup Runner
    let runner = ParameterSweepRunner::new(
        "binance_btcusdt_fresh.wal",
        100000.0,
        0.5,
        Some(42),
    );
    
    // 2. Define Param Space
    let mut optimizer = TpeOptimizer::new(20); 
    
    // AEON Informational & Structural Parameters
    optimizer.add_param("bif_predictability_threshold", ParamRange::Float { min: 0.1, max: 0.4, step: 0.05 });
    optimizer.add_param("fti_trend_threshold", ParamRange::Float { min: 1.0, max: 4.0, step: 0.5 });
    optimizer.add_param("fti_mean_rev_threshold", ParamRange::Float { min: 0.5, max: 1.5, step: 0.1 });
    
    // HYDRA Tactical Parameters
    optimizer.add_param("learning_rate", ParamRange::Float { min: 0.01, max: 0.1, step: 0.01 });
    optimizer.add_param("zscore_action_threshold", ParamRange::Float { min: 0.5, max: 2.0, step: 0.2 });
    
    let factory = AeonFactory;
    
    // 3. TPE Optimization (Global Sweep)
    println!("Starting Global AEON TPE Optimization...");
    let results = runner.run_tpe(&factory, &optimizer).await;
    results.report();
    
    // 4. Walk-Forward Analysis (Robustness Check)
    println!("\nStarting 3-Fold Walk-Forward Analysis...");
    let wfa_config = WalkForwardConfig {
        n_folds: 3,
        train_ratio: 0.7,
        anchored: false,
    };
    
    let mut wfa_template = TpeOptimizer::new(10); 
    wfa_template.param_space = optimizer.param_space.clone();
    
    let wfa_results = runner.run_walk_forward(&factory, wfa_config, wfa_template).await?;
    wfa_results.report();
    
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let child = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            run_optimization()
        })?;

    child.join().expect("Failed to join thread")
}
