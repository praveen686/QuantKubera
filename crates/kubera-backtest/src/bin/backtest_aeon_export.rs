//! # AEON Backtest with Full Parquet Export
//!
//! Runs the AEON strategy backtest and exports the FULL equity curve:
//! - Equity curve (tick-by-tick equity values)
//! - Trades (from backtest results)
//!
//! Output files are compatible with VectorBT Pro for visualization.

use kubera_backtest::BacktestEngine;
use kubera_core::{
    Strategy, AeonStrategy,
    aeon::AeonConfig,
    hydra::HydraConfig,
    parquet_export::{ParquetExporter, EquityPoint},
};
use tracing::info;
use std::fs;

fn load_config() -> (AeonConfig, HydraConfig) {
    let config_str = fs::read_to_string("backtest.toml")
        .or_else(|_| fs::read_to_string("../../backtest.toml"))
        .unwrap_or_default();
    
    if config_str.is_empty() {
        return (AeonConfig::default(), HydraConfig::default());
    }
    
    let toml_value: toml::Value = match config_str.parse() {
        Ok(v) => v,
        Err(_) => return (AeonConfig::default(), HydraConfig::default()),
    };
    
    let aeon_cfg = if let Some(aeon) = toml_value.get("strategy").and_then(|s| s.get("aeon")) {
        AeonConfig {
            bif_predictability_threshold: aeon.get("bif_predictability_threshold")
                .and_then(|v| v.as_float()).unwrap_or(0.38),
            fti_trend_threshold: aeon.get("fti_trend_threshold")
                .and_then(|v| v.as_float()).unwrap_or(3.56),
            fti_mean_rev_threshold: aeon.get("fti_mean_rev_threshold")
                .and_then(|v| v.as_float()).unwrap_or(0.80),
            ..Default::default()
        }
    } else {
        AeonConfig::default()
    };
    
    let hydra_cfg = if let Some(hydra) = toml_value.get("strategy").and_then(|s| s.get("hydra")) {
        HydraConfig {
            learning_rate: hydra.get("learning_rate")
                .and_then(|v| v.as_float()).unwrap_or(0.033),
            zscore_action_threshold: hydra.get("zscore_action_threshold")
                .and_then(|v| v.as_float()).unwrap_or(1.75),
            ..Default::default()
        }
    } else {
        HydraConfig::default()
    };
    
    (aeon_cfg, hydra_cfg)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    
    let wal_path = std::env::args().nth(1).unwrap_or_else(|| "binance_btcusdt_fresh.wal".to_string());
    let output_dir = std::env::args().nth(2).unwrap_or_else(|| ".".to_string());
    
    info!("═══════════════════════════════════════════════════════════════");
    info!("   AEON Backtest with Full Parquet Export");
    info!("═══════════════════════════════════════════════════════════════");
    info!("WAL File:   {}", wal_path);
    info!("Output Dir: {}", output_dir);
    
    let (aeon_cfg, hydra_cfg) = load_config();
    info!("BIF Threshold: {:.2}", aeon_cfg.bif_predictability_threshold);
    info!("FTI Trend:     {:.2}", aeon_cfg.fti_trend_threshold);
    info!("Learning Rate: {:.3}", hydra_cfg.learning_rate);
    
    // Create strategy
    let strategy: Box<dyn Strategy> = Box::new(AeonStrategy::new(aeon_cfg, hydra_cfg));
    
    // Create backtest engine
    let mut engine = BacktestEngine::new(
        &wal_path,
        100000.0,
        Some(42),
        0.5,
    );
    
    // Run backtest
    info!("Starting backtest replay...");
    let results = engine.run(strategy).await?;
    
    // Create parquet exporter
    let mut exporter = ParquetExporter::new(&output_dir);
    
    // Export FULL equity curve from BacktestResults
    info!("Exporting {} equity points to Parquet...", results.equity_curve.len());
    
    let initial_equity = 100000.0;
    for (i, equity) in results.equity_curve.iter().enumerate() {
        let realized_pnl = *equity - initial_equity;
        exporter.record_equity(EquityPoint {
            timestamp_ms: i as i64,  // Use index as timestamp (tick number)
            price: 0.0,              // Price not available in equity_curve
            equity: *equity,
            position: 0.0,           // Position not tracked per-tick
            unrealized_pnl: 0.0,
            realized_pnl,
        });
    }
    
    // Export to Parquet
    exporter.export_equity("aeon_equity.parquet")?;
    
    // Report
    info!("═══════════════════════════════════════════════════════════════");
    info!("   BACKTEST COMPLETE");
    info!("═══════════════════════════════════════════════════════════════");
    results.report();
    
    info!("");
    info!("Parquet export: {}/aeon_equity.parquet", output_dir);
    info!("  - {} equity points exported", results.equity_curve.len());
    info!("");
    info!("Load in Python:");
    info!("  import pandas as pd");
    info!("  equity = pd.read_parquet('aeon_equity.parquet')");
    info!("  equity.plot(x='timestamp_ms', y='equity')");
    
    Ok(())
}
