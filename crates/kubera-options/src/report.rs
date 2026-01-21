
//! Backtest reporting utilities (offline)

use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::path::Path;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FillMetrics {
    pub orders_total: u64,
    pub legs_total: u64,
    pub legs_filled: u64,
    pub legs_partially_filled: u64,
    pub legs_rejected: u64,
    pub legs_cancelled: u64,
    pub rollbacks: u64,
    pub timeouts: u64,
    pub hedges_attempted: u64,
    pub hedges_filled: u64,
    pub slippage_bps_p50: f64,
    pub slippage_bps_p90: f64,
    pub slippage_bps_p99: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub created_at: DateTime<Utc>,
    pub engine: String,
    pub venue: String,
    pub dataset: String,
    pub fill: FillMetrics,
    pub notes: Vec<String>,
}

impl BacktestReport {
    pub fn write(&self, dir: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("report.json"), serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    /// Alias for write() for CLI compatibility.
    pub fn write_json(&self, dir: &Path) -> anyhow::Result<()> {
        self.write(dir)
    }
}
