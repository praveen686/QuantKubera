//! # Execution Mode Definitions
//!
//! Environment configuration for the trading engine lifecycle.
//!
//! ## Description
//! Defines the three primary operational modes for the QuantKubera system,
//! enabling seamless transition between historical simulation, real-time 
//! paper trading, and production live trading.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use serde::{Deserialize, Serialize};

/// Operational environment for strategy execution and fill simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Replay from Write-Ahead Logs (WAL) with deterministic fill matching.
    Backtest,
    /// Real-time data processing with zero-risk simulated order fulfillment.
    Paper,
    /// Production environment with actual exchange connectivity and fund commitment.
    Live,
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::Paper
    }
}

impl std::fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backtest => write!(f, "BACKTEST"),
            Self::Paper => write!(f, "PAPER"),
            Self::Live => write!(f, "LIVE"),
        }
    }
}

/// Composite configuration specifying the execution target and environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeConfig {
    pub mode: ExecutionMode,
    /// The specific exchange or venue identifier (e.g., "binance", "kite").
    pub venue: String,
    pub symbol: String,
    /// Path to the historical data file when in `Backtest` mode.
    #[serde(default)]
    pub wal_path: Option<String>,
}

impl Default for ModeConfig {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Paper,
            venue: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            wal_path: None,
        }
    }
}

impl ModeConfig {
    /// Factory for creating backtest environment configurations.
    pub fn backtest(wal_path: &str, symbol: &str) -> Self {
        Self {
            mode: ExecutionMode::Backtest,
            venue: "replay".to_string(),
            symbol: symbol.to_string(),
            wal_path: Some(wal_path.to_string()),
        }
    }
    
    /// Factory for creating paper trading environment configurations.
    pub fn paper(venue: &str, symbol: &str) -> Self {
        Self {
            mode: ExecutionMode::Paper,
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            wal_path: None,
        }
    }
    
    /// Factory for creating production live environment configurations.
    pub fn live(venue: &str, symbol: &str) -> Self {
        Self {
            mode: ExecutionMode::Live,
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            wal_path: None,
        }
    }
}
