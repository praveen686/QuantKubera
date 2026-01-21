//! # Connector Interface Module
//!
//! Defines the unified abstractions for market data ingestion and execution.
//!
//! ## Description
//! Implements the core traits and mock providers for the QuantKubera ecosystem.
//! Following the Adapter pattern, this module decouples strategy logic from 
//! exchange-specific API nuances.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use async_trait::async_trait;

/// Unified interface for market data connectors (Binance, Zerodha, etc.)
#[async_trait]
pub trait MarketConnector: Send + Sync {
    /// Start the market data ingestion loop.
    ///
    /// # Returns
    /// An `anyhow::Result` which resolves when the connector stops or fails.
    async fn run(&self) -> anyhow::Result<()>;
    
    /// Signals the connector to stop ingestion gracefully.
    fn stop(&self);
    
    /// Provides the human-readable name of the venue.
    fn name(&self) -> &'static str;
}

/// Unified interface for execution venues (Simulated or Live).
#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    /// Provides the human-readable name of the execution venue.
    fn name(&self) -> &'static str;
}

/// Mock connector for integration testing and simulation.
pub struct MockConnector {
    name: &'static str,
}

impl MockConnector {
    /// Creates a new mock connector instance.
    ///
    /// # Parameters
    /// * `name` - The venue name to report.
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}

#[async_trait]
impl MarketConnector for MockConnector {
    async fn run(&self) -> anyhow::Result<()> {
        Ok(())
    }
    
    fn stop(&self) {}
    
    fn name(&self) -> &'static str {
        self.name
    }
}

/// Mock execution venue for testing and baseline benchmarking.
pub struct MockExecutionVenue {
    name: &'static str,
}

impl MockExecutionVenue {
    /// Creates a new mock execution venue instance.
    ///
    /// # Parameters
    /// * `name` - The venue name to report.
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}

#[async_trait]
impl ExecutionVenue for MockExecutionVenue {
    fn name(&self) -> &'static str {
        self.name
    }
}
