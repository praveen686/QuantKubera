//! # Strategy Lifecycle Hub
//!
//! Orchestrates the execution and monitoring of systematic trading strategies.
//!
//! ## Description
//! Defines the `Strategy` contract for signal generation and the `StrategyRunner` 
//! for event-driven execution. Implements panic isolation and multi-channel 
//! event processing.
//!
//! ## Architecture
//! - **Panic Isolation**: Uses `catch_unwind` to prevent faulty strategies from
//!   crashing the entire trading engine.
//! - **Event Routing**: Multiplexes market data, order updates, and risk events
//!   into the strategy lifecycle hooks.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use kubera_models::{MarketEvent, OrderEvent, SignalEvent, RiskEvent};
use crate::EventBus;
use std::sync::Arc;

/// Core interface for systematic trading logic.
///
/// # Lifecycle
/// 1. `on_start`: Initialization and bus discovery.
/// 2. `on_tick`/`on_bar`: Processing market dynamics.
/// 3. `on_order_update`/`on_fill`: Managing order status and lifecycle.
/// 4. `on_signal_timer`: Time-based signal evaluation.
/// 5. `on_risk_event`: Asynchronous risk violation handling.
/// 6. `on_stop`: Teardown and cleanup.
pub trait Strategy: Send + Sync {
    /// Initializes state when the runner activates.
    fn on_start(&mut self, bus: Arc<EventBus>);
    
    /// Logic for L1 tick-level events.
    fn on_tick(&mut self, event: &MarketEvent);
    
    /// Logic for L2 or OHLCV bar events.
    fn on_bar(&mut self, event: &MarketEvent);
    
    /// Logic specifically for execution fills.
    fn on_fill(&mut self, fill: &OrderEvent);
    
    /// Logic for general order status changes (e.g. Cancelled, Rejected).
    fn on_order_update(&mut self, _order: &OrderEvent) {}
    
    /// Periodic pulse for time-dependent alpha generation.
    fn on_signal_timer(&mut self, _elapsed_ms: u64) {}
    
    /// Out-of-band notification of risk engine violations.
    fn on_risk_event(&mut self, _event: &RiskEvent) {}
    
    /// Cleanup logic for graceful shutdown.
    fn on_stop(&mut self);
    
    /// Unique identifier for the strategy instance.
    fn name(&self) -> &str;
}

/// Execution container for strategy implementations.
///
/// # Safety
/// Implements `MAX_PANICS` thresholding to disable runaway strategies
/// without impacting the main system process.
pub struct StrategyRunner {
    /// Polymorphic entry point for strategy logic.
    strategy: Box<dyn Strategy>,
    /// Communication hub for event ingestion.
    bus: Arc<EventBus>,
}

impl StrategyRunner {
    /// Chains a strategy to a specific event bus for execution.
    pub fn new(strategy: Box<dyn Strategy>, bus: Arc<EventBus>) -> Self {
        Self { strategy, bus }
    }
    
    /// Executes the main event-processing loop.
    ///
    /// # Error Handling
    /// Returns an error if the underlying event receivers fail. Panic counts
    /// are logged but do not necessarily terminate the loop until the threshold is met.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("Starting strategy: {}", self.strategy.name());
        
        let bus = self.bus.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.strategy.on_start(bus);
        }));
        
        let mut market_rx = self.bus.subscribe_market();
        let mut order_update_rx = self.bus.subscribe_order_update();
        let mut risk_rx = self.bus.subscribe_risk();
        
        // 1-second pulse for temporal signal generation.
        let mut timer = tokio::time::interval(tokio::time::Duration::from_secs(1));
        let mut total_elapsed_ms = 0;

        let mut panic_count: u32 = 0;
        const MAX_PANICS: u32 = 5;
        
        loop {
            tokio::select! {
                Ok(event) = market_rx.recv() => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        match &event.payload {
                            kubera_models::MarketPayload::Tick { .. } => {
                                self.strategy.on_tick(&event);
                            }
                            kubera_models::MarketPayload::Bar { .. } => {
                                self.strategy.on_bar(&event);
                            }
                            _ => {}
                        }
                    }));
                    if let Err(_) = result { panic_count += 1; }
                }

                Ok(event) = order_update_rx.recv() => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if let kubera_models::OrderPayload::Update { status, .. } = &event.payload {
                            if *status == kubera_models::OrderStatus::Filled {
                                self.strategy.on_fill(&event);
                            }
                        }
                        self.strategy.on_order_update(&event);
                    }));
                    if let Err(_) = result { panic_count += 1; }
                }

                Ok(event) = risk_rx.recv() => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.strategy.on_risk_event(&event);
                    }));
                    if let Err(_) = result { panic_count += 1; }
                }

                _ = timer.tick() => {
                    total_elapsed_ms += 1000;
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.strategy.on_signal_timer(total_elapsed_ms);
                    }));
                    if let Err(_) = result { panic_count += 1; }
                }
            }
            
            if panic_count >= MAX_PANICS {
                tracing::error!("Strategy exceeded max panics, stopping");
                break;
            }
        }
        
        self.strategy.on_stop();
        Ok(())
    }
    
    /// Requests an immediate shutdown of the strategy.
    pub fn stop(&mut self) {
        tracing::info!("Stopping strategy: {}", self.strategy.name());
        self.strategy.on_stop();
    }
}

/// Baseline implementation of a momentum-based trading algorithm.
///
/// # Logic
/// Generates 'Buy' signals when momentum over a window is positive and 
/// 'Sell' signals when it turns negative, provided a position exists.
pub struct MomentumStrategy {
    name: String,
    lookback: usize,
    prices: Vec<f64>,
    bus: Option<Arc<EventBus>>,
    position: f64,
}

impl MomentumStrategy {
    /// Creates a new momentum strategy with specified lookback window.
    pub fn new(lookback: usize) -> Self {
        Self {
            name: "MomentumStrategy".to_string(),
            lookback,
            prices: Vec::new(),
            bus: None,
            position: 0.0,
        }
    }
    
    fn emit_signal(&self, event: &MarketEvent, side: kubera_models::Side, price: f64) {
        if let Some(bus) = &self.bus {
            let signal = SignalEvent {
                timestamp: event.exchange_time,
                strategy_id: self.name.clone(),
                symbol: event.symbol.clone(),
                side,
                price,
                quantity: 1.0,
                intent_id: None,
            };
            let _ = bus.publish_signal_sync(signal);
        }
    }
}

impl Strategy for MomentumStrategy {
    fn on_start(&mut self, bus: Arc<EventBus>) {
        tracing::info!("[{}] Starting with lookback={}", self.name, self.lookback);
        self.bus = Some(bus);
    }
    
    fn on_tick(&mut self, _event: &MarketEvent) {}
    
    fn on_bar(&mut self, event: &MarketEvent) {
        if let kubera_models::MarketPayload::Bar { close, .. } = &event.payload {
            self.prices.push(*close);
            if self.prices.len() > self.lookback {
                self.prices.remove(0);
            }
            
            if self.prices.len() == self.lookback {
                let momentum = self.prices.last().unwrap() - self.prices.first().unwrap();
                
                if momentum > 0.0 && self.position == 0.0 {
                    tracing::info!("[{}] BUY signal @ {}", self.name, close);
                    self.position = 1.0;
                    self.emit_signal(event, kubera_models::Side::Buy, *close);
                } else if momentum < 0.0 && self.position > 0.0 {
                    tracing::info!("[{}] SELL signal @ {}", self.name, close);
                    self.position = 0.0;
                    self.emit_signal(event, kubera_models::Side::Sell, *close);
                }
            }
        }
    }
    
    fn on_fill(&mut self, fill: &OrderEvent) {
        tracing::info!("[{}] Fill: {:?}", self.name, fill.order_id);
    }
    
    fn on_stop(&mut self) {
        tracing::info!("[{}] Stopped. Position: {}", self.name, self.position);
    }
    
    fn name(&self) -> &str { &self.name }
}
