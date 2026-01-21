use kubera_models::MarketPayload;
use kubera_data::VpinCalculator;
use kubera_core::EventBus;
use std::sync::Arc;

pub struct VpinStrategy {
    symbol: String,
    calculator: VpinCalculator,
    vpin_threshold: f64,
    bus: Arc<EventBus>,
}

impl VpinStrategy {
    pub fn new(symbol: String, bucket_size: f64, window_size: usize, vpin_threshold: f64, bus: Arc<EventBus>) -> Self {
        Self {
            symbol: symbol.clone(),
            calculator: VpinCalculator::new(symbol, bucket_size, window_size),
            vpin_threshold,
            bus,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut market_rx = self.bus.subscribe_market();

        while let Ok(event) = market_rx.recv().await {
            if event.symbol != self.symbol {
                continue;
            }

            if let MarketPayload::Tick { price, size, side } = event.payload {
                if let Some(vpin) = self.calculator.on_trade(price, size, side) {
                    // Signal if VPIN exceeds toxic threshold
                    if vpin > self.vpin_threshold {
                        tracing::warn!("TOXICITY DETECTED: Symbol {} VPIN {:.4} exceeds threshold {:.4}", 
                            self.symbol, vpin, self.vpin_threshold);
                        
                        // In a real strategy, we might exit positions or flip to follow the flow
                        // For demonstration, we'll just log and potentially send a signal
                    }
                }
            }
        }
        Ok(())
    }
}
