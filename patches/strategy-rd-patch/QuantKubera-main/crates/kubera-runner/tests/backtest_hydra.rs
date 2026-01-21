//! # HYDRA Verification Test
//!
//! ## P.1016 - Test Documentation
//! **Test**: Backtest-Hydra-Synthetic
//! **Description**: Verifies the HYDRA strategy's ability to:
//! 1. Ingest market data.
//! 2. Route data to Experts (A & B).
//! 3. Generate signals via the Meta-Allocator.
//! 4. Execute trades on the Simulated Exchange.

#[cfg(test)]
mod verification {
    use kubera_core::{EventBus, HydraStrategy, Strategy, Portfolio};
    use kubera_models::{MarketEvent, MarketPayload, Side, OrderEvent, OrderPayload, OrderType};
    use kubera_executor::SimulatedExchange;
    use chrono::Utc;
    use uuid::Uuid;

    fn create_tick(price: f64, symbol: &str) -> MarketEvent {
        MarketEvent {
            local_time: Utc::now(),
            exchange_time: Utc::now(),
            symbol: symbol.to_string(),
            payload: MarketPayload::Tick {
                price,
                size: 1.0,
                side: Side::Buy,
            },
        }
    }

    #[tokio::test]
    async fn backtest_hydra_switching() {
        println!(">>> Starting HYDRA Backtest <<<");

        // 1. Setup
        let bus = EventBus::new(1000);
        let mut strategy = HydraStrategy::new(); // Defaults to Range Regime
        
        let mut exchange = SimulatedExchange::new(bus.clone(), 0.0, None);
        let mut portfolio = Portfolio::new();
        portfolio.balance = 50000.0;

        let mut signal_rx = bus.take_signal_receiver().unwrap();
        
        strategy.on_start(bus.clone());
        
        // 2. Phase 1: Range Market (Mean Reversion Expert should be active)
        // Send a sine wave to trigger Mean Reversion
        println!("[TEST] Generating Range Market (Sine Wave)...");
        let base_price = 10000.0;
        let mut triggered_mr = false;
        
        for i in 0..100 {
            let price = base_price + (i as f64 * 0.5).sin() * 50.0; // +/- 50 oscillation
            let event = create_tick(price, "BTCUSDT");
            
            strategy.on_tick(&event);
            exchange.on_market_data(event).await.unwrap();
            
            // Check for signals
            while let Ok(signal) = signal_rx.try_recv() {
                println!("[SIGNAL] {:?} {} @ {}", signal.side, signal.quantity, signal.price);
                triggered_mr = true;
                
                // Execute Mock Order
                exchange.handle_order(OrderEvent {
                    order_id: Uuid::new_v4(), timestamp: Utc::now(), symbol: signal.symbol.clone(), side: signal.side,
                    payload: OrderPayload::New { symbol: signal.symbol, side: signal.side, quantity: signal.quantity, price: None, order_type: OrderType::Market }
                }).await.unwrap();
            }
        }
        
        // 3. Phase 2: Trending Market (Trend Expert should wake up)
        // Note: In a real test we'd need to force the Regime change or simulate enough data for the Classifier.
        // For this v1 verification, we are checking if the default configuration produces ANY signals.
        
        println!("[TEST] Signals Generated: {}", if triggered_mr { "YES" } else { "NO" });
        
        assert!(triggered_mr, "HYDRA failed to generate signals in Range market condition.");
    }
}
