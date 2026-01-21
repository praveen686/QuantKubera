
#[cfg(test)]
mod verification {
    use kubera_core::{EventBus, OrasStrategy, Strategy, Portfolio};
    use kubera_models::{MarketEvent, MarketPayload, Side, OrderEvent, OrderPayload};
    use kubera_executor::{SimulatedExchange, CommissionModel};
    use chrono::Utc;
    use uuid::Uuid;

    #[tokio::test]
    async fn backtest_oras_chaos() {
        println!(">>> Starting ORAS Backtest (Chaos Simulation) <<<");

        let bus = EventBus::new(10000);
        let mut strategy = OrasStrategy::new();
        strategy.on_start(bus.clone());

        let mut exchange = SimulatedExchange::new(bus.clone(), 0.0, CommissionModel::None, None);
        let mut portfolio = Portfolio::new();
        portfolio.balance = 10000.0; // Seed capital

        let mut signal_rx = bus.subscribe_signal();
        let mut fill_rx = bus.subscribe_fill();

        // Generate Synthetic Market Data (Trending -> Choppy -> Trending)
        let mut price = 50000.0;
        let mut pnl_curve = Vec::new();
        let mut signals = 0;

        for i in 0..2000 {
            // Regime switching logic
            let trend = if i < 500 { 1.5 } // Trend Up
                       else if i < 1000 { 0.0 } // Chop
                       else if i < 1500 { -1.5 } // Trend Down
                       else { 2.0 }; // Strong Recovery

            let noise = (i as f64 * 0.1).sin() * 10.0; // Deterministic "random"
            price += trend + noise;

            let tick = MarketEvent {
                exchange_time: Utc::now(),
                local_time: Utc::now(),
                symbol: "BTCUSDT".to_string(),
                payload: MarketPayload::Tick {
                    price,
                    size: 0.1,
                    side: if i % 2 == 0 { Side::Buy } else { Side::Sell }
                }
            };

            // 1. Feed Strategy
            strategy.on_tick(&tick);

            // 2. Feed Exchange
            exchange.on_market_data(tick.clone()).await.unwrap();

            // 3. Process Signals
            if let Ok(signal) = signal_rx.try_recv() {
                signals += 1;
                // Instant execution for backtest speed
                exchange.handle_order(OrderEvent {
                    order_id: Uuid::new_v4(),
                    intent_id: signal.intent_id,
                    timestamp: Utc::now(),
                    symbol: signal.symbol.clone(),
                    side: signal.side,
                    payload: OrderPayload::New {
                        symbol: signal.symbol,
                        side: signal.side,
                        quantity: signal.quantity,
                        price: None, // Market Order
                        order_type: kubera_models::OrderType::Market
                    }
                }).await.unwrap();
            }

            // 4. Process Fills
            while let Ok(fill) = fill_rx.try_recv() {
                portfolio.apply_fill(&fill.symbol, fill.side, fill.quantity, fill.price, fill.commission);
                strategy.on_fill(&OrderEvent { // Feedback loop
                    order_id: Uuid::new_v4(),
                    intent_id: fill.intent_id,
                    timestamp: Utc::now(),
                    symbol: "BTCUSDT".into(),
                    side: fill.side,
                    payload: OrderPayload::Update {
                        status: kubera_models::OrderStatus::Filled,
                        filled_quantity: fill.quantity,
                        avg_price: fill.price,
                        commission: fill.commission,
                    }
                });
            }

            if i % 100 == 0 {
                let pv = portfolio.calculate_total_value(&std::collections::HashMap::from([("BTCUSDT".to_string(), price)]));
                pnl_curve.push(pv);
            }
        }

        let final_value = portfolio.calculate_total_value(&std::collections::HashMap::from([("BTCUSDT".to_string(), price)]));
        println!("Final PV: ${:.2}", final_value);
        println!("Total Signals: {}", signals);
        println!("Return: {:.2}%", (final_value - 10000.0) / 100.0);

        assert!(signals > 0, "Strategy should have traded");
        assert!(final_value > 9000.0, "Strategy shouldn't have blown up (Chaos test)");
    }
}
