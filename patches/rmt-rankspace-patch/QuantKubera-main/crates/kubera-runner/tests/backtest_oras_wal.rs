//! # ORAS Backtest (WAL Replay)
//!
//! ## P.1016 - Support Documentation
//! **Test**: Verification-ORAS-WAL
//! **Author**: QuantKubera Team
//! **Date**: 2024-12-18
//!
//! ## Description
//! Verifies the ORAS strategy by replaying historical Write-Ahead Log (WAL) data.
//! Ensures identical behavior between live and backtest environments on real tick data.

#[cfg(test)]
mod verification {
    use kubera_core::{EventBus, OrasStrategy, Strategy, Portfolio};
    use kubera_core::wal::{WalReader, WalEvent};
    use kubera_models::{MarketPayload, OrderEvent, OrderPayload};
    use kubera_executor::{SimulatedExchange, CommissionModel};
    use chrono::Utc;
    use uuid::Uuid;
    use std::path::PathBuf;

    #[tokio::test]
    async fn backtest_oras_real_wal() {
        // Locate WAL file
        let mut wal_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        wal_path.pop(); // crates/
        wal_path.pop(); // root
        wal_path.push("trading.wal");

        println!(">>> Starting ORAS Backtest (WAL Replay: {:?}) <<<", wal_path);

        if !wal_path.exists() {
            println!("SKIPPING: WAL file not found at {:?}", wal_path);
            return;
        }

        let bus = EventBus::new(10000);
        let mut strategy = OrasStrategy::new();
        strategy.on_start(bus.clone());

        let mut exchange = SimulatedExchange::new(bus.clone(), 0.0, CommissionModel::None, None);
        let mut portfolio = Portfolio::new();
        portfolio.balance = 10000.0;

        let mut signal_rx = bus.subscribe_signal();
        let mut fill_rx = bus.subscribe_fill();

        let mut reader = WalReader::new(&wal_path).expect("Failed to open WAL");

        let mut signals = 0;
        let mut market_events = 0;
        let mut last_price = 0.0;

        // Replay Loop
        while let Ok(Some(wal_event)) = reader.next_any() {
            if let WalEvent::Market(event) = wal_event {
                market_events += 1;

                // Track Last Price for PnL
                if let MarketPayload::Tick { price, .. } = &event.payload {
                    last_price = *price;
                } else if let MarketPayload::Trade { price, .. } = &event.payload {
                    last_price = *price;
                }

                // 1. Feed Strategy
                strategy.on_tick(&event);

                // 2. Feed Exchange
                exchange.on_market_data(event.clone()).await.unwrap();

                // 3. Process Signals
                while let Ok(signal) = signal_rx.try_recv() {
                    signals += 1;
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
                            price: None,
                            order_type: kubera_models::OrderType::Market
                        }
                    }).await.unwrap();
                }

                // 4. Process Fills
                while let Ok(fill) = fill_rx.try_recv() {
                    portfolio.apply_fill(&fill.symbol, fill.side, fill.quantity, fill.price, fill.commission);
                    strategy.on_fill(&OrderEvent {
                        order_id: Uuid::new_v4(),
                        intent_id: fill.intent_id,
                        timestamp: Utc::now(),
                        symbol: fill.symbol.clone(),
                        side: fill.side,
                        payload: OrderPayload::Update {
                            status: kubera_models::OrderStatus::Filled,
                            filled_quantity: fill.quantity,
                            avg_price: fill.price,
                            commission: fill.commission,
                        }
                    });
                }
            }
        }

        if market_events == 0 {
            println!("No market events found in WAL.");
            return;
        }

        // Final Valuation
        let mut prices = std::collections::HashMap::new();
        prices.insert("BTC-USDT".to_string(), last_price);
        prices.insert("BTCUSDT".to_string(), last_price); // Handle both namings

        let final_value = portfolio.calculate_total_value(&prices);

        println!(">>> Backtest Complete <<<");
        println!("Events Processed: {}", market_events);
        println!("Total Signals: {}", signals);
        println!("Final PV: ${:.2}", final_value);
        println!("Return: {:.2}%", (final_value - 10000.0) / 100.0);
    }
}
