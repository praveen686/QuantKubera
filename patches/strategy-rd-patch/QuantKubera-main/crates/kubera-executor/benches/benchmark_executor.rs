use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kubera_executor::SimulatedExchange;
use kubera_core::EventBus;
use kubera_models::{MarketEvent, MarketPayload, OrderEvent, OrderPayload, OrderType, Side};
use tokio::runtime::Runtime;
use chrono::Utc;
use uuid::Uuid;

fn bench_simulated_exchange_order_matching(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = EventBus::new(1000);
    let mut exchange = SimulatedExchange::new(bus.clone(), 5.0, Some(42));

    c.bench_function("simulated_exchange_order_fill", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Submit a market order
                let order = OrderEvent {
                    order_id: Uuid::new_v4(),
                    timestamp: Utc::now(),
                    symbol: black_box("BTCUSDT".to_string()),
                    side: Side::Buy,
                    payload: OrderPayload::New {
                        symbol: "BTCUSDT".to_string(),
                        side: Side::Buy,
                        price: None,
                        quantity: 1.0,
                        order_type: OrderType::Market,
                    },
                };
                exchange.handle_order(order).await.unwrap();

                // Simulate a tick to trigger fill
                let tick = MarketEvent {
                    exchange_time: Utc::now(),
                    local_time: Utc::now(),
                    symbol: "BTCUSDT".to_string(),
                    payload: MarketPayload::Tick {
                        price: black_box(50000.0),
                        size: 1.0,
                        side: Side::Buy,
                    },
                };
                exchange.on_market_data(tick).await.unwrap();
            })
        });
    });
}

criterion_group!(benches, bench_simulated_exchange_order_matching);
criterion_main!(benches);
