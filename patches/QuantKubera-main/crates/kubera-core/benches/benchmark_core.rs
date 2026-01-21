use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kubera_core::EventBus;
use kubera_models::{MarketEvent, MarketPayload, Side};
use tokio::runtime::Runtime;
use chrono::Utc;

fn bench_event_bus(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let bus = EventBus::new(1000);
    
    c.bench_function("event_bus_publish_market", |b| {
        b.iter(|| {
            rt.block_on(async {
                let event = MarketEvent {
                    exchange_time: Utc::now(),
                    local_time: Utc::now(),
                    symbol: black_box("BTCUSDT".to_string()),
                    payload: MarketPayload::Tick {
                        price: 50000.0,
                        size: 1.0,
                        side: Side::Buy,
                    },
                };
                bus.publish_market(event).await.unwrap();
            })
        });
    });
}

fn bench_sbe_decoding(c: &mut Criterion) {
    use kubera_sbe::{SbeHeader, BinanceSbeDecoder};
    
    // Example Binance SBE Trade message (Template ID 10000)
    // Header (8 bytes) + Body
    let mut trade_bin = vec![0u8; 64];
    trade_bin[2..4].copy_from_slice(&10000u16.to_le_bytes()); // Template ID
    // Minimal data for decoder to not panic
    
    c.bench_function("binance_sbe_trade_decode", |b| {
        b.iter(|| {
            let header = SbeHeader::decode(&trade_bin[0..8]).unwrap();
            let _ = black_box(BinanceSbeDecoder::decode_trade(&header, &trade_bin[8..]));
        });
    });
}

fn bench_e2e_tick_to_decision(c: &mut Criterion) {
    use kubera_core::MomentumStrategy;
    use kubera_core::Strategy;
    use std::sync::Arc;
    
    let rt = Runtime::new().unwrap();
    let bus = Arc::new(EventBus::new(1000));
    
    c.bench_function("e2e_tick_to_strategy_decision", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Simulate tick arrival
                let tick = MarketEvent {
                    exchange_time: Utc::now(),
                    local_time: Utc::now(),
                    symbol: black_box("BTCUSDT".to_string()),
                    payload: MarketPayload::Tick {
                        price: 50000.0,
                        size: 1.0,
                        side: Side::Buy,
                    },
                };
                
                // Publish to bus
                bus.publish_market(tick.clone()).await.unwrap();
                
                // Strategy processes tick (simulated)
                let mut strategy = MomentumStrategy::new(5);
                strategy.on_tick(&tick);
            })
        });
    });
}

criterion_group!(benches, bench_event_bus, bench_sbe_decoding, bench_e2e_tick_to_decision);
criterion_main!(benches);
