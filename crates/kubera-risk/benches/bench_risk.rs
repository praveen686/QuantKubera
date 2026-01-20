//! # Risk Engine Performance Benchmarks
//!
//! Measures the latency of pre-trade risk validation logic.
//!
//! ## Description
//! Uses Criterion to profile `RiskEngine::check_order` under high-frequency
//! simulation constraints. Goals are sub-nanosecond overhead for hot path.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kubera_risk::{RiskEngine, RiskConfig};
use kubera_models::{OrderEvent, OrderPayload, Side, OrderType};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use chrono::Utc;
use uuid::Uuid;

fn bench_risk_check(c: &mut Criterion) {
    let config = RiskConfig {
        max_order_value_usd: 10000.0,
        max_notional_per_symbol_usd: 50000.0,
    };
    let kill_switch = Arc::new(AtomicBool::new(false));
    let engine = RiskEngine::new(config, kill_switch);
    
    let order = OrderEvent {
        order_id: Uuid::new_v4(),
        intent_id: None,
        timestamp: Utc::now(),
        symbol: "BTCUSDT".to_string(),
        side: Side::Buy,
        payload: OrderPayload::New {
            symbol: "BTCUSDT".to_string(),
            side: Side::Buy,
            price: Some(50000.0),
            quantity: 0.1,
            order_type: OrderType::Limit,
        },
    };
    
    c.bench_function("risk_engine_check_order", |b| {
        b.iter(|| {
            let _ = black_box(engine.check_order(&order, 50000.0));
        });
    });
}

criterion_group!(benches, bench_risk_check);
criterion_main!(benches);
