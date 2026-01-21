use kubera_core::{EventBus, Portfolio};
use kubera_models::{MarketEvent, MarketPayload, Side, SignalEvent, OrderEvent, OrderPayload};
use kubera_risk::{RiskEngine, RiskConfig};
use kubera_executor::{SimulatedExchange, CommissionModel};
use std::sync::{Arc, Mutex, atomic::AtomicBool};
use chrono::Utc;
use uuid::Uuid;

#[tokio::test]
async fn test_full_trading_lifecycle() {
    // 1. Setup
    let bus = EventBus::new(1024);
    let kill_switch = Arc::new(AtomicBool::new(false));
    let risk_config = RiskConfig {
        max_order_value_usd: 10000.0,
        max_notional_per_symbol_usd: 50000.0,
    };
    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(risk_config, kill_switch.clone())));
    let mut exchange = SimulatedExchange::new(bus.clone(), 5.0, CommissionModel::None, None); // 5bps slippage
    let mut portfolio = Portfolio::new();

    let symbol = "BTCUSDT".to_string();

    // 2. Mock Market Event (Trade)
    let _market_trade = MarketEvent {
        exchange_time: Utc::now(),
        local_time: Utc::now(),
        symbol: symbol.clone(),
        payload: MarketPayload::Trade {
            trade_id: 1,
            price: 50000.0,
            quantity: 1.0,
            is_buyer_maker: false,
        },
    };

    // 3. Strategy generates Signal
    let _signal = SignalEvent {
        timestamp: Utc::now(),
        symbol: symbol.clone(),
        side: Side::Buy,
        quantity: 0.1,
        price: 50005.0,
        strategy_id: "momentum_test".to_string(),
        intent_id: None,
        // HFT V2: Default book context for test
        decision_bid: 50005.0,
        decision_ask: 50005.0,
        decision_mid: 50005.0,
        spread_bps: 0.0,
        book_ts_ns: 0,
        expected_edge_bps: 0.0,
    };

    // 4. Convert Signal to Order
    let order_id = Uuid::new_v4();
    let order = OrderEvent {
        order_id,
        intent_id: None,
        timestamp: Utc::now(),
        symbol: symbol.clone(),
        side: Side::Buy,
        payload: OrderPayload::New {
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Some(50005.0),
            quantity: 0.1,
            order_type: kubera_models::OrderType::Limit,
        },
    };

    // 5. Risk Check
    let risk_ok = {
        let engine = risk_engine.lock().unwrap();
        engine.check_order(&order, 50000.0) // Pass current price
    };
    assert!(risk_ok.is_ok(), "Order should pass risk checks: {:?}", risk_ok.err());

    // 6. Submit to Exchange
    let _ = exchange.handle_order(order).await;

    // 7. Trigger Execution (Market move to cross limit)
    let cross_trade = MarketEvent {
        exchange_time: Utc::now(),
        local_time: Utc::now(),
        symbol: symbol.clone(),
        payload: MarketPayload::Tick {
            price: 50004.0, // Below limit price of 50005.0 for Buy
            size: 1.0,
            side: Side::Sell,
        },
    };

    // Subscribe to fills BEFORE pushing market data
    let mut fill_rx = bus.subscribe_fill();

    let _ = exchange.on_market_data(cross_trade).await;

    // 8. Capture Fill Event from Bus
    // Note: SimulatedExchange publishes fills to the bus
    // (Wait, checking SimulatedExchange... it publishes OrderUpdate with Filled status, not FillEvent yet?)
    // Actually, looking at SimulatedExchange, it publishes OrderUpdate.
    // Let's verify if it publishes FillEvent.
    let fill = fill_rx.recv().await.expect("Should receive a fill event");

    assert_eq!(fill.symbol, symbol);
    assert_eq!(fill.side, Side::Buy);
    assert!(fill.quantity > 0.0);

    // 9. Update Portfolio
    portfolio.apply_fill(&fill.symbol, fill.side, fill.quantity, fill.price, fill.commission);

    // 10. Verify State
    let position = portfolio.get_position(&symbol).unwrap();
    assert!(position.quantity > 0.0);
    assert_eq!(position.symbol, symbol);

    println!("Integration test successful: {} position is {}", symbol, position.quantity);
}
