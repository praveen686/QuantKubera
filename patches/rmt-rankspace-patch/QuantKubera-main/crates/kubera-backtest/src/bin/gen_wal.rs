use kubera_core::wal::WalWriter;
use kubera_models::{MarketEvent, MarketPayload, Side};
use chrono::Utc;

fn main() -> anyhow::Result<()> {
    println!("Generating sample WAL for backtest testing...");
    
    let mut writer = WalWriter::new("trading.wal")?;
    
    // Generate 1000 tick events with a trending pattern for momentum strategy
    let mut price = 50000.0;
    
    for i in 0..1000 {
        // Create a trend: first 500 ticks go up, next 500 go down
        let change = if i < 500 { 10.0 } else { -10.0 };
        price += change + (rand::random::<f64>() - 0.5) * 5.0;
        
        let event = MarketEvent {
            exchange_time: Utc::now(),
            local_time: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            payload: MarketPayload::Tick {
                price,
                size: 1.0 + rand::random::<f64>() * 2.0,
                side: if change > 0.0 { Side::Buy } else { Side::Sell },
            },
        };
        
        writer.log_event(&event)?;
    }
    
    // Also generate some bar events every 100 ticks
    price = 50000.0;
    for i in 0..10 {
        let open = price;
        let high = price + 500.0;
        let low = price - 200.0;
        let close = if i < 5 { price + 400.0 } else { price - 400.0 };
        price = close;
        
        let bar = MarketEvent {
            exchange_time: Utc::now(),
            local_time: Utc::now(),
            symbol: "BTCUSDT".to_string(),
            payload: MarketPayload::Bar {
                open,
                high,
                low,
                close,
                volume: 100.0,
                interval_ms: 60000, // 1 minute
            },
        };
        
        writer.log_event(&bar)?;
    }
    
    writer.flush()?;
    println!("Generated trading.wal with 1000 ticks and 10 bars");
    
    Ok(())
}
