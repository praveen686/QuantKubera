use kubera_core::wal::WalWriter;
use kubera_models::{MarketEvent, MarketPayload};
use chrono::{Utc, TimeZone, DateTime};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct AggTrade {
    #[serde(rename = "a")]
    trade_id: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    timestamp: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let symbol = "BTCUSDT";
    let hours = 2;
    let output_file = "binance_btcusdt_fresh.wal";

    println!("Fetching last {} hours of aggTrades for {}...", hours, symbol);

    let end_time = Utc::now().timestamp_millis();
    let start_time = end_time - (hours * 3600 * 1000);

    let client = reqwest::Client::new();
    let mut current_start = start_time;
    let mut writer = WalWriter::new(output_file)?;
    let mut total_trades = 0;

    let limit = 1000;

    while current_start < end_time {
        let url = format!(
            "https://api.binance.com/api/v3/aggTrades?symbol={}&startTime={}&endTime={}&limit={}",
            symbol, current_start, end_time, limit
        );

        let response = client.get(&url).send().await?.error_for_status()?;
        let trades: Vec<AggTrade> = response.json().await?;

        if trades.is_empty() {
            break;
        }

        for trade in &trades {
            let exchange_time = Utc.timestamp_millis_opt(trade.timestamp).unwrap();
            
            let event = MarketEvent {
                exchange_time,
                local_time: Utc::now(),
                symbol: symbol.to_string(),
                payload: MarketPayload::Trade {
                    trade_id: trade.trade_id,
                    price: trade.price.parse()?,
                    quantity: trade.quantity.parse()?,
                    is_buyer_maker: trade.is_buyer_maker,
                },
            };
            writer.log_event(&event)?;
        }

        total_trades += trades.len();
        println!("Fetched {} trades, current timestamp: {}", total_trades, trades.last().unwrap().timestamp);
        
        current_start = trades.last().unwrap().timestamp + 1;
        
        // Rate limiting
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    writer.flush()?;
    println!("Successfully saved {} trades to {}", total_trades, output_file);

    Ok(())
}
