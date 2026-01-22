//! Binance capture (Spot) -> QuoteEvent JSONL for KiteSim replay.
//!
//! Uses Binance bookTicker stream: best bid/ask updates.
//! Output format matches kubera-options::replay::QuoteEvent (one JSON per line).
//!
//! Notes:
//! - This is for TESTING and replay generation. No trading, no API keys required.
//! - Binance Spot is 24x7 so you can generate replay packs any time.

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;

use kubera_options::replay::{QuoteEvent, QuoteIntegrity};

#[derive(Debug, serde::Deserialize)]
struct BookTickerEvent {
    /// Event time (ms) - optional, not present in individual symbol streams
    #[serde(rename = "E")]
    event_time_ms: Option<i64>,
    /// Symbol
    #[serde(rename = "s")]
    symbol: String,
    /// Best bid price
    #[serde(rename = "b")]
    bid_price: String,
    /// Best bid qty
    #[serde(rename = "B")]
    bid_qty: String,
    /// Best ask price
    #[serde(rename = "a")]
    ask_price: String,
    /// Best ask qty
    #[serde(rename = "A")]
    ask_qty: String,
}

fn parse_f64(s: &str) -> Result<f64> {
    Ok(s.parse::<f64>().with_context(|| format!("parse f64: {}", s))?)
}

fn parse_u32_qty(s: &str) -> Result<u32> {
    // Binance quantities are decimal; bucket to u32 for L1 visibility.
    let x = parse_f64(s)?;
    Ok(x.max(0.0).round().min(u32::MAX as f64) as u32)
}

fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

pub async fn capture_book_ticker_jsonl(symbol: &str, out_path: &Path, duration_secs: u64) -> Result<()> {
    let sym = symbol.to_lowercase();
    let url = format!("wss://stream.binance.com:9443/ws/{}@bookTicker", sym);

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connect websocket: {}", url))?;

    let (_write, mut read) = ws_stream.split();

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out_path)
        .await
        .with_context(|| format!("open output: {:?}", out_path))?;

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(duration_secs);

    while tokio::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), read.next()).await;
        let item = match msg {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(_) => continue,
        };

        let msg = item?;
        if !msg.is_text() {
            continue;
        }
        let txt = msg.into_text()?;
        // Skip messages that don't match bookTicker format (e.g., subscription confirmations)
        let ev: BookTickerEvent = match serde_json::from_str(&txt) {
            Ok(e) => e,
            Err(_) => continue,
        };

        // Use event time if provided, otherwise use local time
        let ts = match ev.event_time_ms {
            Some(ms) => ms_to_dt(ms),
            None => Utc::now(),
        };

        let q = QuoteEvent {
            ts,
            tradingsymbol: ev.symbol,
            bid: parse_f64(&ev.bid_price)?,
            ask: parse_f64(&ev.ask_price)?,
            bid_qty: parse_u32_qty(&ev.bid_qty)?,
            ask_qty: parse_u32_qty(&ev.ask_qty)?,
            source: Some("BINANCE_BOOKTICKER".to_string()),
            integrity: Some(QuoteIntegrity::QuoteGradeD5),
            is_synthetic: Some(false),
        };

        let line = serde_json::to_string(&q)?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
    }

    file.flush().await?;
    Ok(())
}
