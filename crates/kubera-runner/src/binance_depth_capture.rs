//! Binance depth capture (Spot) -> DepthEvent JSONL for L2 replay.
//!
//! ⚠️ DEPRECATED: This module uses JSON WebSocket depth stream.
//! For production/certified replay, use `binance_sbe_depth_capture` instead.
//!
//! Uses Binance diff depth stream: orderbook deltas.
//! Output format matches kubera-options::replay::DepthEvent (one JSON per line).
//!
//! ## Limitations (why this is deprecated)
//! - JSON parsing uses f64 intermediate, potential cross-platform drift
//! - No bootstrap protocol (REST snapshot + diff sync)
//! - Missing update IDs can cause gaps
//!
//! Notes:
//! - This is for DEBUGGING ONLY. Not suitable for certified replay.
//! - Binance Spot is 24x7 so you can generate depth replay packs any time.
//! - Uses scaled integers (mantissa + exponent) for deterministic replay.

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;

use kubera_options::replay::{DepthEvent, DepthLevel};
use kubera_models::IntegrityTier;

/// Binance depth diff event from WebSocket.
#[derive(Debug, serde::Deserialize)]
struct DepthDiffEvent {
    /// Event type
    #[serde(rename = "e")]
    event_type: String,
    /// Event time (ms)
    #[serde(rename = "E")]
    event_time_ms: i64,
    /// Symbol
    #[serde(rename = "s")]
    symbol: String,
    /// First update ID in event
    #[serde(rename = "U")]
    first_update_id: u64,
    /// Final update ID in event
    #[serde(rename = "u")]
    last_update_id: u64,
    /// Bids to be updated
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    /// Asks to be updated
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap())
}

/// Convert a price/qty string to scaled integer mantissa.
/// Returns (mantissa, exponent) where value = mantissa * 10^exponent.
fn parse_to_mantissa(s: &str, target_exponent: i8) -> Result<i64> {
    let val: f64 = s.parse().with_context(|| format!("parse f64: {}", s))?;
    let mantissa = (val / 10f64.powi(target_exponent as i32)).round() as i64;
    Ok(mantissa)
}

/// Capture depth diff stream to JSONL file.
///
/// # Arguments
/// * `symbol` - Trading pair (e.g., "BTCUSDT")
/// * `out_path` - Output file path
/// * `duration_secs` - Duration to capture
/// * `price_exponent` - Exponent for price (e.g., -2 for 2 decimal places)
/// * `qty_exponent` - Exponent for quantity (e.g., -8 for 8 decimal places)
pub async fn capture_depth_jsonl(
    symbol: &str,
    out_path: &Path,
    duration_secs: u64,
    price_exponent: i8,
    qty_exponent: i8,
) -> Result<usize> {
    let sym = symbol.to_lowercase();
    // Use 100ms update speed for reasonable granularity
    let url = format!("wss://stream.binance.com:9443/ws/{}@depth@100ms", sym);

    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .with_context(|| format!("connect websocket: {}", url))?;

    let (_write, mut read) = ws_stream.split();

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(out_path)
        .await
        .with_context(|| format!("open output: {:?}", out_path))?;

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(duration_secs);
    let mut count = 0;

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

        // Parse the depth diff event
        let ev: DepthDiffEvent = match serde_json::from_str(&txt) {
            Ok(e) => e,
            Err(_) => continue,
        };

        // Skip non-depth events
        if ev.event_type != "depthUpdate" {
            continue;
        }

        // Convert bids to DepthLevels with scaled integers
        let bids: Vec<DepthLevel> = ev
            .bids
            .iter()
            .filter_map(|[price_str, qty_str]| {
                let price = parse_to_mantissa(price_str, price_exponent).ok()?;
                let qty = parse_to_mantissa(qty_str, qty_exponent).ok()?;
                Some(DepthLevel { price, qty })
            })
            .collect();

        // Convert asks to DepthLevels with scaled integers
        let asks: Vec<DepthLevel> = ev
            .asks
            .iter()
            .filter_map(|[price_str, qty_str]| {
                let price = parse_to_mantissa(price_str, price_exponent).ok()?;
                let qty = parse_to_mantissa(qty_str, qty_exponent).ok()?;
                Some(DepthLevel { price, qty })
            })
            .collect();

        // Create DepthEvent
        // NOTE: This JSON capture is DEPRECATED. Use SBE capture for production.
        // Marked as NON_CERTIFIED - will be rejected by certified replay mode.
        let depth_event = DepthEvent {
            ts: ms_to_dt(ev.event_time_ms),
            tradingsymbol: ev.symbol,
            first_update_id: ev.first_update_id,
            last_update_id: ev.last_update_id,
            price_exponent,
            qty_exponent,
            bids,
            asks,
            is_snapshot: false, // JSON diffs are never snapshots
            integrity_tier: IntegrityTier::NonCertified,
            source: Some("binance_json_depth_DEPRECATED".to_string()),
        };

        // Write as JSONL
        let line = serde_json::to_string(&depth_event)?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        count += 1;
    }

    file.flush().await?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_to_mantissa() {
        // 90000.12 with exponent -2 = 9000012
        let mantissa = parse_to_mantissa("90000.12", -2).unwrap();
        assert_eq!(mantissa, 9000012);

        // 1.50000000 with exponent -8 = 150000000
        let mantissa = parse_to_mantissa("1.50000000", -8).unwrap();
        assert_eq!(mantissa, 150000000);

        // 0 quantity
        let mantissa = parse_to_mantissa("0.00000000", -8).unwrap();
        assert_eq!(mantissa, 0);
    }

    #[test]
    fn test_depth_event_serialization() {
        let event = DepthEvent {
            ts: Utc::now(),
            tradingsymbol: "BTCUSDT".to_string(),
            first_update_id: 100,
            last_update_id: 100,
            price_exponent: -2,
            qty_exponent: -8,
            bids: vec![DepthLevel { price: 9000012, qty: 150000000 }],
            asks: vec![DepthLevel { price: 9000100, qty: 200000000 }],
            is_snapshot: false,
            integrity_tier: IntegrityTier::NonCertified,
            source: Some("test".to_string()),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("BTCUSDT"));
        assert!(json.contains("9000012"));
        assert!(json.contains("NON_CERTIFIED"));
    }
}
