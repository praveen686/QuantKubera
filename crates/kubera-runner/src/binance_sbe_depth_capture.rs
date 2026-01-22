//! Binance SBE Depth Capture → DepthEvent JSONL for L2 Replay.
//!
//! Uses Binance SBE (Simple Binary Encoding) websocket stream for depth data.
//! This is the authoritative Phase-2A capture path - NO JSON depth allowed.
//!
//! ## SBE vs JSON
//! - SBE: `wss://stream-sbe.binance.com:9443/stream` with binary frames
//! - JSON (WRONG): `wss://stream.binance.com:9443/ws/{symbol}@depth@100ms`
//!
//! ## Bootstrap Protocol (Binance-mandated)
//! 1. Fetch REST depth snapshot to get initial book state + lastUpdateId
//! 2. Connect to SBE stream and buffer depth diffs
//! 3. Apply snapshot, then apply diffs where U <= lastUpdateId+1 <= u
//! 4. Continue with strict update ID sequencing
//!
//! ## Output Format
//! DepthEvent JSONL with scaled integer mantissas for deterministic replay.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use futures_util::{StreamExt, SinkExt};
use std::collections::VecDeque;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

use kubera_sbe::{SbeHeader, BinanceSbeDecoder, DepthUpdate, SBE_HEADER_SIZE};
use kubera_options::replay::{DepthEvent, DepthLevel};
use kubera_models::IntegrityTier;

/// REST depth snapshot response from Binance.
#[derive(Debug, serde::Deserialize)]
struct DepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

/// Bootstrap state for managing snapshot + diff sequencing.
#[derive(Debug)]
enum BootstrapState {
    /// Waiting for snapshot to be fetched.
    WaitingForSnapshot,
    /// Snapshot applied, processing diffs.
    Ready { last_update_id: u64 },
}

/// Pure string-to-mantissa parser (NO float conversion for determinism).
///
/// Parses decimal strings like "90000.12" directly to mantissa without
/// intermediate f64 conversion, avoiding cross-platform float drift.
///
/// # Algorithm
/// 1. Split on '.' to get integer and fractional parts
/// 2. Count decimal places and scale appropriately
/// 3. Return mantissa = value * 10^(-exponent)
///
/// # Examples
/// - "90000.12" with exponent -2 → 9000012
/// - "1.50000000" with exponent -8 → 150000000
fn parse_to_mantissa_pure(s: &str, exponent: i8) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty string");
    }

    // Handle negative numbers
    let (is_negative, s) = if s.starts_with('-') {
        (true, &s[1..])
    } else {
        (false, s)
    };

    // Split on decimal point
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 2 {
        bail!("invalid decimal format: {}", s);
    }

    let int_part = parts[0];
    let frac_part = if parts.len() == 2 { parts[1] } else { "" };

    // Target decimal places = -exponent (e.g., exponent=-2 means 2 decimals)
    let target_decimals = (-exponent) as usize;

    // Build the mantissa string: integer part + fractional part padded/truncated
    let mut mantissa_str = String::with_capacity(int_part.len() + target_decimals);
    mantissa_str.push_str(int_part);

    if frac_part.len() >= target_decimals {
        // Truncate fractional part (with rounding check)
        mantissa_str.push_str(&frac_part[..target_decimals]);

        // Check if we need to round up (look at next digit if exists)
        if frac_part.len() > target_decimals {
            let next_digit = frac_part.chars().nth(target_decimals).unwrap_or('0');
            if next_digit >= '5' {
                // Parse, increment, return
                let mut val: i64 = mantissa_str.parse()
                    .with_context(|| format!("parse mantissa: {}", mantissa_str))?;
                val += 1;
                return Ok(if is_negative { -val } else { val });
            }
        }
    } else {
        // Pad fractional part with zeros
        mantissa_str.push_str(frac_part);
        for _ in 0..(target_decimals - frac_part.len()) {
            mantissa_str.push('0');
        }
    }

    // Parse the combined string as i64
    let val: i64 = mantissa_str.parse()
        .with_context(|| format!("parse mantissa: {}", mantissa_str))?;

    Ok(if is_negative { -val } else { val })
}

/// Fetch REST depth snapshot for bootstrap.
async fn fetch_depth_snapshot(symbol: &str, limit: u32) -> Result<DepthSnapshot> {
    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit={}",
        symbol.to_uppercase(),
        limit
    );

    let resp = reqwest::get(&url)
        .await
        .with_context(|| format!("fetch depth snapshot: {}", url))?;

    if !resp.status().is_success() {
        bail!("Depth snapshot request failed: {}", resp.status());
    }

    let snapshot: DepthSnapshot = resp
        .json()
        .await
        .with_context(|| "parse depth snapshot JSON")?;

    Ok(snapshot)
}

/// Convert REST snapshot to DepthEvent for initial state.
/// Uses pure string-to-mantissa parsing for determinism.
fn snapshot_to_depth_event(
    snapshot: &DepthSnapshot,
    symbol: &str,
    price_exponent: i8,
    qty_exponent: i8,
) -> Result<DepthEvent> {
    let bids: Vec<DepthLevel> = snapshot
        .bids
        .iter()
        .filter_map(|[price_str, qty_str]| {
            let price = parse_to_mantissa_pure(price_str, price_exponent).ok()?;
            let qty = parse_to_mantissa_pure(qty_str, qty_exponent).ok()?;
            Some(DepthLevel { price, qty })
        })
        .collect();

    let asks: Vec<DepthLevel> = snapshot
        .asks
        .iter()
        .filter_map(|[price_str, qty_str]| {
            let price = parse_to_mantissa_pure(price_str, price_exponent).ok()?;
            let qty = parse_to_mantissa_pure(qty_str, qty_exponent).ok()?;
            Some(DepthLevel { price, qty })
        })
        .collect();

    Ok(DepthEvent {
        ts: Utc::now(),
        tradingsymbol: symbol.to_uppercase(),
        // For snapshot, first_update_id == last_update_id (single atomic state)
        first_update_id: snapshot.last_update_id,
        last_update_id: snapshot.last_update_id,
        price_exponent,
        qty_exponent,
        bids,
        asks,
        is_snapshot: true,
        integrity_tier: IntegrityTier::Certified,
        source: Some("binance_sbe_depth_capture".to_string()),
    })
}

/// Convert SBE DepthUpdate to DepthEvent.
/// Note: SBE decoder returns f64 from binary wire format - this is unavoidable.
/// The f64→mantissa conversion here is deterministic since the binary source is fixed.
fn sbe_depth_to_event(
    update: &DepthUpdate,
    symbol: &str,
    price_exponent: i8,
    qty_exponent: i8,
) -> DepthEvent {
    // SBE decoder returns f64; we need to convert back to mantissa
    // This is deterministic because the source is binary (not string parsing)
    let bids: Vec<DepthLevel> = update
        .bids
        .iter()
        .map(|l| DepthLevel {
            price: (l.price / 10f64.powi(price_exponent as i32)).round() as i64,
            qty: (l.size / 10f64.powi(qty_exponent as i32)).round() as i64,
        })
        .collect();

    let asks: Vec<DepthLevel> = update
        .asks
        .iter()
        .map(|l| DepthLevel {
            price: (l.price / 10f64.powi(price_exponent as i32)).round() as i64,
            qty: (l.size / 10f64.powi(qty_exponent as i32)).round() as i64,
        })
        .collect();

    DepthEvent {
        ts: BinanceSbeDecoder::timestamp_to_datetime(update.transact_time_us),
        tradingsymbol: symbol.to_uppercase(),
        first_update_id: update.first_update_id,
        last_update_id: update.last_update_id,
        price_exponent,
        qty_exponent,
        bids,
        asks,
        is_snapshot: false,
        integrity_tier: IntegrityTier::Certified,
        source: Some("binance_sbe_depth_capture".to_string()),
    }
}

/// Capture SBE depth stream to JSONL file with proper bootstrap.
///
/// # Bootstrap Protocol
/// 1. Fetch REST depth snapshot
/// 2. Connect to SBE stream
/// 3. Buffer diffs until we receive one where U <= lastUpdateId+1 <= u
/// 4. Write snapshot first, then apply diffs in sequence
///
/// # Arguments
/// * `symbol` - Trading pair (e.g., "BTCUSDT")
/// * `out_path` - Output file path
/// * `duration_secs` - Duration to capture
/// * `price_exponent` - Exponent for price (e.g., -2 for 2 decimal places)
/// * `qty_exponent` - Exponent for quantity (e.g., -8 for 8 decimal places)
/// * `api_key` - Binance API key (required for SBE stream)
pub async fn capture_sbe_depth_jsonl(
    symbol: &str,
    out_path: &Path,
    duration_secs: u64,
    price_exponent: i8,
    qty_exponent: i8,
    api_key: &str,
) -> Result<CaptureStats> {
    let sym_upper = symbol.to_uppercase();
    let sym_lower = symbol.to_lowercase();

    // CORRECT BOOTSTRAP ORDER (per Binance docs):
    // 1. Connect to WebSocket FIRST and start buffering
    // 2. THEN fetch REST snapshot
    // 3. Find sync point where first_update_id <= snapshot.lastUpdateId+1 <= last_update_id

    // Step 1: Connect to SBE stream FIRST
    let url_str = "wss://stream-sbe.binance.com:9443/stream";
    println!("Connecting to Binance SBE stream: {}", url_str);

    let url = Url::parse(url_str)?;
    let mut request = url.into_client_request()?;
    request.headers_mut().insert("X-MBX-APIKEY", api_key.parse()?);
    request.headers_mut().insert("Sec-WebSocket-Protocol", "binance-sbe".parse()?);

    let (ws_stream, _) = tokio_tungstenite::connect_async(request)
        .await
        .with_context(|| format!("connect SBE websocket: {}", url_str))?;

    println!("✅ Connected to SBE stream");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to depth stream (SBE format uses @depth, not @depth@100ms)
    let sub = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [format!("{}@depth", sym_lower)],
        "id": 1
    });

    println!("Subscribing to {}@depth...", sym_lower);
    write.send(Message::Text(sub.to_string())).await?;

    // Wait for subscription confirmation and buffer some initial messages
    let mut initial_buffer: VecDeque<DepthUpdate> = VecDeque::new();
    let mut subscription_confirmed = false;

    // Buffer messages for up to 2 seconds while waiting for subscription confirmation
    let buffer_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while tokio::time::Instant::now() < buffer_deadline {
        let msg = tokio::time::timeout(std::time::Duration::from_millis(500), read.next()).await;
        match msg {
            Ok(Some(Ok(Message::Text(text)))) => {
                if text.contains("\"result\":null") {
                    println!("Subscription confirmed");
                    subscription_confirmed = true;
                } else if text.contains("\"error\"") {
                    bail!("Subscription error: {}", text);
                }
            }
            Ok(Some(Ok(Message::Binary(bin)))) => {
                if bin.len() >= SBE_HEADER_SIZE {
                    if let Ok(header) = SbeHeader::decode(&bin[..SBE_HEADER_SIZE]) {
                        if header.template_id == 10003 {
                            if let Ok(update) = BinanceSbeDecoder::decode_depth_update(&header, &bin[SBE_HEADER_SIZE..]) {
                                initial_buffer.push_back(update);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        if subscription_confirmed && !initial_buffer.is_empty() {
            break;
        }
    }

    if !subscription_confirmed {
        bail!("Subscription not confirmed within 2 seconds");
    }

    println!("Buffered {} initial depth updates", initial_buffer.len());

    // Step 2: NOW fetch REST snapshot (after WebSocket is connected and buffering)
    println!("Fetching depth snapshot for {}...", sym_upper);
    let snapshot = fetch_depth_snapshot(&sym_upper, 1000).await?;
    let snapshot_last_id = snapshot.last_update_id;
    println!("Snapshot lastUpdateId: {}", snapshot_last_id);

    // Open output file
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(out_path)
        .await
        .with_context(|| format!("open output: {:?}", out_path))?;

    let mut stats = CaptureStats::default();
    let mut state = BootstrapState::WaitingForSnapshot;

    // Start with initial buffer
    let mut diff_buffer: VecDeque<DepthUpdate> = initial_buffer;

    // Step 3: Check buffered messages for sync point FIRST
    // Since we fetched snapshot after buffering, the sync point might already be in the buffer
    for update in diff_buffer.iter() {
        if update.first_update_id <= snapshot_last_id + 1
            && snapshot_last_id + 1 <= update.last_update_id
        {
            println!("Bootstrap sync found in buffer: U={}, u={}, snapshot={}",
                update.first_update_id, update.last_update_id, snapshot_last_id);

            // Write snapshot first
            let snapshot_event = snapshot_to_depth_event(
                &snapshot, &sym_upper, price_exponent, qty_exponent
            )?;
            let line = serde_json::to_string(&snapshot_event)?;
            file.write_all(line.as_bytes()).await?;
            file.write_all(b"\n").await?;
            stats.snapshot_written = true;
            stats.events_written += 1;

            // Apply buffered diffs that come after snapshot
            let mut last_update_id = snapshot_last_id;
            for buffered in diff_buffer.iter() {
                if buffered.first_update_id > snapshot_last_id {
                    let event = sbe_depth_to_event(
                        buffered, &sym_upper, price_exponent, qty_exponent
                    );
                    let line = serde_json::to_string(&event)?;
                    file.write_all(line.as_bytes()).await?;
                    file.write_all(b"\n").await?;
                    stats.events_written += 1;
                    last_update_id = buffered.last_update_id;
                }
            }

            state = BootstrapState::Ready { last_update_id };
            println!("Bootstrap complete from buffer, now capturing...");
            break;
        }
    }

    // Clear buffer if bootstrap completed (no longer needed)
    if matches!(state, BootstrapState::Ready { .. }) {
        diff_buffer.clear();
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(duration_secs);

    // Step 4: Process remaining messages
    while tokio::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), read.next()).await;
        let item = match msg {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(_) => continue, // Timeout, retry
        };

        let msg = item?;

        match msg {
            Message::Binary(bin) => {
                if bin.len() < SBE_HEADER_SIZE {
                    continue;
                }

                let header = match SbeHeader::decode(&bin[..SBE_HEADER_SIZE]) {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                // Only process depth updates (template 10003)
                if header.template_id != 10003 {
                    continue;
                }

                let update = match BinanceSbeDecoder::decode_depth_update(&header, &bin[SBE_HEADER_SIZE..]) {
                    Ok(u) => u,
                    Err(e) => {
                        eprintln!("SBE decode error: {}", e);
                        continue;
                    }
                };

                match &mut state {
                    BootstrapState::WaitingForSnapshot => {
                        // Buffer diffs until we find the right one
                        diff_buffer.push_back(update.clone());

                        // Check if this diff can be used to sync with snapshot
                        // Condition: U <= lastUpdateId+1 <= u
                        if update.first_update_id <= snapshot_last_id + 1
                            && snapshot_last_id + 1 <= update.last_update_id
                        {

                            // Write snapshot first
                            let snapshot_event = snapshot_to_depth_event(
                                &snapshot, &sym_upper, price_exponent, qty_exponent
                            )?;
                            let line = serde_json::to_string(&snapshot_event)?;
                            file.write_all(line.as_bytes()).await?;
                            file.write_all(b"\n").await?;
                            stats.snapshot_written = true;
                            stats.events_written += 1;

                            // Apply buffered diffs that come after snapshot
                            while let Some(buffered) = diff_buffer.pop_front() {
                                if buffered.first_update_id > snapshot_last_id {
                                    let event = sbe_depth_to_event(
                                        &buffered, &sym_upper, price_exponent, qty_exponent
                                    );
                                    let line = serde_json::to_string(&event)?;
                                    file.write_all(line.as_bytes()).await?;
                                    file.write_all(b"\n").await?;
                                    stats.events_written += 1;
                                }
                            }

                            state = BootstrapState::Ready {
                                last_update_id: update.last_update_id
                            };
                            println!("Bootstrap complete, now capturing...");
                        }

                        // Limit buffer size to avoid memory issues
                        while diff_buffer.len() > 1000 {
                            diff_buffer.pop_front();
                        }
                    }
                    BootstrapState::Ready { last_update_id } => {
                        // Strict sequencing check
                        if update.first_update_id != *last_update_id + 1 {
                            // Gap detected - this is a hard failure for replay
                            stats.gaps_detected += 1;
                            eprintln!(
                                "SEQUENCE GAP: expected {}, got {} (gap of {})",
                                *last_update_id + 1,
                                update.first_update_id,
                                update.first_update_id.saturating_sub(*last_update_id + 1)
                            );
                            // For capture, we log and continue (replay will fail on gaps)
                            // But we record the gap in stats
                        }

                        let event = sbe_depth_to_event(
                            &update, &sym_upper, price_exponent, qty_exponent
                        );
                        let line = serde_json::to_string(&event)?;
                        file.write_all(line.as_bytes()).await?;
                        file.write_all(b"\n").await?;
                        stats.events_written += 1;

                        *last_update_id = update.last_update_id;
                    }
                }
            }
            Message::Text(text) => {
                // JSON response (subscription confirmation, etc.)
                if text.contains("\"error\"") {
                    eprintln!("Server error: {}", text);
                }
            }
            Message::Ping(p) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            _ => {}
        }
    }

    file.flush().await?;

    if !stats.snapshot_written {
        bail!("Bootstrap failed: never synced with snapshot. \
            This may indicate a timing issue - try again or increase buffer time.");
    }

    Ok(stats)
}

/// Statistics from depth capture.
#[derive(Debug, Default)]
pub struct CaptureStats {
    pub snapshot_written: bool,
    pub events_written: usize,
    pub gaps_detected: usize,
}

impl std::fmt::Display for CaptureStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "snapshot={}, events={}, gaps={}",
            self.snapshot_written, self.events_written, self.gaps_detected
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_to_mantissa_pure() {
        // 90000.12 with exponent -2 = 9000012
        let mantissa = parse_to_mantissa_pure("90000.12", -2).unwrap();
        assert_eq!(mantissa, 9000012);

        // 1.50000000 with exponent -8 = 150000000
        let mantissa = parse_to_mantissa_pure("1.50000000", -8).unwrap();
        assert_eq!(mantissa, 150000000);

        // Test zero
        let mantissa = parse_to_mantissa_pure("0.00000000", -8).unwrap();
        assert_eq!(mantissa, 0);

        // Test rounding up
        let mantissa = parse_to_mantissa_pure("90000.125", -2).unwrap();
        assert_eq!(mantissa, 9000013); // rounds up from .125 → .13

        // Test rounding down
        let mantissa = parse_to_mantissa_pure("90000.124", -2).unwrap();
        assert_eq!(mantissa, 9000012); // rounds down from .124 → .12

        // Test negative
        let mantissa = parse_to_mantissa_pure("-100.50", -2).unwrap();
        assert_eq!(mantissa, -10050);
    }

    #[test]
    fn test_snapshot_to_depth_event() {
        let snapshot = DepthSnapshot {
            last_update_id: 12345,
            bids: vec![
                ["90000.00".to_string(), "1.5".to_string()],
            ],
            asks: vec![
                ["90001.00".to_string(), "2.0".to_string()],
            ],
        };

        let event = snapshot_to_depth_event(&snapshot, "BTCUSDT", -2, -8).unwrap();

        assert_eq!(event.first_update_id, 12345);
        assert_eq!(event.last_update_id, 12345);
        assert!(event.is_snapshot);
        assert_eq!(event.integrity_tier, IntegrityTier::Certified);
        assert_eq!(event.source, Some("binance_sbe_depth_capture".to_string()));
        assert_eq!(event.bids.len(), 1);
        assert_eq!(event.bids[0].price, 9000000); // 90000.00 * 10^2
        assert_eq!(event.bids[0].qty, 150000000); // 1.5 * 10^8
    }
}
