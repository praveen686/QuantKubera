//! Zerodha Quote Capture → QuoteEvent JSONL for KiteSim Replay.
//!
//! Captures real-time quotes from Zerodha Kite WebSocket and writes
//! to JSONL format compatible with KiteSim backtest runner.
//!
//! ## Usage
//! ```bash
//! cargo run --release --bin kubera-runner -- capture-zerodha \
//!     --symbols BANKNIFTY26JAN48000CE,BANKNIFTY26JAN48000PE \
//!     --duration-secs 300 \
//!     --out data/replay/BANKNIFTY/2026-01-22/quotes.jsonl
//! ```
//!
//! ## Prerequisites
//! - Zerodha credentials in `.env` (ZERODHA_USER_ID, ZERODHA_PASSWORD, etc.)
//! - Python sidecar for TOTP authentication

use anyhow::{Context, Result, bail};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::tungstenite::Message;
use byteorder::{BigEndian, ByteOrder};
use serde::Deserialize;

use kubera_options::replay::QuoteEvent;

const KITE_API_URL: &str = "https://api.kite.trade";

/// Authentication response from Python sidecar.
#[derive(Deserialize)]
struct AuthOutput {
    access_token: String,
    api_key: String,
}

/// Quote response from Kite API.
#[derive(Debug, Deserialize)]
struct KiteQuoteResponse {
    status: String,
    data: HashMap<String, serde_json::Value>,
}

/// Capture statistics.
#[derive(Debug, Default)]
pub struct CaptureStats {
    pub events_written: usize,
    pub symbols_active: usize,
}

impl std::fmt::Display for CaptureStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "events={}, symbols={}", self.events_written, self.symbols_active)
    }
}

/// Authenticate with Zerodha via Python sidecar.
fn authenticate() -> Result<(String, String)> {
    println!("Authenticating with Zerodha...");
    let output = Command::new("python3")
        .arg("crates/kubera-connectors/scripts/zerodha_auth.py")
        .output()
        .context("Failed to run zerodha_auth.py")?;

    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!("Zerodha authentication failed: {}", err);
    }

    let auth: AuthOutput = serde_json::from_slice(&output.stdout)
        .context("Failed to parse auth response")?;

    println!("✅ Authenticated with Zerodha");
    Ok((auth.api_key, auth.access_token))
}

/// Fetch instrument tokens from NFO instruments master.
async fn fetch_instrument_tokens(
    api_key: &str,
    access_token: &str,
    symbols: &[String],
) -> Result<Vec<(String, u32)>> {
    let client = reqwest::Client::new();

    // Fetch NFO instruments master
    let url = format!("{}/instruments/NFO", KITE_API_URL);
    let response = client.get(&url)
        .header("X-Kite-Version", "3")
        .header("Authorization", format!("token {}:{}", api_key, access_token))
        .send()
        .await?
        .text()
        .await?;

    let mut tokens = Vec::new();
    let symbols_upper: Vec<String> = symbols.iter().map(|s| s.to_uppercase()).collect();

    // Parse CSV: instrument_token,exchange_token,tradingsymbol,...
    for line in response.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 3 {
            let token_str = parts[0];
            let tradingsymbol = parts[2].to_uppercase();

            if symbols_upper.contains(&tradingsymbol) {
                if let Ok(token) = token_str.parse::<u32>() {
                    tokens.push((tradingsymbol, token));
                }
            }
        }
    }

    Ok(tokens)
}

/// Parse binary tick data from Kite WebSocket.
/// Returns (token, best_bid, best_ask, bid_qty, ask_qty) if valid.
///
/// Kite Binary Format (Full mode - 184 bytes):
/// - Offset 0-4: Token (uint32)
/// - Offset 4-8: LTP (int32, divide by 100)
/// - Offset 8-12: Last traded quantity
/// - Offset 12-16: Average traded price
/// - Offset 16-20: Volume
/// - Offset 20-24: Total buy quantity
/// - Offset 24-28: Total sell quantity
/// - Offset 28-32: Open price
/// - Offset 32-36: High price
/// - Offset 36-40: Low price
/// - Offset 40-44: Close price
/// - Offset 44-104: Buy depth (5 levels x 12 bytes each)
/// - Offset 104-164: Sell depth (5 levels x 12 bytes each)
///
/// Each depth level (12 bytes):
/// - Offset +0: Quantity (int32)
/// - Offset +4: Price (int32, divide by 100)
/// - Offset +8: Orders (int16) + padding (2 bytes)
fn parse_tick(data: &[u8]) -> Option<Vec<(u32, f64, f64, u32, u32)>> {
    if data.len() < 4 {
        return None;
    }

    let num_packets = BigEndian::read_i16(&data[0..2]) as usize;
    let mut ticks = Vec::new();
    let mut offset = 2;

    for _ in 0..num_packets {
        if offset + 2 > data.len() {
            break;
        }

        let packet_len = BigEndian::read_i16(&data[offset..offset + 2]) as usize;
        offset += 2;

        if offset + packet_len > data.len() || packet_len < 8 {
            break;
        }

        let packet = &data[offset..offset + packet_len];
        let token = BigEndian::read_u32(&packet[0..4]);
        let ltp = BigEndian::read_i32(&packet[4..8]) as f64 / 100.0;

        // Full mode (184 bytes) has depth data
        if packet_len >= 184 {
            let depth_start = 44;

            // Find best bid (highest price with qty > 0 from buy side)
            let mut best_bid_price = 0.0;
            let mut best_bid_qty = 0u32;
            for i in 0..5 {
                let level_offset = depth_start + (i * 12);
                let qty = BigEndian::read_i32(&packet[level_offset..level_offset + 4]);
                let price = BigEndian::read_i32(&packet[level_offset + 4..level_offset + 8]) as f64 / 100.0;
                if qty > 0 && price > 0.0 && price > best_bid_price {
                    best_bid_price = price;
                    best_bid_qty = qty as u32;
                }
            }

            // Find best ask (lowest price with qty > 0 from sell side)
            let mut best_ask_price = f64::MAX;
            let mut best_ask_qty = 0u32;
            for i in 0..5 {
                let level_offset = depth_start + 60 + (i * 12);
                let qty = BigEndian::read_i32(&packet[level_offset..level_offset + 4]);
                let price = BigEndian::read_i32(&packet[level_offset + 4..level_offset + 8]) as f64 / 100.0;
                if qty > 0 && price > 0.0 && price < best_ask_price {
                    best_ask_price = price;
                    best_ask_qty = qty as u32;
                }
            }

            // Validate: bid < ask and both are reasonable (within 50% of LTP)
            let valid_depth = best_bid_price > 0.0
                && best_ask_price < f64::MAX
                && best_bid_price < best_ask_price
                && best_bid_price > ltp * 0.5
                && best_ask_price < ltp * 1.5;

            if valid_depth {
                ticks.push((token, best_bid_price, best_ask_price, best_bid_qty, best_ask_qty));
            } else if ltp > 0.0 {
                // Fallback: use LTP with synthetic spread (0.1%) and synthetic quantity
                // Use 150 as synthetic qty (~10 lots for BANKNIFTY options)
                let spread = ltp * 0.001;
                let synthetic_qty = 150u32;
                ticks.push((token, ltp - spread, ltp + spread, synthetic_qty, synthetic_qty));
            }
        } else if packet_len >= 44 {
            // Quote mode - use LTP with synthetic spread
            if ltp > 0.0 {
                let spread = ltp * 0.001;
                let synthetic_qty = 150u32;
                ticks.push((token, ltp - spread, ltp + spread, synthetic_qty, synthetic_qty));
            }
        } else if packet_len >= 8 {
            // LTP mode - use LTP with synthetic spread
            if ltp > 0.0 {
                let spread = ltp * 0.001;
                let synthetic_qty = 150u32;
                ticks.push((token, ltp - spread, ltp + spread, synthetic_qty, synthetic_qty));
            }
        }

        offset += packet_len;
    }

    if ticks.is_empty() { None } else { Some(ticks) }
}

/// Capture Zerodha quotes to JSONL file.
pub async fn capture_zerodha_quotes(
    symbols: &[String],
    out_path: &Path,
    duration_secs: u64,
) -> Result<CaptureStats> {
    // Step 1: Authenticate
    let (api_key, access_token) = authenticate()?;

    // Step 2: Get instrument tokens
    println!("Fetching instrument tokens for {} symbols...", symbols.len());
    let tokens = fetch_instrument_tokens(&api_key, &access_token, symbols).await?;

    if tokens.is_empty() {
        bail!("No valid instrument tokens found for symbols: {:?}", symbols);
    }

    println!("Found {} instrument tokens:", tokens.len());
    for (sym, tok) in &tokens {
        println!("  {} -> {}", sym, tok);
    }

    // Build token -> symbol map
    let token_to_symbol: HashMap<u32, String> = tokens.iter()
        .map(|(s, t)| (*t, s.clone()))
        .collect();

    // Step 3: Connect to WebSocket
    let ws_url = format!(
        "wss://ws.kite.trade/?api_key={}&access_token={}",
        api_key, access_token
    );
    println!("Connecting to Kite WebSocket...");

    let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .context("Failed to connect to Kite WebSocket")?;

    println!("✅ Connected to Kite WebSocket");
    let (mut write, mut read) = ws_stream.split();

    // Subscribe to instruments in Full mode
    let token_list: Vec<u32> = tokens.iter().map(|(_, t)| *t).collect();
    let subscribe_msg = serde_json::json!({
        "a": "subscribe",
        "v": token_list
    });
    write.send(Message::Text(subscribe_msg.to_string())).await?;

    // Set mode to Full (for depth data)
    let mode_msg = serde_json::json!({
        "a": "mode",
        "v": ["full", token_list]
    });
    write.send(Message::Text(mode_msg.to_string())).await?;
    println!("Subscribed to {} instruments in Full mode", tokens.len());

    // Open output file
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(out_path)
        .await
        .context(format!("Failed to open output file: {:?}", out_path))?;

    let mut stats = CaptureStats {
        events_written: 0,
        symbols_active: tokens.len(),
    };

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(duration_secs);

    // Step 4: Process messages
    while tokio::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), read.next()).await;

        let item = match msg {
            Ok(Some(Ok(m))) => m,
            Ok(Some(Err(e))) => {
                eprintln!("WebSocket error: {}", e);
                continue;
            }
            Ok(None) => {
                println!("WebSocket closed");
                break;
            }
            Err(_) => continue, // Timeout, retry
        };

        match item {
            Message::Binary(data) => {
                if let Some(ticks) = parse_tick(&data) {
                    for (token, bid, ask, bid_qty, ask_qty) in ticks {
                        if let Some(symbol) = token_to_symbol.get(&token) {
                            let event = QuoteEvent {
                                ts: Utc::now(),
                                tradingsymbol: symbol.clone(),
                                bid,
                                ask,
                                bid_qty,
                                ask_qty,
                            };

                            let line = serde_json::to_string(&event)?;
                            file.write_all(line.as_bytes()).await?;
                            file.write_all(b"\n").await?;
                            stats.events_written += 1;

                            // Progress indicator every 100 events
                            if stats.events_written % 100 == 0 {
                                print!("\rCaptured {} events...", stats.events_written);
                                std::io::Write::flush(&mut std::io::stdout())?;
                            }
                        }
                    }
                }
            }
            Message::Text(text) => {
                // Usually subscription confirmation or errors
                if text.contains("error") {
                    eprintln!("Server message: {}", text);
                }
            }
            Message::Ping(p) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            _ => {}
        }
    }

    file.flush().await?;
    println!("\n✅ Capture complete");

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tick_empty() {
        assert!(parse_tick(&[]).is_none());
        assert!(parse_tick(&[0, 0]).is_none());
    }
}
