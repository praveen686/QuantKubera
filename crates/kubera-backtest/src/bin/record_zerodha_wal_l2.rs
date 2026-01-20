//! Zerodha live recorder: KiteTicker (Python sidecar) -> WAL (JSON lines)
//!
//! Design goals (Phase Z0):
//! - No synthetic L2. All snapshots originate from Zerodha ticks (best-5 depth).
//! - Guaranteed arming point for replay: write an initial L2Snapshot as soon as
//!   the first depth tick is observed per symbol.
//! - Stable replay even if incremental updates are sparse: write periodic
//!   L2Snapshot every N seconds for any symbol that has a current book.
//!
//! This recorder intentionally uses a Python sidecar (kiteconnect) to avoid
//! re-implementing KiteTicker framing in Rust for Phase Z0.

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use kubera_core::wal::{UniverseSpecV1, WalWriter};
use kubera_models::{L2Level, L2Snapshot, MarketEvent, MarketPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
struct DepthLevelMsg {
    price: f64,
    quantity: f64,
}

#[derive(Debug, Deserialize)]
struct DepthMsg {
    buy: Vec<DepthLevelMsg>,
    sell: Vec<DepthLevelMsg>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InstrumentInfo {
    token: i64,
    lot_size: u32,
    exchange: String,
}

/// Metadata message sent at startup by the Python sidecar
#[derive(Debug, Deserialize)]
struct MetadataMsg {
    #[serde(rename = "type")]
    msg_type: String,
    instruments: HashMap<String, InstrumentInfo>,
}

/// Tick message for market data
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TickMsg {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    /// Unix epoch in ns (sidecar computes from exchange timestamp or local time).
    ts_ns: i64,
    instrument_token: i64,
    tradingsymbol: String,
    last_price: Option<f64>,
    depth: Option<DepthMsg>,
}

/// Generic message to detect type
#[derive(Debug, Deserialize)]
struct GenericMsg {
    #[serde(rename = "type")]
    msg_type: String,
}

fn parse_arg(flag: &str, args: &[String]) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn parse_arg_u64(flag: &str, args: &[String], default: u64) -> u64 {
    parse_arg(flag, args)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_arg_path(flag: &str, args: &[String], default: &str) -> PathBuf {
    parse_arg(flag, args)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn read_tokens(tokens_file: &Path) -> Result<Vec<i64>> {
    let raw = fs::read_to_string(tokens_file)
        .with_context(|| format!("failed to read tokens file: {}", tokens_file.display()))?;
    let mut tokens = Vec::new();
    for (idx, line) in raw.lines().enumerate() {
        let s = line.trim();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        let t = s.parse::<i64>()
            .with_context(|| format!("invalid token at line {}: '{}'", idx + 1, s))?;
        tokens.push(t);
    }
    if tokens.is_empty() {
        return Err(anyhow!("tokens file contains no instrument tokens"));
    }
    Ok(tokens)
}

fn read_universe_json(path: &Path) -> Result<UniverseSpecV1> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read universe json: {}", path.display()))?;
    let mut spec: UniverseSpecV1 = serde_json::from_str(&raw)
        .with_context(|| format!("invalid universe json: {}", path.display()))?;
    if spec.version.trim().is_empty() {
        spec.version = "v1".to_string();
    }
    if spec.venue.trim().is_empty() {
        spec.venue = "zerodha".to_string();
    }
    if spec.symbols.is_empty() {
        return Err(anyhow!("universe json has empty symbols"));
    }
    Ok(spec)
}

fn tokens_from_universe(spec: &UniverseSpecV1) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    for sym in &spec.symbols {
        let Some(v) = spec.instruments.get(sym) else {
            return Err(anyhow!("universe missing instruments entry for symbol: {sym}"));
        };
        let token = v
            .get("token")
            .and_then(|x| x.as_i64())
            .ok_or_else(|| anyhow!("instrument metadata missing token for symbol: {sym}"))?;
        out.push(token);
    }
    if out.is_empty() {
        return Err(anyhow!("universe produced empty token list"));
    }
    Ok(out)
}

fn depth_to_snapshot(depth: &DepthMsg, update_id: u64) -> L2Snapshot {
    // Zerodha depth is typically best-5.
    let bids = depth
        .buy
        .iter()
        .map(|l| L2Level {
            price: l.price,
            size: l.quantity,
        })
        .collect::<Vec<_>>();
    let asks = depth
        .sell
        .iter()
        .map(|l| L2Level {
            price: l.price,
            size: l.quantity,
        })
        .collect::<Vec<_>>();
    L2Snapshot {
        bids,
        asks,
        update_id,
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Required inputs
    let out_wal = parse_arg_path("--out", &args, "zerodha_live.wal");
    let seconds = parse_arg_u64("--seconds", &args, 600);
    let snapshot_every_s = parse_arg_u64("--snapshot-every", &args, 5);
    let tokens_file = parse_arg_path("--tokens-file", &args, "tokens.txt");
    let universe_json = parse_arg("--universe-json", &args).map(PathBuf::from);

    // Optional: override python executable (e.g. python3.11)
    let python = parse_arg("--python", &args).unwrap_or_else(|| "python3".to_string());

    // Sidecar script path (repo-relative)
    let sidecar = PathBuf::from("crates/kubera-connectors/scripts/zerodha_ticker_stream.py");
    if !sidecar.exists() {
        return Err(anyhow!(
            "missing sidecar script at {} (expected in repo)",
            sidecar.display()
        ));
    }

    // Prefer universe-json (canonical) -> derive tokens; fall back to tokens file.
    let universe_spec: Option<UniverseSpecV1> = match &universe_json {
        Some(p) => Some(read_universe_json(p)?),
        None => None,
    };

    let tokens = if let Some(spec) = &universe_spec {
        tokens_from_universe(spec)?
    } else {
        eprintln!("[record_zerodha_wal_l2] WARN: --universe-json not provided; falling back to --tokens-file. Consider generating a universe spec and embedding it in WAL for deterministic replay.");
        read_tokens(&tokens_file)?
    };
    eprintln!(
        "[record_zerodha_wal_l2] recording {} tokens for {}s; snapshots every {}s -> {}",
        tokens.len(),
        seconds,
        snapshot_every_s,
        out_wal.display()
    );

    // Spawn python sidecar.
    // The sidecar requires:
    // - KITE_API_KEY
    // - KITE_ACCESS_TOKEN
    // and will subscribe tokens in full mode.
    let mut child = Command::new(&python)
        .arg(&sidecar)
        .arg("--tokens")
        .arg(tokens.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(","))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to spawn python sidecar using '{}'", python))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture sidecar stdout"))?;
    let reader = BufReader::new(stdout);

    let mut wal = WalWriter::new(&out_wal)?;

    // latest books keyed by tradingsymbol.
    let mut latest: HashMap<String, L2Snapshot> = HashMap::new();
    // track which symbols have emitted their first snapshot (arming point)
    let mut armed: HashMap<String, bool> = HashMap::new();
    // lot sizes per symbol (from metadata)
    let mut lot_sizes: HashMap<String, u32> = HashMap::new();
    let mut update_id: u64 = 1;
    let mut metadata_received = false;

    // Persist universe spec upfront for deterministic replay.
    if let Some(spec) = &universe_spec {
        wal.write_meta("universe_spec_v1", spec)?;
    }

    let start = Instant::now();
    let mut last_periodic = Instant::now();

    for line in reader.lines() {
        if start.elapsed() > Duration::from_secs(seconds) {
            break;
        }

        let line = match line {
            Ok(l) => l,
            Err(e) => {
                return Err(anyhow!("failed reading sidecar stdout: {}", e));
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        // First, detect message type
        let generic: GenericMsg = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[record_zerodha_wal_l2] bad json (skipping): {} | {}", e, line);
                continue;
            }
        };

        // Handle metadata message (first line from sidecar)
        if generic.msg_type == "metadata" {
            let meta: MetadataMsg = serde_json::from_str(&line)?;

            // Validate msg_type field matches expected value
            if meta.msg_type != "metadata" {
                return Err(anyhow!(
                    "metadata message has unexpected type field: '{}' (expected 'metadata')",
                    meta.msg_type
                ));
            }

            eprintln!(
                "[record_zerodha_wal_l2] received metadata for {} instruments (msg_type={})",
                meta.instruments.len(),
                meta.msg_type
            );

            for (symbol, info) in &meta.instruments {
                lot_sizes.insert(symbol.clone(), info.lot_size);
                eprintln!(
                    "[record_zerodha_wal_l2] {} lot_size={} exchange={}",
                    symbol, info.lot_size, info.exchange
                );
            }
            metadata_received = true;

            // Write typed metadata to WAL (lot sizes + instrument info)
            wal.write_meta("lot_sizes", &lot_sizes)?;
            wal.write_meta("instrument_info", &meta.instruments)?;
            continue;
        }

        // Handle tick message
        if generic.msg_type != "tick" {
            eprintln!("[record_zerodha_wal_l2] unknown message type: {}", generic.msg_type);
            continue;
        }

        let msg: TickMsg = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[record_zerodha_wal_l2] bad tick json (skipping): {} | {}", e, line);
                continue;
            }
        };

        if !metadata_received {
            eprintln!("[record_zerodha_wal_l2] warning: received tick before metadata");
        }

        let now = Utc::now();
        let symbol = msg.tradingsymbol.clone();

        // Log tick LTP if present (useful for sanity & MTM even when depth missing)
        if let Some(px) = msg.last_price {
            let event = MarketEvent {
                exchange_time: now,
                local_time: now,
                symbol: symbol.clone(),
                payload: MarketPayload::Tick {
                    price: px,
                    size: 0.0,
                    side: kubera_models::Side::Buy,
                },
            };
            wal.log_event(&event)?;
        }

        // If depth present, update book and emit snapshot.
        if let Some(depth) = msg.depth {
            let snap = depth_to_snapshot(&depth, update_id);
            update_id = update_id.wrapping_add(1);
            latest.insert(symbol.clone(), snap.clone());

            // Initial arming snapshot: first depth tick for this symbol.
            let already_armed = *armed.get(&symbol).unwrap_or(&false);
            if !already_armed {
                let event = MarketEvent {
                    exchange_time: now,
                    local_time: now,
                    symbol: symbol.clone(),
                    payload: MarketPayload::L2Snapshot(snap.clone()),
                };
                wal.log_event(&event)?;
                armed.insert(symbol.clone(), true);
            }
        }

        // Periodic snapshots for all symbols with a book.
        if last_periodic.elapsed() >= Duration::from_secs(snapshot_every_s) {
            let now = Utc::now();
            for (sym, snap) in latest.iter() {
                let mut snap2 = snap.clone();
                snap2.update_id = update_id;
                update_id = update_id.wrapping_add(1);
                let event = MarketEvent {
                    exchange_time: now,
                    local_time: now,
                    symbol: sym.clone(),
                    payload: MarketPayload::L2Snapshot(snap2),
                };
                wal.log_event(&event)?;
            }
            wal.flush()?;
            last_periodic = Instant::now();
        }
    }

    // Ensure child is terminated.
    let _ = child.kill();
    let _ = child.wait();
    wal.flush()?;

    eprintln!("[record_zerodha_wal_l2] done: {}", out_wal.display());
    Ok(())
}
