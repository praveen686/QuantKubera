//! KiteSim Offline Backtest Runner
//!
//! Provides a CLI-friendly entrypoint:
//! - load replay events (JSONL of QuoteEvent)
//! - load orders (JSON) or intents (JSON with timestamps)
//! - execute sequentially through MultiLegCoordinator
//! - emit report.json, fills.jsonl, pnl.json

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use kubera_options::execution::{LegSide, LegStatus, MultiLegOrder};
use kubera_options::kitesim::{AtomicExecPolicy, KiteSim, KiteSimConfig, MultiLegCoordinator, SimExecutionMode};
use kubera_options::replay::{QuoteEvent, DepthEvent, ReplayEvent, ReplayFeed};
use kubera_options::report::{BacktestReport, FillMetrics};
use kubera_options::specs::SpecStore;

use crate::binance_exchange_info::fetch_spot_specs;
use crate::order_io::OrderFile;

/// Scheduled order intent with timestamp
#[derive(Debug, Clone, serde::Deserialize)]
pub struct OrderIntent {
    /// RFC3339 timestamp (UTC) at which the order should be placed
    pub ts: String,
    pub order: MultiLegOrder,
}

/// File format for scheduled intents
#[derive(Debug, Clone, serde::Deserialize)]
pub struct OrderIntentFile {
    pub strategy_name: String,
    pub intents: Vec<OrderIntent>,
}

pub struct KiteSimCliConfig {
    pub venue: String,
    pub qty_scale: u32,
    pub strategy_name: String,
    pub replay_path: String,
    pub orders_path: String,
    pub intents_path: Option<String>,
    /// Path to depth replay file (DepthEvent JSONL). If set, uses L2Book mode.
    pub depth_path: Option<String>,
    pub out_dir: String,
    pub timeout_ms: i64,
    pub latency_ms: i64,
    pub slippage_bps: f64,
    pub adverse_bps: f64,
    pub stale_quote_ms: i64,
    pub hedge_on_failure: bool,
    /// If true, reject any depth events with NON_CERTIFIED integrity tier.
    /// Use this for production backtests to ensure only SBE-captured data is used.
    pub certified: bool,
}

fn parse_rfc3339_utc(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}

/// Load JSONL quotes. Each line must be a QuoteEvent JSON object.
pub fn load_quotes_jsonl(path: &Path) -> Result<Vec<ReplayEvent>> {
    let f = File::open(path).with_context(|| format!("open replay file: {:?}", path))?;
    let br = BufReader::new(f);
    let mut out = Vec::new();
    for (i, line) in br.lines().enumerate() {
        let line = line.with_context(|| format!("read line {}", i + 1))?;
        if line.trim().is_empty() { continue; }
        let q: QuoteEvent = serde_json::from_str(&line)
            .with_context(|| format!("parse QuoteEvent JSON on line {}", i + 1))?;
        out.push(ReplayEvent::Quote(q));
    }
    Ok(out)
}

/// Load JSONL depth events. Each line must be a DepthEvent JSON object.
/// If `certified` is true, rejects any event with NON_CERTIFIED integrity tier.
pub fn load_depth_jsonl(path: &Path, certified: bool) -> Result<Vec<ReplayEvent>> {
    let f = File::open(path).with_context(|| format!("open depth file: {:?}", path))?;
    let br = BufReader::new(f);
    let mut out = Vec::new();
    let mut non_certified_count = 0;
    for (i, line) in br.lines().enumerate() {
        let line = line.with_context(|| format!("read line {}", i + 1))?;
        if line.trim().is_empty() { continue; }
        let d: DepthEvent = serde_json::from_str(&line)
            .with_context(|| format!("parse DepthEvent JSON on line {}", i + 1))?;

        // Check integrity tier in certified mode
        if certified && !d.integrity_tier.is_certified() {
            non_certified_count += 1;
            if non_certified_count == 1 {
                // Log source on first encounter
                let source = d.source.as_deref().unwrap_or("unknown");
                anyhow::bail!(
                    "CERTIFIED MODE: Rejected NON_CERTIFIED depth event on line {} (source: {}). \
                    Use SBE capture (capture-sbe-depth) for certified replay data.",
                    i + 1,
                    source
                );
            }
        }

        out.push(ReplayEvent::Depth(d));
    }
    Ok(out)
}

pub fn load_orders_json(path: &Path) -> Result<OrderFile> {
    let s = std::fs::read_to_string(path).with_context(|| format!("read orders file: {:?}", path))?;
    let of: OrderFile = serde_json::from_str(&s).with_context(|| "parse OrderFile JSON")?;
    Ok(of)
}

pub fn load_intents_json(path: &Path) -> Result<OrderIntentFile> {
    let s = std::fs::read_to_string(path).with_context(|| format!("read intents file: {:?}", path))?;
    let intf: OrderIntentFile = serde_json::from_str(&s).with_context(|| "parse OrderIntentFile JSON")?;
    Ok(intf)
}

pub async fn run_kitesim_backtest_cli(cfg: KiteSimCliConfig) -> Result<()> {
    let replay_path = Path::new(&cfg.replay_path);
    let orders_path = Path::new(&cfg.orders_path);
    let out_dir = Path::new(&cfg.out_dir);

    // Determine execution mode based on depth_path
    let use_l2_mode = cfg.depth_path.is_some();
    let execution_mode = if use_l2_mode {
        SimExecutionMode::L2Book
    } else {
        SimExecutionMode::L1Quote
    };

    // Load events: quote events (always), depth events (if L2 mode)
    let mut replay_events = load_quotes_jsonl(replay_path)?;

    if let Some(ref depth_path) = cfg.depth_path {
        if cfg.certified {
            println!("CERTIFIED MODE: Only accepting SBE-captured depth data");
        }
        let depth_events = load_depth_jsonl(Path::new(depth_path), cfg.certified)?;
        println!("Loaded {} depth events for L2Book mode", depth_events.len());
        replay_events.extend(depth_events);
    }

    // Determine if using intents or bulk orders
    let use_intents = cfg.intents_path.is_some();
    let intents_file = if let Some(ref ip) = cfg.intents_path {
        Some(load_intents_json(Path::new(ip))?)
    } else {
        None
    };
    let order_file = load_orders_json(orders_path)?;

    // Use CLI strategy label if provided; otherwise trust file.
    let strategy_name = if cfg.strategy_name.trim().is_empty() {
        if use_intents {
            intents_file.as_ref().map(|f| f.strategy_name.clone()).unwrap_or_else(|| order_file.strategy_name.clone())
        } else {
            order_file.strategy_name.clone()
        }
    } else {
        cfg.strategy_name.clone()
    };

    let mut sim = KiteSim::new(KiteSimConfig {
        latency: Duration::milliseconds(cfg.latency_ms),
        allow_partial: true,
        taker_slippage_bps: cfg.slippage_bps,
        adverse_selection_max_bps: cfg.adverse_bps,
        reject_if_no_quote_after: Duration::milliseconds(cfg.stale_quote_ms),
        execution_mode,
    });

    // Build SpecStore with venue-specific specs
    let mut specs = SpecStore::new();

    // Collect all symbols from orders or intents
    let symbols: std::collections::HashSet<String> = if use_intents {
        intents_file.as_ref()
            .map(|f| f.intents.iter()
                .flat_map(|i| i.order.legs.iter().map(|l| l.tradingsymbol.clone()))
                .collect())
            .unwrap_or_default()
    } else {
        order_file.orders.iter()
            .flat_map(|o| o.legs.iter().map(|l| l.tradingsymbol.clone()))
            .collect()
    };

    if cfg.venue.to_lowercase() == "binance" {
        // Fetch real tick_size and qty_scale from Binance exchangeInfo
        match fetch_spot_specs(&symbols) {
            Ok(specs_map) => {
                for (sym, (tick_size, qty_scale)) in specs_map {
                    specs.insert_with_scale(&sym, 1, tick_size, qty_scale);
                }
            }
            Err(e) => {
                eprintln!("WARN: fetch_spot_specs failed ({}), using CLI defaults", e);
                let tick = 0.01_f64;
                for sym in &symbols {
                    specs.insert_with_scale(sym, 1, tick, cfg.qty_scale);
                }
            }
        }
    }
    sim = sim.with_specs(specs);

    let policy = AtomicExecPolicy {
        timeout: Duration::milliseconds(cfg.timeout_ms),
        hedge_on_failure: cfg.hedge_on_failure,
    };

    let mut feed = ReplayFeed::new(replay_events);
    let mut all_results = Vec::new();

    if use_intents {
        // Scheduled intents mode: advance feed to each intent timestamp
        let intf = intents_file.as_ref().unwrap();
        let mut intents: Vec<_> = intf.intents.iter().collect();
        intents.sort_by_key(|i| parse_rfc3339_utc(&i.ts).ok());

        for intent in intents {
            let target_ts = parse_rfc3339_utc(&intent.ts)?;

            // Advance replay feed to intent time (consume events strictly before target_ts)
            loop {
                let should_consume = feed.peek().map(|ev| ev.ts() < target_ts).unwrap_or(false);
                if !should_consume {
                    break;
                }
                if let Some(ev) = feed.next() {
                    sim.ingest_event(&ev)?;
                }
            }

            // Set simulator clock to intent time and execute
            sim.set_now(target_ts);
            let mut coord = MultiLegCoordinator::new(&mut sim, policy.clone());
            let res = coord.execute_with_feed(&intent.order, &mut feed).await?;
            all_results.push(res);
        }
    } else {
        // Bulk orders mode (original behavior)
        for order in order_file.orders.iter() {
            let mut coord = MultiLegCoordinator::new(&mut sim, policy.clone());
            let res = coord.execute_with_feed(order, &mut feed).await?;
            all_results.push(res);
        }
    }

    let stats = sim.stats();

    // Fill metrics from execution results
    let fill = FillMetrics {
        orders_total: all_results.len() as u64,
        legs_total: all_results.iter().map(|r| r.leg_results.len() as u64).sum(),
        legs_filled: all_results.iter().flat_map(|r| r.leg_results.iter())
            .filter(|lr| lr.status == LegStatus::Filled).count() as u64,
        legs_partially_filled: all_results.iter().flat_map(|r| r.leg_results.iter())
            .filter(|lr| lr.status == LegStatus::PartiallyFilled).count() as u64,
        legs_rejected: all_results.iter().flat_map(|r| r.leg_results.iter())
            .filter(|lr| lr.status == LegStatus::Rejected).count() as u64,
        legs_cancelled: all_results.iter().flat_map(|r| r.leg_results.iter())
            .filter(|lr| lr.status == LegStatus::Cancelled).count() as u64,
        rollbacks: stats.rollbacks,
        timeouts: stats.timeouts,
        hedges_attempted: stats.hedges_attempted,
        hedges_filled: stats.hedges_filled,
        slippage_bps_p50: quantile(&stats.slippage_samples_bps, 0.50),
        slippage_bps_p90: quantile(&stats.slippage_samples_bps, 0.90),
        slippage_bps_p99: quantile(&stats.slippage_samples_bps, 0.99),
    };

    let venue_label = if cfg.venue.to_lowercase() == "binance" {
        "Binance-Spot-Sim"
    } else {
        "NSE-Zerodha-Sim"
    };

    let mut report = BacktestReport::default();
    report.created_at = Utc::now();
    report.engine = "KiteSim".to_string();
    report.venue = venue_label.to_string();
    report.dataset = replay_path.to_string_lossy().to_string();
    report.fill = fill;
    report.notes.push(format!("strategy={}", strategy_name));
    if use_intents {
        report.notes.push(format!("intents_file={}", cfg.intents_path.as_ref().unwrap()));
    } else {
        report.notes.push(format!("orders_file={}", orders_path.to_string_lossy()));
    }
    report.notes.push(format!("venue={}", cfg.venue));
    report.notes.push(format!("qty_scale={}", cfg.qty_scale));

    std::fs::create_dir_all(out_dir)?;
    report.write_json(out_dir)?;

    // Execution traces: one MultiLegResult per line (JSONL)
    let mut fills_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(out_dir.join("fills.jsonl"))?;
    for r in &all_results {
        let line = serde_json::to_string(r)?;
        writeln!(fills_file, "{}", line)?;
    }

    // Simple PnL calculation for Binance Spot
    if cfg.venue.to_lowercase() == "binance" {
        let mut per_symbol = std::collections::HashMap::<String, f64>::new();
        let scale = (cfg.qty_scale as f64).max(1.0);

        // Get orders from either intents or order_file
        let orders: Vec<&MultiLegOrder> = if use_intents {
            intents_file.as_ref().unwrap().intents.iter().map(|i| &i.order).collect()
        } else {
            order_file.orders.iter().collect()
        };

        for (order, res) in orders.iter().zip(all_results.iter()) {
            for (leg, leg_res) in order.legs.iter().zip(res.leg_results.iter()) {
                if leg_res.status != LegStatus::Filled {
                    continue;
                }
                let px = leg_res.fill_price.unwrap_or(0.0);
                let qty = (leg_res.filled_qty as f64) / scale;
                let signed = match leg.side {
                    LegSide::Buy => -1.0,
                    LegSide::Sell => 1.0,
                };
                *per_symbol.entry(leg.tradingsymbol.clone()).or_insert(0.0) += signed * qty * px;
            }
        }
        let total: f64 = per_symbol.values().sum();
        let pnl = serde_json::json!({
            "venue": cfg.venue,
            "qty_scale": cfg.qty_scale,
            "per_symbol_quote_pnl": per_symbol,
            "total_quote_pnl": total
        });
        std::fs::write(out_dir.join("pnl.json"), serde_json::to_string_pretty(&pnl)?)?;
    }

    println!("KiteSim backtest complete. Report written to: {}/report.json", out_dir.display());
    Ok(())
}

fn quantile(v: &[f64], q: f64) -> f64 {
    if v.is_empty() { return 0.0; }
    let mut vv = v.to_vec();
    vv.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let qq = q.clamp(0.0, 1.0);
    let idx = ((vv.len() as f64 - 1.0) * qq).round() as usize;
    vv[idx]
}
