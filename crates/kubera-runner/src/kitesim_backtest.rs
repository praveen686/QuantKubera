//! KiteSim Offline Backtest Runner
//!
//! Provides a CLI-friendly entrypoint:
//! - load replay events (JSONL of QuoteEvent)
//! - load orders (JSON)
//! - execute sequentially through MultiLegCoordinator
//! - emit report.json, fills.jsonl, pnl.json

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use kubera_options::execution::{LegSide, LegStatus};
use kubera_options::kitesim::{AtomicExecPolicy, KiteSim, KiteSimConfig, MultiLegCoordinator};
use kubera_options::replay::{QuoteEvent, ReplayEvent, ReplayFeed};
use kubera_options::report::{BacktestReport, FillMetrics};
use kubera_options::specs::SpecStore;

use crate::order_io::OrderFile;

pub struct KiteSimCliConfig {
    pub venue: String,
    pub qty_scale: u32,
    pub strategy_name: String,
    pub replay_path: String,
    pub orders_path: String,
    pub out_dir: String,
    pub timeout_ms: i64,
    pub latency_ms: i64,
    pub slippage_bps: f64,
    pub adverse_bps: f64,
    pub stale_quote_ms: i64,
    pub hedge_on_failure: bool,
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

pub fn load_orders_json(path: &Path) -> Result<OrderFile> {
    let s = std::fs::read_to_string(path).with_context(|| format!("read orders file: {:?}", path))?;
    let of: OrderFile = serde_json::from_str(&s).with_context(|| "parse OrderFile JSON")?;
    Ok(of)
}

pub async fn run_kitesim_backtest_cli(cfg: KiteSimCliConfig) -> Result<()> {
    let replay_path = Path::new(&cfg.replay_path);
    let orders_path = Path::new(&cfg.orders_path);
    let out_dir = Path::new(&cfg.out_dir);

    let replay_events = load_quotes_jsonl(replay_path)?;
    let order_file = load_orders_json(orders_path)?;

    // Use CLI strategy label if provided; otherwise trust file.
    let strategy_name = if cfg.strategy_name.trim().is_empty() {
        order_file.strategy_name.clone()
    } else {
        cfg.strategy_name.clone()
    };

    let mut sim = KiteSim::new(KiteSimConfig {
        latency: Duration::milliseconds(cfg.latency_ms),
        allow_partial: true,
        taker_slippage_bps: cfg.slippage_bps,
        adverse_selection_max_bps: cfg.adverse_bps,
        reject_if_no_quote_after: Duration::milliseconds(cfg.stale_quote_ms),
    });

    // Build SpecStore with venue-specific defaults
    let mut specs = SpecStore::new();
    if cfg.venue.to_lowercase() == "binance" {
        // Minimal defaults for Binance Spot fixed-point quantities.
        // tick_size here is a placeholder; you can refine per-symbol later.
        let tick = 0.01_f64;
        for o in order_file.orders.iter() {
            for leg in o.legs.iter() {
                specs.insert_with_scale(&leg.tradingsymbol, 1, tick, cfg.qty_scale);
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

    for order in order_file.orders.iter() {
        let mut coord = MultiLegCoordinator::new(&mut sim, policy.clone());
        let res = coord.execute_with_feed(order, &mut feed).await;
        all_results.push(res);
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
    report.notes.push(format!("orders_file={}", orders_path.to_string_lossy()));
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

    // Simple PnL: only meaningful for Binance Spot smoke tests (single symbol, USDT quote).
    if cfg.venue.to_lowercase() == "binance" {
        let mut per_symbol = std::collections::HashMap::<String, f64>::new();
        let scale = (cfg.qty_scale as f64).max(1.0);
        for (order, res) in order_file.orders.iter().zip(all_results.iter()) {
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
