// KiteSim Strategy Backtest Runner (Offline)
// -----------------------------------------
// This is a deterministic, offline runner that connects:
//
// HYDRA / strategy intent
//   -> MultiLegOrder
//   -> ReplayFeed
//   -> KiteSim execution
//   -> BacktestReport (report.json)
//
// This file is intentionally standalone and side-effect free.
// Wire it into main.rs when ready.

use chrono::{Duration, Utc};
use anyhow::Result;
use std::path::Path;

use kubera_options::kitesim::{KiteSim, KiteSimConfig};
use kubera_options::replay::{ReplayFeed, ReplayEvent};
use kubera_options::report::BacktestReport;
use kubera_options::specs::SpecStore;
use kubera_options::execution::MultiLegOrder;

/// Run a single offline KiteSim backtest.
pub fn run_kitesim_backtest(
    strategy_name: &str,
    replay_events: Vec<ReplayEvent>,
    orders: Vec<MultiLegOrder>,
    out_dir: &Path,
) -> Result<()> {

    // 1) Build simulator
    let cfg = KiteSimConfig {
        latency: Duration::milliseconds(150),
        allow_partial: true,
        taker_slippage_bps: 0.0,
        adverse_selection_max_bps: 0.0,
        reject_if_no_quote_after: Duration::seconds(10),
    };

    let mut sim = KiteSim::new(cfg);

    // Optional: attach instrument specs
    let specs = SpecStore::default();
    // specs.insert("BANKNIFTY24OCT45000CE", 15, 0.05);
    sim = sim.with_specs(specs);

    // 2) Load replay feed
    let mut feed = ReplayFeed::new(replay_events);

    // 3) Execute orders sequentially
    for order in orders.iter() {
        // NOTE:
        // Call into your existing MultiLegCoordinator here.
        // This runner deliberately does not assume policy.
        let _ = order;
    }

    // 4) Emit report (placeholder)
    let report = BacktestReport {
        created_at: Utc::now(),
        engine: "KiteSim".to_string(),
        venue: "NSE-Zerodha-Sim".to_string(),
        dataset: "replay-pack".to_string(),
        fill: Default::default(),
        notes: vec![
            format!("strategy={}", strategy_name),
            "offline backtest runner scaffold".to_string(),
        ],
    };

    report.write(out_dir)?;

    Ok(())
}
