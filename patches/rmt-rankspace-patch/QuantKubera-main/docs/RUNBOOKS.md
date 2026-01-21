# QuantKubera Operations Runbooks

## 1. Safety Procedures

### Emergency Shutdown (Kill Switch)
- **Automatic**: Triggered by circuit breakers (MTM loss > 5%, Data Disconnect > 60s).
- **Manual**: Press `Ctrl+C` in the TUI runner.

### Risk Limit Breaches
1. Check TUI for alert messages.
2. Verify Greeks levels in the dashboard.
3. If delta exceeds limit, the system will pause automated execution.
4. Manually re-hedge or reduce positions via Zerodha/Binance dashboards.

## 2. Common Issues & Resolution

### Zerodha Authentication Failure
- **Symptom**: `Unauthorized` errors in logs.
- **Cause**: Access token expired or TOTP incorrect.
- **Fix**: Run `python3 crates/kubera-connectors/scripts/zerodha_auth.py` to refresh terminal session.

### WebSocket Disconnect
- **Symptom**: Stale prices in TUI.
- **Cause**: Network instability or exchange maintenance.
- **Behavior**: System enters "PAUSED" mode and attempts reconnection with exponential backoff.

## 3. Deployment Runbook

### Local Development
```bash
cargo check
cargo run -p kubera-runner -- --mode live --config config.toml
```

### Production (Docker)
```bash
docker build -t kubera .
docker run -e ZERODHA_API_KEY=... kubera
```

## 4. Backtesting Strategy
1. Ensure WAL files are in `data/`.
2. Run `cargo run -p kubera-backtest`.
3. Review `reports/backtest_results.json`.
