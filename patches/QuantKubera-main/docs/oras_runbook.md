
# 📘 ORAS Strategy Runbook

**Version**: 1.0.0
**Date**: 2024-12-18
**Strategy**: OmniRegime Adaptive Sniper (ORAS-v1)
**Compliance**: IEEE 12207 - Software Life Cycle Processes

---

## 1. Overview
The OmniRegime Adaptive Sniper (ORAS) is the flagship high-frequency trading strategy for the QuantKubera platform. It is designed to trade `BTCUSDT` by dynamically switching behaviors based on real-time market regimes (Trending vs. Mean-Reverting) and exploiting L2 order book imbalances.

## 2. Deployment

### 2.1 Prerequisites
- **Rust Toolchain**: Stable channel (1.70+)
- **Binance API Credentials**: Required for live data (Paper/Live mode). Set `BINANCE_API_KEY_ED25519`.
- **Network**: Low-latency connection to AWS/GCP (for production).

### 2.2 Execution Modes

#### A. Live Paper Trading (Recommended for Testing)
Runs the strategy against live market data but simulates execution (0-risk).
```bash
# In workspace root
export BINANCE_API_KEY_ED25519="your_key_here"
cargo run --release -p kubera-runner -- --sandbox
```

#### B. Backtesting (WAL Replay)
Replays historical data to verify logic on past events.
```bash
cargo test --release -p kubera-runner --test backtest_oras_wal -- --nocapture
```

#### C. Synthetic Backtesting (Chaos Mode)
Stress tests the strategy against randomized/choppy data.
```bash
cargo test --release -p kubera-runner --test backtest_oras -- --nocapture
```

## 3. Monitoring & Operations

### 3.1 TUI Dashboard
When running in `kubera-runner`, the Terminal UI provides real-time telemetry:
- **Strategy Status**: `ON` (Cyan) denotes ORAS is active.
- **Position**: Current inventory (positive = Long, negative = Short).
- **PnL**: Realized and Unrealized Profit/Loss.
- **Order Log**: Stream of signals, risk rejections, and fills.

### 3.2 Key Controls (TUI)
- `s`: Toggle ORAS (BTCUSDT) On/Off.
- `k`: Kill Switch (Halts all outgoing orders immediately).
- `q`: Quit application/

### 3.3 Log Files
- **Execution Log**: Standard output (redirect to file for persistence).
- **WAL**: `trading.wal` contains a binary log of all Market Data and Orders for compliance auditing.

## 4. Troubleshooting

### 4.1 "Strategy not trading"
- **Cause**: Signal threshold not met (0.8) or Market Regime is "Toxic".
- **Action**: Check logs for `[ORAS] High volatility` messages. Verify `trading.wal` is growing (data flow).

### 4.2 "High Slippage Detected"
- **Cause**: Latency between Signal and Fill.
- **Action**: Strategy auto-recalibrates. Check network latency or CPU load.

### 4.3 "Risk Reject"
- **Cause**: Position limit or Max Order Value exceeded.
- **Action**: Check `risk_config` in `kubera-runner/src/main.rs`.

## 5. Compliance & Safety
- **Isolation**: ORAS runs in a separate process (`kubera-strategy-host`) to prevent crashes from affecting the core engine.
- **Audit**: All decisions are deterministically replayable from the WAL.
- **Kill Switch**: Hard-coded safety override available via `k` key or API.

---
**QuantKubera Team** | *Confidential & Proprietary*
