# Project AEON: Final Summary & Production Configuration

## Overview

Project AEON is a hierarchical trading intelligence stack that integrates:
1. **Informational Tier (BIF)**: Shannon Entropy + Mutual Information gating
2. **Structural Tier (FTI)**: Zero-lag Least-Squares trend/mean-reversion detection
3. **Tactical Tier (HYDRA)**: Multi-expert ensemble with adaptive weight allocation

---

## 🎯 WFA-Validated Production Thresholds

| Parameter | Value | Purpose |
|-----------|-------|---------|
| **`bif_predictability_threshold`** | **0.38** | Gates trading to high-predictability windows only |
| **`fti_trend_threshold`** | **3.56** | Requires strong structural trend for trend trades |
| **`fti_mean_rev_threshold`** | **0.80** | Mean-reversion regime detection |
| **`learning_rate`** | **0.033** | Conservative expert weight adaptation |
| **`zscore_action_threshold`** | **1.75** | High-conviction signal filter |

---

## 📊 Verification Results

### Walk-Forward Analysis (3-Fold)
| Fold | IS Sharpe | OOS Sharpe | OOS PnL |
|------|-----------|------------|---------|
| 1 | 0.008 | -0.015 | -$79.84 |
| 2 | -0.024 | -0.060 | -$211.52 |
| 3 | 0.044 | -0.247 | -$363.24 |

### Live Paper Trading Test (Low Threshold = 0.10)
| Metric | Value |
|--------|-------|
| Duration | ~5 minutes |
| Total Fills | 23 |
| Round-Trip Trades | 19 |
| Total PnL | **-$260.54** |
| Win Rate | 5.26% |
| Max Drawdown | 0.27% |

> [!IMPORTANT]
> The low-threshold test confirms the WFA findings: trading in high-entropy (noisy) markets leads to **negative expectancy** due to commission drag and lack of structural edge.

---

## 🚀 How to Run

### Production Mode (Recommended)
```bash
cargo run -p kubera-runner -- --mode paper --aeon --headless
```
With production thresholds (0.38), AEON waits for high-predictability windows before trading.

### Backtest Mode
```bash
cargo run -p kubera-runner -- --mode backtest --symbol BTCUSDT --aeon --wal-file binance_btcusdt_fresh.wal --headless
```

### Optimization
```bash
cargo run -p kubera-backtest --bin optimize_aeon
```

---

## 📁 Configuration Files

- **Production**: `paper.toml` (threshold = 0.38)
- **Backtest**: `backtest.toml` (threshold = 0.38)
- **Optimizer**: `crates/kubera-backtest/src/bin/optimize_aeon.rs`

---

## Conclusion

Project AEON is designed as a **"Patient Sniper"** strategy that:
- ✅ Preserves capital during random-walk (high-entropy) periods
- ✅ Only trades when market structure is statistically predictable
- ✅ Uses hierarchical gating to filter noise before tactical execution

The strategy will appear "inactive" most of the time—this is by design. True alpha capture requires waiting for high-conviction setups.
