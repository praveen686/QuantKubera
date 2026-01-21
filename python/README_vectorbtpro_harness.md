# VectorBT Pro Quick-Test Harness (Binance Klines)

This harness is for **fast strategy logic testing** using Binance `klines` (OHLCV).
It is intentionally **not** an execution simulator (no bid/ask, partial fills, atomic multi-leg).

Use this to quickly identify promising signals and parameter regimes, then promote candidates to
**KiteSim replay** for realistic execution testing.

## Run

From repo root (ensure your Python env has `vectorbtpro`, `pandas`, `numpy`, `requests`):

```bash
python3 python/vbt_hydra_klines_harness.py \
  --symbol BTCUSDT \
  --interval 1m \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-03T00:00:00Z \
  --fees-bps 1.0 \
  --slippage-bps 0.5 \
  --out artifacts/vbt_quicktest
```

## Outputs

Example:

- `artifacts/vbt_quicktest/BTCUSDT_1m/equity.csv`
- `artifacts/vbt_quicktest/BTCUSDT_1m/signals.csv`
- `artifacts/vbt_quicktest/BTCUSDT_1m/summary.json`

## Next step (promotion)

Once a baseline is promising, wire HYDRA → orders, and validate on:

- `kubera-runner backtest-kitesim --replay ... --orders ... --venue binance`
