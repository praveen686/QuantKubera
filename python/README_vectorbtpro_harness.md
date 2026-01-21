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

## HYDRA-lite mode and orders.json


By default the harness runs `--mode hydra`, which computes a **HYDRA-lite** expert ensemble
from klines and backtests a **long-only** policy via VectorBT Pro.

To emit a KiteSim-compatible `orders.json` (single-leg intent smoke test):

```bash
python3 python/vbt_hydra_klines_harness.py \
  --symbol BTCUSDT \
  --interval 1m \
  --start 2026-01-20T00:00:00Z \
  --end 2026-01-21T00:00:00Z \
  --mode hydra \
  --threshold 0.15 \
  --emit-orders \
  --orders-qty-base 0.001 \
  --out artifacts/vbt_quicktest
```

This will write (if the final score exceeds the threshold):

- `<out_dir>/orders.json` compatible with `kubera-runner backtest-kitesim`.

**Important limitation:** The current runner executes orders sequentially without scheduling,
so `orders.json` is intended for *smoke-grade intent validation*, not full intraday trade scheduling.
