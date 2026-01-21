# Phase-2A: L2 Depth Replay Notes

## 1. Capture Depth Data

Capture Binance Spot depth stream to DepthEvent JSONL:

```bash
# Capture 60 seconds of BTCUSDT depth at 100ms granularity
cargo run --release --bin kubera-runner -- capture-depth \
    --symbol BTCUSDT \
    --duration 60 \
    --out data/depth/btcusdt_depth.jsonl \
    --price-exponent -2 \
    --qty-exponent -8
```

**Parameters:**
- `--symbol`: Trading pair (e.g., BTCUSDT, ETHUSDT)
- `--duration`: Capture duration in seconds
- `--out`: Output JSONL file path
- `--price-exponent`: Decimal places for price (e.g., -2 = 2 decimals)
- `--qty-exponent`: Decimal places for quantity (e.g., -8 = 8 decimals)

## 2. Run Backtest in L2 Mode

Run KiteSim backtest with depth-aware execution:

```bash
# L2 Book mode (requires both quotes and depth files)
cargo run --release --bin kubera-runner -- backtest-kitesim \
    --venue binance \
    --replay data/quotes/btcusdt_quotes.jsonl \
    --depth data/depth/btcusdt_depth.jsonl \
    --orders data/orders/sample_orders.json \
    --out artifacts/kitesim/l2_test \
    --timeout-ms 5000 \
    --latency-ms 50

# L1 Quote mode (original behavior, no --depth flag)
cargo run --release --bin kubera-runner -- backtest-kitesim \
    --venue binance \
    --replay data/quotes/btcusdt_quotes.jsonl \
    --orders data/orders/sample_orders.json \
    --out artifacts/kitesim/l1_test
```

**L2 Mode Behavior:**
- Maintains full order book from depth updates
- Market orders consume multiple price levels
- Enforces update ID sequencing (gaps cause hard failure)
- Fill prices are actual book levels consumed

## 3. Example Replay Files

Generate a small test dataset:

```bash
# Step 1: Capture 30 seconds of quotes
cargo run --release --bin kubera-runner -- capture-quotes \
    --symbol BTCUSDT \
    --duration 30 \
    --out data/replay/btcusdt_quotes_30s.jsonl

# Step 2: Capture 30 seconds of depth (run concurrently or sequentially)
cargo run --release --bin kubera-runner -- capture-depth \
    --symbol BTCUSDT \
    --duration 30 \
    --out data/replay/btcusdt_depth_30s.jsonl \
    --price-exponent -2 \
    --qty-exponent -8
```

**Expected file locations:**
- Quotes: `data/replay/btcusdt_quotes_30s.jsonl`
- Depth: `data/replay/btcusdt_depth_30s.jsonl`

## 4. DepthEvent JSONL Format

Each line is a JSON object:

```json
{
  "ts": "2026-01-21T10:30:00.123Z",
  "tradingsymbol": "BTCUSDT",
  "first_update_id": 12345,
  "last_update_id": 12345,
  "price_exponent": -2,
  "qty_exponent": -8,
  "bids": [{"price": 10500000, "qty": 150000000}],
  "asks": [{"price": 10500100, "qty": 200000000}]
}
```

**Scaled integer decoding:**
- `price = 10500000 * 10^(-2) = 105000.00`
- `qty = 150000000 * 10^(-8) = 1.50000000`

## 5. Determinism Guarantees

- **BTreeMap ordering**: LOB uses BTreeMap for deterministic price level iteration
- **Scaled integers**: All prices/quantities stored as i64 mantissas with exponents
- **Gap detection**: `first_update_id` must equal `last_update_id + 1` or replay fails
- **Same input → Same output**: Identical replay files produce bit-identical fills.jsonl and pnl.json

## 6. Output Files

After backtest completes in `--out` directory:
- `report.json`: Backtest summary with fill metrics
- `fills.jsonl`: Detailed execution traces per order
- `pnl.json`: Per-symbol and total PnL (Binance venue only)
