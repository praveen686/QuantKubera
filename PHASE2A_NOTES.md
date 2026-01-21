# Phase-2A: L2 Depth Replay Notes

## SBE-First Architecture

> **IMPORTANT**: SBE depth capture is the only certified L2 source for production backtesting.
> JSON depth capture is **DEPRECATED** and marked as NON_CERTIFIED.

### Data Integrity Tiers

| Tier | Source | Use Case |
|------|--------|----------|
| `CERTIFIED` | SBE capture (`capture-sbe-depth`) | Production backtests, certification |
| `NON_CERTIFIED` | JSON capture (`capture-depth`) | Debugging only, NOT for production |

### Certified Mode

Use `--certified` flag to reject NON_CERTIFIED depth data:

```bash
cargo run --release --bin kubera-runner -- backtest-kitesim \
    --venue binance \
    --replay data/quotes/btcusdt_quotes.jsonl \
    --depth data/depth/btcusdt_depth.jsonl \
    --orders data/orders/sample_orders.json \
    --out artifacts/kitesim/l2_test \
    --certified  # Rejects JSON-captured depth data
```

---

## 1. Capture Depth Data (SBE - Recommended)

Capture Binance Spot SBE depth stream with proper bootstrap protocol:

```bash
# Set API key (required for SBE stream)
export BINANCE_API_KEY_ED25519="your-api-key"

# Capture 60 seconds of BTCUSDT depth via SBE
cargo run --release --bin kubera-runner -- capture-sbe-depth \
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

**SBE Bootstrap Protocol:**
1. Fetch REST depth snapshot (initial state + lastUpdateId)
2. Connect to SBE WebSocket stream
3. Buffer diffs until sync point (U <= lastUpdateId+1 <= u)
4. Write snapshot first, then apply diffs in sequence

---

## 2. Deprecated: JSON Depth Capture

> **WARNING**: JSON capture is DEPRECATED and NOT suitable for certified replay.
> Data captured via JSON will be marked `NON_CERTIFIED` and rejected in certified mode.

```bash
# DEPRECATED - use capture-sbe-depth instead
cargo run --release --bin kubera-runner -- capture-depth \
    --symbol BTCUSDT \
    --duration 60 \
    --out data/depth/btcusdt_depth_json.jsonl
```

**Limitations of JSON capture:**
- No bootstrap protocol (missing snapshot)
- f64 intermediate parsing (potential cross-platform drift)
- Missing update IDs can cause gaps

---

## 3. Run Backtest in L2 Mode

```bash
# L2 Book mode with certified SBE data
cargo run --release --bin kubera-runner -- backtest-kitesim \
    --venue binance \
    --replay data/quotes/btcusdt_quotes.jsonl \
    --depth data/depth/btcusdt_depth.jsonl \
    --orders data/orders/sample_orders.json \
    --out artifacts/kitesim/l2_test \
    --timeout-ms 5000 \
    --latency-ms 50 \
    --certified  # Enable certified mode

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

---

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
  "asks": [{"price": 10500100, "qty": 200000000}],
  "is_snapshot": true,
  "integrity_tier": "CERTIFIED",
  "source": "binance_sbe_depth_capture"
}
```

**Field descriptions:**
- `integrity_tier`: `"CERTIFIED"` (SBE) or `"NON_CERTIFIED"` (JSON)
- `source`: Capture module identifier
- `is_snapshot`: `true` for initial book state, `false` for diffs

**Scaled integer decoding:**
- `price = 10500000 * 10^(-2) = 105000.00`
- `qty = 150000000 * 10^(-8) = 1.50000000`

---

## 5. Determinism Guarantees

- **Pure string-to-mantissa parsing**: SBE bootstrap uses string parsing without f64 intermediates
- **BTreeMap ordering**: LOB uses BTreeMap for deterministic price level iteration
- **Scaled integers**: All prices/quantities stored as i64 mantissas with exponents
- **Gap detection**: `first_update_id` must equal `last_update_id + 1` or replay fails
- **Same input → Same output**: Identical replay files produce bit-identical fills.jsonl and pnl.json

---

## 6. Output Files

After backtest completes in `--out` directory:
- `report.json`: Backtest summary with fill metrics
- `fills.jsonl`: Detailed execution traces per order
- `pnl.json`: Per-symbol and total PnL (Binance venue only)

---

## 7. Test Coverage

Key tests for L2 replay:
- `kitesim_l2_determinism_golden_test`: Same input always produces identical output
- `kitesim_l2_gap_detection_hard_fail`: Missing depth update causes hard fail

Run tests:
```bash
cargo test -p kubera-options -- kitesim_l2
```
