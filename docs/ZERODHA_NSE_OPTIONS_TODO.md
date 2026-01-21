# Zerodha NSE Options — Tomorrow's Action Plan

**Status:** All infrastructure and execution plumbing complete.
**What remains:** Data → Intent → Test

---

## 1. Data Requirements (Minimum Viable Replay Pack)

### Instrument Scope
- **Index:** BANKNIFTY
- **Expiry:** Nearest weekly
- **Strikes:**
  - ATM CE / PE
  - ±1 strike
  - ±2 strikes
- **Total:** ~6–10 instruments

### Fields Required (per tick/quote)
| Field | Description |
|-------|-------------|
| `ts` | Exchange timestamp (UTC) |
| `tradingsymbol` | e.g., `BANKNIFTY26JAN48000CE` |
| `bid` | Best bid price |
| `ask` | Best ask price |
| `bid_qty` | Bid quantity |
| `ask_qty` | Ask quantity |

---

## 2. Data Collection Options (During Market Hours)

### Option A: WebSocket Capture (Preferred)
1. Subscribe to selected BANKNIFTY option instruments
2. Capture every quote update
3. Append to JSONL file

**Output path:**
```
data/replay/BANKNIFTY/2026-01-22/quotes.jsonl
```

**Format (one JSON per line):**
```json
{"ts":"2026-01-22T09:15:00Z","tradingsymbol":"BANKNIFTY26JAN48000CE","bid":123.45,"ask":123.55,"bid_qty":150,"ask_qty":75}
```

### Option B: Historical Snapshot (Acceptable for Day 1)
- Use existing tick dumps or minute-level bid/ask snapshots
- Reconstruct synthetic quotes in chronological order
- Fine for execution logic testing, not alpha discovery

---

## 3. Orders Input (Create Before Market)

**Path:** `data/orders/manual_smoke/orders.json`

```json
{
  "strategy_name": "MANUAL_SMOKE",
  "orders": [
    {
      "strategy_name": "MANUAL_SMOKE",
      "legs": [
        {
          "tradingsymbol": "BANKNIFTY26JAN48000CE",
          "exchange": "NFO",
          "side": "Buy",
          "quantity": 15,
          "order_type": "Market",
          "price": null
        }
      ],
      "total_margin_required": 0.0
    }
  ]
}
```

**Purpose:**
- Validate KiteSim execution
- Validate fill logic
- Validate reporting
- Validate slippage & rollback paths
- No strategy assumptions yet

---

## 4. First Test Command

```bash
cargo run -p kubera-runner -- backtest-kitesim \
  --replay data/replay/BANKNIFTY/2026-01-22/quotes.jsonl \
  --orders data/orders/manual_smoke/orders.json \
  --out artifacts/kitesim/zerodha_day1 \
  --timeout-ms 5000 \
  --latency-ms 150 \
  --slippage-bps 0.0 \
  --adverse-bps 0.0 \
  --stale-quote-ms 10000 \
  --hedge-on-failure true
```

### Success Criteria
- [ ] `report.json` exists in output directory
- [ ] Legs filled as expected
- [ ] No silent failures
- [ ] Slippage stats make sense
- [ ] Rollbacks only when expected

**If this passes → Zerodha offline testing spine is certified.**

---

## 5. After Certification (Only Then)

1. Wire HYDRA → `Vec<MultiLegOrder>` conversion
2. Generate orders programmatically
3. Run multi-day offline tests

---

## Quick Reference: Commits

| Commit | Feature |
|--------|---------|
| `a13c3cf` | KiteSim skeleton |
| `243ef5d` | Validation + Reporting |
| `a1d387c` | Patch 2 scaffold |
| `7dced94` | Patch 3 stress realism |
| `858db26` | Runner scaffold |
| `05c9a58` | CLI wiring |

---

## Directory Structure

```
data/
├── replay/
│   └── BANKNIFTY/
│       └── 2026-01-22/
│           └── quotes.jsonl
└── orders/
    └── manual_smoke/
        └── orders.json

artifacts/
└── kitesim/
    └── zerodha_day1/
        └── report.json
```
