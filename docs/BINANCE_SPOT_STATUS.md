# Binance Spot — Phase-2A Status & Pending Items

**Status:** L2 Depth Replay infrastructure complete and smoke-tested.
**Current Phase:** Phase-2A (Deterministic L2 Replay) — CERTIFIED

---

## Accomplished

### 1. SBE Depth Capture (Production-Grade)

| Feature | Status | Notes |
|---------|--------|-------|
| SBE WebSocket connection | ✅ | `wss://stream-sbe.binance.com:9443/stream` |
| Bootstrap protocol | ✅ | Connect WS → Buffer → Fetch REST snapshot → Sync |
| Pure string-to-mantissa parser | ✅ | No f64 drift in bootstrap |
| IntegrityTier tagging | ✅ | `CERTIFIED` for SBE capture |
| Gap detection | ✅ | Logs sequence gaps for replay failure |

**Command:**
```bash
cargo run --release --bin kubera-runner -- capture-sbe-depth \
    --symbol BTCUSDT \
    --duration-secs 60 \
    --out data/depth/btcusdt_depth.jsonl \
    --price-exponent=-2 \
    --qty-exponent=-8
```

### 2. JSON Depth Capture (Deprecated)

| Feature | Status | Notes |
|---------|--------|-------|
| JSON WebSocket connection | ✅ | `wss://stream.binance.com:9443/ws` |
| IntegrityTier tagging | ✅ | `NON_CERTIFIED` with deprecation warning |
| Source watermark | ✅ | `binance_json_depth_DEPRECATED` |

**Command (DEPRECATED):**
```bash
cargo run --release --bin kubera-runner -- capture-depth \
    --symbol BTCUSDT \
    --duration-secs 60 \
    --out data/depth/btcusdt_depth_json.jsonl
```

### 3. Certified Mode in Backtest Runner

| Feature | Status | Notes |
|---------|--------|-------|
| `--certified` flag | ✅ | Rejects NON_CERTIFIED depth data |
| Clear error messages | ✅ | Points to SBE capture |
| Backward compatibility | ✅ | Old logs default to NON_CERTIFIED |

**Command:**
```bash
cargo run --release --bin kubera-runner -- backtest-kitesim \
    --venue binance \
    --replay data/quotes/btcusdt_quotes.jsonl \
    --depth data/depth/btcusdt_depth.jsonl \
    --orders data/orders/sample.json \
    --out artifacts/kitesim/l2_test \
    --certified
```

### 4. Data Model Updates

| Component | Status | Location |
|-----------|--------|----------|
| `IntegrityTier` enum | ✅ | `kubera-models/src/depth.rs` |
| `DepthEvent.integrity_tier` | ✅ | `kubera-models/src/depth.rs` |
| `DepthEvent.source` | ✅ | `kubera-models/src/depth.rs` |
| Serde backward compat | ✅ | `#[serde(default)]` for old logs |

### 5. Smoke Test Results (2026-01-21)

| Test | Result |
|------|--------|
| JSON capture (10s) | ✅ 100 events, NON_CERTIFIED |
| SBE capture (10s) | ✅ 207 events, CERTIFIED, 0 gaps |
| Certified mode rejects JSON | ✅ Clear error message |
| Certified mode accepts SBE | ✅ Backtest completes |
| Unit tests | ✅ All pass |

---

## Pending Items

### High Priority (Before Production)

| Item | Priority | Notes |
|------|----------|-------|
| Multi-symbol depth capture | P1 | Currently single-symbol only |
| Depth + Quote synchronization | P1 | Ensure timestamp alignment |
| Long-duration capture stability | P1 | Test 1hr+ captures |
| Network reconnection handling | P1 | Auto-recover from disconnects |

### Medium Priority (Phase-2B)

| Item | Priority | Notes |
|------|----------|-------|
| Trade tape capture | P2 | SBE template 10000 |
| L2 Book → Fill simulation | P2 | Multi-level consumption |
| IV surface integration | P2 | For options pricing |
| Real-time trading bridge | P2 | Live order execution |

### Low Priority (Future)

| Item | Priority | Notes |
|------|----------|-------|
| Futures SBE capture | P3 | Different endpoint |
| Options chain fetching | P3 | Binance options API |
| Cross-exchange arbitrage | P3 | Unified order book |

---

## Key Files

| File | Purpose |
|------|---------|
| `kubera-runner/src/binance_sbe_depth_capture.rs` | SBE depth capture (authoritative) |
| `kubera-runner/src/binance_depth_capture.rs` | JSON depth capture (deprecated) |
| `kubera-runner/src/kitesim_backtest.rs` | Backtest runner with certified mode |
| `kubera-models/src/depth.rs` | IntegrityTier, DepthEvent |
| `kubera-sbe/src/lib.rs` | SBE decoder (template 10003) |
| `PHASE2A_NOTES.md` | L2 replay documentation |

---

## Environment Variables Required

```bash
# .env file
BINANCE_API_KEY_ED25519=your_api_key  # Required for SBE stream
BINANCE_API_KEY=your_api_key          # For REST API
BINANCE_API_SECRET=your_secret        # For authenticated endpoints
```

---

## Quick Reference: Recent Commits

| Commit | Feature |
|--------|---------|
| `e3fd5de` | Coherence gate and MeanRev signal saturation |
| `b4f3e6b` | PATCHES.md for tracking patch history |
| `9c53581` | UniverseSpecV1 for deterministic WAL replay |
| `2982b30` | Circuit breaker TUI, IV surface display |
| `3195411` | Binance tuning patch with cost-aware gating |

---

## Directory Structure

```
data/
├── depth/
│   ├── btcusdt_depth.jsonl      # SBE capture (CERTIFIED)
│   └── btcusdt_depth_json.jsonl # JSON capture (NON_CERTIFIED)
├── quotes/
│   └── btcusdt_quotes.jsonl     # Quote events
└── orders/
    └── sample.json              # Test orders

artifacts/
└── kitesim/
    └── l2_test/
        ├── report.json
        ├── fills.jsonl
        └── pnl.json
```

---

## Next Steps for Production

1. **Capture production data** (1hr BTCUSDT depth via SBE)
2. **Run determinism test** (same input → same output)
3. **Validate gap handling** (inject synthetic gap, verify hard fail)
4. **Tag milestone** `v0.2.0-phase2a-certified`
