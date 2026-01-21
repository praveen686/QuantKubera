# QuantKubera Patch Notes & Instructions

This document tracks all patches applied to QuantKubera, their purpose, usage instructions, and implementation status.

---

## Table of Contents

1. [Applied Patches](#applied-patches)
   - [Binance Cost-Aware Trade Gating](#1-binance-cost-aware-trade-gating)
   - [Circuit Breaker TUI & IV Surface Display](#2-circuit-breaker-tui--iv-surface-display)
   - [UniverseSpec WAL for Deterministic Replay](#3-universespec-wal-for-deterministic-replay)
   - [Strategy R&D: Coherence Gate & MeanRev Fix](#4-strategy-rd-coherence-gate--meanrev-fix)
2. [Planned Patches](#planned-patches)
   - [Shadow Calibration](#shadow-calibration)
   - [Rank-Space Mean Reversion / Random Matrix Theory](#rank-space-mean-reversion--random-matrix-theory)
   - [Gamma Blast Strategy (Index Expiries)](#gamma-blast-strategy-index-expiries)
3. [Patch Application Guide](#patch-application-guide)

---

## Applied Patches

### 1. Binance Cost-Aware Trade Gating

**Commit:** `3195411`
**Date:** 2026-01-20
**Status:** ✅ Applied

#### Purpose
Prevents HYDRA from executing trades unless expected edge exceeds transaction costs multiplied by a hurdle rate. Implements "no-leakage" trading discipline.

#### Key Changes
- Added `edge_hurdle_multiplier` config (default: 2.5)
- Added `commission_bps` and `slippage_bps` to cost model
- Trade gating formula: `required_edge = max(min_edge_bps, total_cost * hurdle)`
- Added candidate logging for tuning analysis

#### Configuration
```toml
[strategy.hydra]
edge_hurdle_multiplier = 2.5  # Require 2.5x cost coverage
commission_bps = 7.5          # Binance taker fee
slippage_bps = 0.0            # Estimated slippage
min_edge_bps = 8.0            # Minimum edge threshold
```

#### Observations
- System correctly blocks low-edge trades (max observed ~16 bps vs required ~18.8 bps)
- Expert signals often cancel out due to disagreement (committee cancellation)
- MeanRev expert signal saturating at 0.900 (needs investigation)

---

### 2. Circuit Breaker TUI & IV Surface Display

**Commit:** `2982b30`
**Date:** 2026-01-21
**Status:** ✅ Applied

#### Purpose
Expose circuit breaker internals and IV surface data in the TUI for real-time monitoring.

#### Key Changes

**Circuit Breakers (`circuit_breakers.rs`):**
- Added `CircuitBreakerStatus` struct with all status fields
- Added `detailed_status()` method for TUI display
- Added `signal_rate_available()`, `order_rate_available()`, `current_latency_p99()`
- Latency recording on market event processing

**Main Runner (`main.rs`):**
- New Circuit Breakers panel in TUI showing:
  - Status (OK/TRIPPED with reasons: KILL, LAT, FLOW, DD)
  - p99 latency
  - Current drawdown %
  - Signal/Order rate limiter tokens
  - Trip count
- Added 'r' key to reset circuit breakers
- IV Surface display in portfolio panel (when available)
- Removed unreachable pattern warning

**Headless Mode:**
- Circuit breaker status in periodic metrics output
- IV surface term structure display

#### TUI Hotkeys
```
's' - Toggle BTC Strategy
't' - Toggle ETH Strategy
'n' - Toggle NIFTY Strategy
'k' - Kill Switch
'r' - Reset Circuit Breakers
'm' - Log Metrics Report
'q' - Quit
```

---

### 3. UniverseSpec WAL for Deterministic Replay

**Commit:** `9c53581`
**Date:** 2026-01-21
**Status:** ✅ Applied

#### Purpose
Enable fully deterministic WAL replay by embedding a canonical trading universe specification (symbols, tokens, lot sizes) in WAL metadata.

#### Key Changes

**kubera-core (`lib.rs`):**
```rust
pub struct UniverseSpecV1 {
    pub version: String,           // "v1"
    pub venue: String,             // "zerodha" or "binance"
    pub name: String,              // Human-readable name
    pub symbols: Vec<String>,      // Canonical symbol list
    pub instruments: HashMap<String, Value>,  // Per-symbol metadata
    pub extra: Value,              // Experimental data
}
```
- Added `WalReader::read_universe_spec_v1()` method

**record_zerodha_wal_l2.rs:**
- New `--universe-json <path>` argument (preferred over `--tokens-file`)
- Derives tokens from universe spec instruments
- Writes `universe_spec_v1` metadata to WAL upfront
- Warning when falling back to legacy tokens file

**New Script: `zerodha_build_universe_json.py`:**
- Builds canonical universe spec from Kite API
- Supports all NSE indices

#### Usage

**Step 1: Build Universe Spec**
```bash
export KITE_API_KEY=your_key
export KITE_ACCESS_TOKEN=your_token

python3 crates/kubera-connectors/scripts/zerodha_build_universe_json.py \
  --indices NIFTY,BANKNIFTY,FINNIFTY,MIDCPNIFTY \
  --mode fut+opt \
  --near-atm 10 \
  --max-expiries 1 \
  --out universe.json
```

**Arguments:**
| Arg | Description | Default |
|-----|-------------|---------|
| `--indices` | Comma-separated index underlyings | `NIFTY,BANKNIFTY` |
| `--mode` | `fut`, `opt`, or `fut+opt` | `fut` |
| `--near-atm` | Include only N strikes around ATM (0=all) | `0` |
| `--max-expiries` | Number of nearest expiries per index | `1` |
| `--out` | Output JSON path | `universe.json` |

**Step 2: Record WAL with Universe**
```bash
cargo run -p kubera-backtest --bin record_zerodha_wal_l2 -- \
  --universe-json universe.json \
  --out zerodha_live.wal \
  --seconds 600 \
  --snapshot-every 5
```

**Step 3: Read Universe from WAL (in code)**
```rust
let mut reader = WalReader::open("zerodha_live.wal")?;
if let Some(spec) = reader.read_universe_spec_v1()? {
    println!("Universe: {} symbols on {}", spec.symbols.len(), spec.venue);
}
```

---

### 4. Strategy R&D: Coherence Gate & MeanRev Fix

**Commit:** `818fcb5`
**Date:** 2026-01-21
**Status:** ✅ Applied

#### Purpose
Address expert signal cancellation and MeanRev saturation issues identified during paper trading. Adds coherence gating to require expert alignment before trading.

#### Key Changes

**A. Coherence Gate (`hydra.rs`):**
- Added `coherence_min` config (default: 0.65)
- Coherence = |net_signal| / sum(|expert_contrib|)
- Skips trades when experts disagree (coherence < threshold)

```rust
let coherence = if abs_contrib_sum > 1e-9 {
    (portfolio_signal.abs() / abs_contrib_sum).clamp(0.0, 1.0)
} else { 0.0 };

if coherence < self.config.coherence_min {
    return;  // Experts disagree, skip trade
}
```

**B. MeanRev Saturation Fix (`hydra.rs`):**
- Replaced hard clamp with tanh() soft clipping
- Reduced liquidity boost from 1.5 to 1.2
- Added diagnostic logging

```rust
// Before: signal saturated at ~0.9 due to hard clamp
self.signal = ((combined) * liquidity_mult).clamp(-1.0, 1.0);

// After: soft clip preserves magnitude information
self.signal = (combined * liquidity_mult).tanh();
```

**C. Edge Scale Multiplier:**
- Added `edge_scale_mult` config (default: 1.0)
- Allows controlled experiments with heuristic edge estimates

**D. Simplified Cost Model:**
- Removed deprecated Binance tuning fields
- Cost model now: `total_cost = slippage_bps + commission_bps`
- Required edge: `hurdle * total_cost` (no min_edge_bps)

#### Configuration
```toml
[strategy.hydra]
coherence_min = 0.65       # Require 65% expert alignment
edge_scale_mult = 1.0      # No edge scaling (for experiments)
edge_hurdle_multiplier = 2.5
```

#### Exchange Profile Defaults
| Profile | coherence_min | Notes |
|---------|--------------|-------|
| BinanceSpot | 0.60 | Lower for more liquid markets |
| BinanceFutures | 0.60 | Lower for more liquid markets |
| ZerodhaFnO | 0.70 | Higher for options (lower churn) |
| Default | 0.65 | Balanced default |

#### Expected Behavior
- Trades only execute when experts agree on direction
- MeanRev signal now varies continuously (not stuck at 0.9)
- Lower false signal rate due to coherence filtering
- Diagnostic logs show: `[MEANREV] vwap_z=... signal=...`

---

## Planned Patches

### Shadow Calibration

**Status:** 🔜 Planned
**Priority:** High

#### Problem
The current "edge_bps" is a heuristic score conversion (e.g., `25.0 * signal.abs()`), not calibrated to actual expected return.

#### Proposed Changes
- Record all candidates (even rejected) with:
  - `net_signal`, `coherence`, `future_return_bps` over horizon H
- Fit monotonic mapping: `E[return | signal, coherence]`
- Replace heuristic edge with calibrated edge

#### Instrumentation to Add
```
candidates, rejected, passed
max_edge, p95_edge
max_edge_when_coherence>0.65
p95_edge_when_coherence>0.65
max_coherence observed
```

---

### Rank-Space Mean Reversion / Random Matrix Theory

**Status:** 📋 Research
**Priority:** Medium

#### Concept
Use Random Matrix Theory (RMT) to:
1. Identify eigenvalue outliers in correlation matrices
2. Filter noise from signal in high-dimensional return data
3. Detect regime changes via spectral analysis

#### Potential Implementation
- Compute rolling correlation matrix of returns
- Apply Marchenko-Pastur denoising
- Extract top eigenportfolios for mean reversion signals
- Use Tracy-Widom distribution for outlier detection

---

### Gamma Blast Strategy (Index Expiries)

**Status:** 📋 Research
**Priority:** High (for Indian market)

#### Concept
Exploit gamma explosion near expiry on Indian index options (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY).

#### Requirements
- All index expiries via UniverseSpec (✅ supported now)
- Real-time Greeks calculation (✅ Black-Scholes in kubera-options)
- Gamma exposure tracking per strike
- Pin risk detection near ATM

#### Target Indices
- NIFTY (Thursday expiry)
- BANKNIFTY (Wednesday expiry)
- FINNIFTY (Tuesday expiry)
- MIDCPNIFTY (Monday expiry)

---

## Patch Application Guide

### From ZIP Archive
```bash
# Extract patch
unzip /path/to/patch.zip -d /tmp/patch

# Compare changes
diff -rq /home/isoula/QuantKubera1/crates /tmp/patch/QuantKubera-main/crates

# Apply specific files
cp /tmp/patch/QuantKubera-main/path/to/file.rs /home/isoula/QuantKubera1/path/to/file.rs

# Build and verify
cargo build --release
cargo test
```

### Commit Convention
```
feat: Short description

Detailed explanation of changes.

- Bullet points for specific changes
- Another change

Usage:
  Example command here

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
```

---

## Patch Archive Location

All patch ZIP files are stored in:
```
/home/isoula/QuantKubera1/patches/
```

| File | Description | Applied |
|------|-------------|---------|
| `QuantKubera-main/` | Base snapshot (no changes) | - |
| `QuantKubera-universe-wal.patch.zip` | UniverseSpec WAL | ✅ |
| `QuantKubera-strategy-rd-patch.zip` | Coherence Gate & MeanRev Fix | ✅ |

---

*Last updated: 2026-01-21*
