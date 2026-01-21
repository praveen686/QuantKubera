"""Fetch quote snapshots for a Zerodha options universe.

Phase Z0 intent:
 - Read the options universe CSV produced by zerodha_options_universe.py
 - Build a selection set (nearest expiry, ATM +/- K strikes)
 - Poll kite.quote() in batches and emit time-series snapshots

This writes JSONL by default so it can be ingested into the QuantKubera WAL
layer in a follow-on step.

Required env vars:
  - KITE_API_KEY
  - KITE_ACCESS_TOKEN

Example:
  python3 zerodha_options_quotes.py \
    --universe options_universe.csv \
    --underlying NIFTY \
    --expiry-nearest 1 \
    --strikes 20 \
    --interval-seconds 2 \
    --out quotes_nifty.jsonl
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
from typing import Dict, List


def require_env(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        print(f"Missing required env var: {key}", file=sys.stderr)
        raise SystemExit(2)
    return v


def load_universe(path: str) -> List[Dict[str, str]]:
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f)
        rows = [row for row in r]
    return rows


def parse_expiry(s: str) -> dt.date:
    # instruments() expiry can be like '2026-01-29'
    return dt.date.fromisoformat(s)


def pick_contracts(rows: List[Dict[str, str]], underlying: str, expiry_nearest: int, strikes: int) -> List[Dict[str, str]]:
    rows = [r for r in rows if r.get("name") == underlying]
    for r in rows:
        r["expiry_date"] = parse_expiry(r["expiry"]).toordinal()
        r["strike_f"] = float(r["strike"] or 0)

    expiries = sorted({r["expiry_date"] for r in rows})
    if not expiries:
        raise SystemExit(f"No contracts for underlying {underlying}")
    expiry_nearest = max(1, expiry_nearest)
    idx = min(expiry_nearest - 1, len(expiries) - 1)
    target_exp = expiries[idx]

    rows = [r for r in rows if r["expiry_date"] == target_exp]

    # Approx ATM: pick the median strike (better: compute from spot LTP, but we avoid leakage and extra calls here)
    strikes_sorted = sorted({r["strike_f"] for r in rows if r["strike_f"] > 0})
    if not strikes_sorted:
        raise SystemExit("No strikes found")
    atm = strikes_sorted[len(strikes_sorted) // 2]

    # Select ± strikes steps based on unique strikes ordering.
    # Here, `strikes` means count on each side.
    atm_idx = min(range(len(strikes_sorted)), key=lambda i: abs(strikes_sorted[i] - atm))
    lo = max(0, atm_idx - strikes)
    hi = min(len(strikes_sorted) - 1, atm_idx + strikes)
    chosen_strikes = set(strikes_sorted[lo : hi + 1])

    picked = [r for r in rows if r["strike_f"] in chosen_strikes]
    return picked


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--underlying", required=True)
    ap.add_argument("--expiry-nearest", type=int, default=1)
    ap.add_argument("--strikes", type=int, default=20)
    ap.add_argument("--interval-seconds", type=float, default=2.0)
    ap.add_argument("--seconds", type=int, default=600)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    api_key = require_env("KITE_API_KEY")
    access_token = require_env("KITE_ACCESS_TOKEN")

    try:
        from kiteconnect import KiteConnect
    except Exception as e:
        print("kiteconnect is required: pip install kiteconnect", file=sys.stderr)
        raise

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    rows = load_universe(args.universe)
    picked = pick_contracts(rows, args.underlying, args.expiry_nearest, args.strikes)

    # Zerodha quote() expects fully qualified symbols like "NFO:...".
    # Instruments master field 'tradingsymbol' is present as 'tradingsymbol'.
    syms = [f"NFO:{r['tradingsymbol']}" for r in picked if r.get("tradingsymbol")]
    if not syms:
        print("No tradingsymbols found in universe", file=sys.stderr)
        return 2

    print(f"Polling {len(syms)} option symbols; writing JSONL -> {args.out}")

    start = time.time()
    with open(args.out, "w") as f:
        while time.time() - start < args.seconds:
            # Batch to avoid URL length and rate limits.
            batch_size = 50
            ts = dt.datetime.utcnow().isoformat() + "Z"
            for i in range(0, len(syms), batch_size):
                batch = syms[i : i + batch_size]
                try:
                    q = kite.quote(batch)
                except Exception as e:
                    print(f"quote() error: {e}", file=sys.stderr)
                    time.sleep(max(1.0, args.interval_seconds))
                    continue

                out = {"ts": ts, "quotes": q}
                f.write(json.dumps(out) + "\n")
                f.flush()

            time.sleep(args.interval_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
