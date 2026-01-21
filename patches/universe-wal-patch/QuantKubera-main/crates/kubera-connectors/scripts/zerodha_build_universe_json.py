"""Build a canonical Zerodha universe specification JSON (UniverseSpecV1).

Why this exists
--------------
QuantKubera's WAL replay relies on deterministic symbol/token/lot_size mapping.
This script produces a single JSON file that:

  - Lists the exact symbols you intend to trade/record
  - Includes per-symbol instrument metadata (token, lot_size, expiry, strike, etc.)

The recorder (`record_zerodha_wal_l2.rs`) can then embed this spec into the WAL
as metadata (`universe_spec_v1`). Every backtest run has a guaranteed arming
point and stable replay, even if live L2 updates are sparse.

Required env vars:
  - KITE_API_KEY
  - KITE_ACCESS_TOKEN

Examples
--------

All index futures:
  python3 zerodha_build_universe_json.py --indices NIFTY,BANKNIFTY,FINNIFTY,MIDCPNIFTY --mode fut --out universe.json

Nearest expiry options near ATM (lightweight):
  python3 zerodha_build_universe_json.py --indices NIFTY,BANKNIFTY --mode opt --near-atm 8 --out universe.json

Both FUT + options (heavier):
  python3 zerodha_build_universe_json.py --indices NIFTY,BANKNIFTY --mode fut+opt --near-atm 10 --out universe.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--indices", default="NIFTY,BANKNIFTY", help="Comma-separated index underlyings")
    ap.add_argument(
        "--mode",
        default="fut",
        choices=["fut", "opt", "fut+opt"],
        help="Which instruments to include",
    )
    ap.add_argument(
        "--near-atm",
        type=int,
        default=0,
        help="For options: include only strikes within N steps of ATM per expiry (0 = include all)",
    )
    ap.add_argument(
        "--max-expiries",
        type=int,
        default=1,
        help="For options: number of nearest expiries per index to include",
    )
    ap.add_argument("--out", default="universe.json")
    args = ap.parse_args()

    api_key = _require_env("KITE_API_KEY")
    access_token = _require_env("KITE_ACCESS_TOKEN")

    try:
        from kiteconnect import KiteConnect
    except Exception:
        print("kiteconnect is not installed. Install: pip install kiteconnect", file=sys.stderr)
        return 2

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    indices = {x.strip().upper() for x in args.indices.split(",") if x.strip()}
    rows = kite.instruments("NFO")

    # Partition by underlying
    by_name: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        name = (r.get("name") or "").upper()
        if name not in indices:
            continue
        by_name[name].append(r)

    selected_symbols: list[str] = []
    instruments: dict[str, dict] = {}

    for name, items in by_name.items():
        # Futures
        if args.mode in ("fut", "fut+opt"):
            futs = [r for r in items if (r.get("instrument_type") or "").upper() == "FUT"]
            # Prefer nearest expiry futures per underlying
            futs.sort(key=lambda x: x.get("expiry") or "")
            for r in futs[:1]:
                sym = r.get("tradingsymbol")
                if not sym:
                    continue
                selected_symbols.append(sym)
                instruments[sym] = {
                    "token": r.get("instrument_token"),
                    "exchange": r.get("exchange"),
                    "segment": r.get("segment"),
                    "name": name,
                    "instrument_type": "FUT",
                    "expiry": r.get("expiry"),
                    "lot_size": r.get("lot_size"),
                    "tick_size": r.get("tick_size"),
                }

        # Options
        if args.mode in ("opt", "fut+opt"):
            opts = [
                r
                for r in items
                if (r.get("instrument_type") or "").upper() in ("CE", "PE")
            ]
            # Group by expiry
            expiries = sorted({r.get("expiry") for r in opts if r.get("expiry")})
            expiries = expiries[: max(1, args.max_expiries)]
            for ex in expiries:
                ex_rows = [r for r in opts if r.get("expiry") == ex]
                # Optional near-ATM filter: approximate ATM by median strike
                strikes = sorted({float(r.get("strike") or 0.0) for r in ex_rows})
                if args.near_atm and strikes:
                    mid = strikes[len(strikes) // 2]
                    # find index of mid in strikes
                    i = min(range(len(strikes)), key=lambda k: abs(strikes[k] - mid))
                    lo = max(0, i - args.near_atm)
                    hi = min(len(strikes), i + args.near_atm + 1)
                    keep_strikes = set(strikes[lo:hi])
                    ex_rows = [r for r in ex_rows if float(r.get("strike") or 0.0) in keep_strikes]
                for r in ex_rows:
                    sym = r.get("tradingsymbol")
                    if not sym:
                        continue
                    selected_symbols.append(sym)
                    instruments[sym] = {
                        "token": r.get("instrument_token"),
                        "exchange": r.get("exchange"),
                        "segment": r.get("segment"),
                        "name": name,
                        "instrument_type": r.get("instrument_type"),
                        "expiry": r.get("expiry"),
                        "strike": r.get("strike"),
                        "lot_size": r.get("lot_size"),
                        "tick_size": r.get("tick_size"),
                    }

    selected_symbols = sorted(set(selected_symbols))
    spec = {
        "version": "v1",
        "venue": "zerodha",
        "name": "index_universe",
        "created_utc": datetime.utcnow().isoformat() + "Z",
        "symbols": selected_symbols,
        "instruments": instruments,
    }

    with open(args.out, "w") as f:
        json.dump(spec, f, indent=2, sort_keys=True)
    print(f"Wrote universe spec: {args.out} (symbols={len(selected_symbols)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
