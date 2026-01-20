"""Fetch and build a Zerodha options universe from the instruments master.

This is a production-safe Phase Z0 utility:
 - Downloads (or reads cached) instruments master
 - Filters to NFO options for selected underlyings
 - Emits a compact universe file (CSV) suitable for quote polling + selection

Required env vars:
  - KITE_API_KEY
  - KITE_ACCESS_TOKEN

Example:
  python3 zerodha_options_universe.py --underlying NIFTY,BANKNIFTY --out options_universe.csv
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--underlying", default="NIFTY,BANKNIFTY")
    ap.add_argument("--out", default="options_universe.csv")
    args = ap.parse_args()

    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        print("Missing KITE_API_KEY or KITE_ACCESS_TOKEN", file=sys.stderr)
        return 2

    try:
        from kiteconnect import KiteConnect
    except Exception:
        print("kiteconnect is not installed. Install: pip install kiteconnect", file=sys.stderr)
        return 2

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    underlyings = {u.strip().upper() for u in args.underlying.split(",") if u.strip()}
    rows = kite.instruments("NFO")

    # Keep only options, and only requested underlyings.
    keep = []
    for r in rows:
        if r.get("instrument_type") not in ("CE", "PE"):
            continue
        name = (r.get("name") or "").upper()
        if name not in underlyings:
            continue
        keep.append(r)

    # Write CSV
    import csv

    # Minimal stable columns
    cols = [
        "instrument_token",
        "tradingsymbol",
        "name",
        "expiry",
        "strike",
        "instrument_type",
        "lot_size",
        "tick_size",
    ]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in keep:
            out = {c: r.get(c) for c in cols}
            w.writerow(out)

    print(f"Wrote {len(keep)} option contracts to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
