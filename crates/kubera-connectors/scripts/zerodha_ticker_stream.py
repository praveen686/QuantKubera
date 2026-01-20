"""Zerodha KiteTicker stream -> JSON lines on stdout.

Phase Z0 design: use official python kiteconnect client to stream ticks.

Required env vars:
  - KITE_API_KEY
  - KITE_ACCESS_TOKEN

Usage:
  python3 zerodha_ticker_stream.py --tokens 256265,260105

Output (one JSON object per line):
  {
    "ts_ns": 173735...,
    "instrument_token": 256265,
    "tradingsymbol": "NIFTY24JANFUT",
    "last_price": 21455.2,
    "depth": {"buy": [{"price":...,"quantity":...},...], "sell": [...]}
  }

Notes:
  - Subscribes in FULL mode so that best-5 depth is available.
  - If depth is not present for a tick, it is omitted (null).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime


def _now_ns() -> int:
    # monotonic is not epoch; we need epoch ns
    return time.time_ns()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tokens", required=True, help="comma-separated instrument tokens")
    args = parser.parse_args()

    api_key = os.environ.get("KITE_API_KEY")
    access_token = os.environ.get("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        print(
            "Missing KITE_API_KEY or KITE_ACCESS_TOKEN env vars. "
            "Generate access token via zerodha_auth.py and export them.",
            file=sys.stderr,
        )
        return 2

    try:
        from kiteconnect import KiteTicker  # type: ignore
    except Exception as e:
        print(
            "kiteconnect is not installed. Install: pip install kiteconnect\n" + str(e),
            file=sys.stderr,
        )
        return 3

    tokens = [int(t) for t in args.tokens.split(",") if t.strip()]
    if not tokens:
        print("No tokens provided", file=sys.stderr)
        return 4

    kws = KiteTicker(api_key, access_token)

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_ticks(ws, ticks):
        ts_ns = _now_ns()
        for t in ticks:
            out = {
                "ts_ns": ts_ns,
                "instrument_token": t.get("instrument_token"),
                "tradingsymbol": t.get("tradingsymbol", ""),
                "last_price": t.get("last_price"),
                "depth": None,
            }
            depth = t.get("depth")
            if isinstance(depth, dict):
                # Kite format keys: 'buy' and 'sell', each list of dicts {'price','quantity','orders'}
                out["depth"] = {
                    "buy": [
                        {"price": lvl.get("price"), "quantity": lvl.get("quantity")}
                        for lvl in depth.get("buy", [])
                    ],
                    "sell": [
                        {"price": lvl.get("price"), "quantity": lvl.get("quantity")}
                        for lvl in depth.get("sell", [])
                    ],
                }

            sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()

    def on_error(ws, code, reason):
        print(f"KiteTicker error {code}: {reason}", file=sys.stderr)

    def on_close(ws, code, reason):
        print(f"KiteTicker closed {code}: {reason}", file=sys.stderr)

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_error = on_error
    kws.on_close = on_close

    kws.connect(threaded=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
