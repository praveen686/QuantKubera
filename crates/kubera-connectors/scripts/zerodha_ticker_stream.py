"""Zerodha KiteTicker stream -> JSON lines on stdout.

Phase Z0 design: use official python kiteconnect client to stream ticks.

Required env vars:
  - KITE_API_KEY
  - KITE_ACCESS_TOKEN

Usage:
  python3 zerodha_ticker_stream.py --tokens 256265,260105

Output (one JSON object per line):
  First line is metadata with instrument info:
  {
    "type": "metadata",
    "instruments": {
      "NIFTY24JANFUT": {"token": 256265, "lot_size": 65, "exchange": "NFO"},
      ...
    }
  }

  Subsequent lines are tick data:
  {
    "type": "tick",
    "ts_ns": 173735...,
    "instrument_token": 256265,
    "tradingsymbol": "NIFTY24JANFUT",
    "last_price": 21455.2,
    "depth": {"buy": [{"price":...,"quantity":...},...], "sell": [...]}
  }

Notes:
  - Subscribes in FULL mode so that best-5 depth is available.
  - If depth is not present for a tick, it is omitted (null).
  - Builds token->symbol mapping at startup from instruments API.
  - Outputs lot_size per instrument for NSE F&O position sizing.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Dict


def _now_ns() -> int:
    # monotonic is not epoch; we need epoch ns
    return time.time_ns()


def build_instrument_info(kite, tokens: list[int]) -> tuple[Dict[int, str], Dict[str, dict]]:
    """Build instrument mappings from Zerodha instruments API.

    Returns:
        token_to_symbol: Dict[int, str] - token -> tradingsymbol mapping
        instrument_info: Dict[str, dict] - symbol -> {token, lot_size, exchange} mapping
    """
    token_set = set(tokens)
    token_to_symbol: Dict[int, str] = {}
    instrument_info: Dict[str, dict] = {}

    # Fetch instruments from all relevant exchanges
    for exchange in ["NFO", "NSE", "BFO", "BSE"]:
        try:
            instruments = kite.instruments(exchange)
            for inst in instruments:
                token = inst.get("instrument_token")
                if token in token_set:
                    symbol = inst.get("tradingsymbol", "")
                    lot_size = inst.get("lot_size", 1)
                    token_to_symbol[token] = symbol
                    instrument_info[symbol] = {
                        "token": token,
                        "lot_size": lot_size,
                        "exchange": exchange,
                    }
            # Early exit if we found all tokens
            if len(token_to_symbol) == len(token_set):
                break
        except Exception as e:
            print(f"Warning: failed to fetch {exchange} instruments: {e}", file=sys.stderr)

    # Log any missing mappings
    for token in tokens:
        if token not in token_to_symbol:
            print(f"Warning: no symbol found for token {token}", file=sys.stderr)
            symbol = f"TOKEN_{token}"
            token_to_symbol[token] = symbol
            instrument_info[symbol] = {"token": token, "lot_size": 1, "exchange": "UNKNOWN"}

    return token_to_symbol, instrument_info


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
        from kiteconnect import KiteConnect, KiteTicker  # type: ignore
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

    # Build token -> symbol mapping and instrument info at startup
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    token_to_symbol, instrument_info = build_instrument_info(kite, tokens)
    print(f"Token mapping: {token_to_symbol}", file=sys.stderr)
    print(f"Instrument info: {instrument_info}", file=sys.stderr)

    # Output metadata JSON as first line (for Rust runner to parse lot sizes)
    metadata = {
        "type": "metadata",
        "instruments": instrument_info,
    }
    sys.stdout.write(json.dumps(metadata) + "\n")
    sys.stdout.flush()

    kws = KiteTicker(api_key, access_token)

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_ticks(ws, ticks):
        ts_ns = _now_ns()
        for t in ticks:
            inst_token = t.get("instrument_token")
            # Use mapping if tradingsymbol is missing from tick
            symbol = t.get("tradingsymbol") or token_to_symbol.get(inst_token, "")

            out = {
                "type": "tick",
                "ts_ns": ts_ns,
                "instrument_token": inst_token,
                "tradingsymbol": symbol,
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
