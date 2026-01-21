#!/usr/bin/env python3
"""
VectorBT Pro quick-testing harness using Binance klines.

Purpose
-------
Fast, cheap strategy logic validation (NOT execution realism).

Workflow
--------
1) Fetch klines for a symbol/timeframe into a local cache (CSV).
2) Build OHLCV DataFrame.
3) Run a small set of fast baseline strategies in VectorBT Pro:
   - Momentum breakout (close vs SMA)
   - Mean reversion (zscore vs rolling mean)
   - Volatility filter (ATR/realized vol gate)
4) Emit artifacts suitable for candidate screening:
   - equity curve CSV
   - summary.json (Sharpe, DD, trades, fees assumptions)
   - signals.csv (entry/exit booleans)

Notes
-----
- This harness is intentionally minimal and deterministic.
- It does NOT simulate bid/ask, partial fills, or multi-leg atomicity.
  For that, use KiteSim replay + execution tests.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import requests

# VectorBT Pro (paid package) import:
import vectorbtpro as vbt


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


@dataclass(frozen=True)
class FetchCfg:
    symbol: str
    interval: str
    start_ms: Optional[int]
    end_ms: Optional[int]
    limit: int = 1000


def _ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def fetch_binance_klines(cfg: FetchCfg) -> pd.DataFrame:
    """
    Fetch Binance klines in chunks (limit=1000 per request).
    Returns DataFrame indexed by UTC timestamp with OHLCV.
    """
    all_rows = []
    start = cfg.start_ms
    while True:
        params = {"symbol": cfg.symbol, "interval": cfg.interval, "limit": cfg.limit}
        if start is not None:
            params["startTime"] = start
        if cfg.end_ms is not None:
            params["endTime"] = cfg.end_ms

        r = requests.get(BINANCE_KLINES_URL, params=params, timeout=30)
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break

        all_rows.extend(rows)

        last_open_time = rows[-1][0]
        next_start = last_open_time + 1
        if start is not None and next_start <= start:
            break
        start = next_start

        # stop condition: if fewer than limit, we're done
        if len(rows) < cfg.limit:
            break

        # If end_ms set and last_open_time >= end_ms, stop
        if cfg.end_ms is not None and last_open_time >= cfg.end_ms:
            break

    if not all_rows:
        raise RuntimeError("No klines returned. Check symbol/interval/time range.")

    cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "num_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]
    df = pd.DataFrame(all_rows, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("open_time", drop=True)

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[["open", "high", "low", "close", "volume"]].dropna()
    return df


def cache_path(cache_dir: Path, symbol: str, interval: str, start_ms: Optional[int], end_ms: Optional[int]) -> Path:
    a = start_ms if start_ms is not None else "na"
    b = end_ms if end_ms is not None else "na"
    return cache_dir / f"binance_klines_{symbol}_{interval}_{a}_{b}.csv"


def load_or_fetch(cache_dir: Path, cfg: FetchCfg, force: bool) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = cache_path(cache_dir, cfg.symbol, cfg.interval, cfg.start_ms, cfg.end_ms)
    if p.exists() and not force:
        return pd.read_csv(p, parse_dates=["open_time"], index_col="open_time")
    df = fetch_binance_klines(cfg)
    df.to_csv(p, index_label="open_time")
    return df


def zscore(x: pd.Series, window: int) -> pd.Series:
    mu = x.rolling(window).mean()
    sd = x.rolling(window).std(ddof=0)
    return (x - mu) / (sd.replace(0.0, np.nan))


def realized_vol(returns: pd.Series, window: int) -> pd.Series:
    # annualization optional; we keep raw window vol for gating
    return returns.rolling(window).std(ddof=0)


def run_baselines(
    ohlcv: pd.DataFrame,
    fees_bps: float,
    slippage_bps: float,
    out_dir: Path,
) -> None:
    """
    Run quick baselines for candidate screening.
    Emits artifacts in out_dir.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    close = ohlcv["close"]
    ret = close.pct_change().fillna(0.0)

    # --- Strategy 1: Momentum breakout (close > SMA)
    sma_fast = close.rolling(20).mean()
    sma_slow = close.rolling(80).mean()
    entries_mom = sma_fast > sma_slow
    exits_mom = sma_fast < sma_slow

    # --- Strategy 2: Mean reversion (zscore)
    z = zscore(close, window=60)
    entries_mr = z < -1.0
    exits_mr = z > 0.0

    # --- Strategy 3: Volatility-gated momentum
    rv = realized_vol(ret, window=60)
    rv_q = rv.rolling(500).quantile(0.7)  # gate on "high vol" regime
    gate = rv > rv_q
    entries_vm = gate & entries_mom
    exits_vm = (~gate) | exits_mom

    # common fees in decimal:
    fees = (fees_bps + slippage_bps) / 1e4

    # Use from_signals for speed and clarity
    pf_mom = vbt.Portfolio.from_signals(close, entries_mom, exits_mom, fees=fees, freq="1T")
    pf_mr  = vbt.Portfolio.from_signals(close, entries_mr, exits_mr, fees=fees, freq="1T")
    pf_vm  = vbt.Portfolio.from_signals(close, entries_vm, exits_vm, fees=fees, freq="1T")

    # Save equity curves
    eq = pd.DataFrame({
        "close": close,
        "mom_equity": pf_mom.value,
        "mr_equity": pf_mr.value,
        "vm_equity": pf_vm.value,
    })
    eq.to_csv(out_dir / "equity.csv", index_label="ts")

    # Save signals for inspection
    sig = pd.DataFrame({
        "mom_entry": entries_mom.astype(int),
        "mom_exit": exits_mom.astype(int),
        "mr_entry": entries_mr.astype(int),
        "mr_exit": exits_mr.astype(int),
        "vm_entry": entries_vm.astype(int),
        "vm_exit": exits_vm.astype(int),
    }, index=close.index)
    sig.to_csv(out_dir / "signals.csv", index_label="ts")

    # Summaries
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "engine": "vectorbtpro",
        "fees_bps": fees_bps,
        "slippage_bps": slippage_bps,
        "strategies": {
            "momentum": _pf_stats(pf_mom),
            "mean_reversion": _pf_stats(pf_mr),
            "vol_gated_momentum": _pf_stats(pf_vm),
        }
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))


def _pf_stats(pf: "vbt.Portfolio") -> dict:
    # Use a small stable subset of metrics
    stats = pf.stats()
    keep = [
        "Total Return [%]",
        "Sharpe Ratio",
        "Max Drawdown [%]",
        "Total Trades",
        "Win Rate [%]",
        "Profit Factor",
    ]
    out = {}
    for k in keep:
        if k in stats.index:
            v = stats.loc[k]
            try:
                out[k] = float(v)
            except Exception:
                out[k] = str(v)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="e.g., BTCUSDT")
    ap.add_argument("--interval", default="1m", help="Binance kline interval (1m,5m,15m,1h,...)")
    ap.add_argument("--start", default=None, help="UTC start, e.g. 2026-01-01T00:00:00Z")
    ap.add_argument("--end", default=None, help="UTC end, e.g. 2026-01-03T00:00:00Z")
    ap.add_argument("--cache-dir", default="data/klines_cache", help="Cache folder inside repo")
    ap.add_argument("--out", default="artifacts/vbt_quicktest", help="Output folder")
    ap.add_argument("--fees-bps", type=float, default=1.0, help="Fees in bps (rough)")
    ap.add_argument("--slippage-bps", type=float, default=0.5, help="Slippage in bps (rough)")
    ap.add_argument("--force-fetch", action="store_true", help="Ignore cache and refetch")
    args = ap.parse_args()

    def parse_dt(s: Optional[str]) -> Optional[int]:
        if s is None:
            return None
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        return _ms(dt)

    cfg = FetchCfg(
        symbol=args.symbol.upper(),
        interval=args.interval,
        start_ms=parse_dt(args.start),
        end_ms=parse_dt(args.end),
    )
    cache_dir = Path(args.cache_dir)
    out_dir = Path(args.out) / f"{cfg.symbol}_{cfg.interval}"
    df = load_or_fetch(cache_dir, cfg, force=args.force_fetch)

    # Basic sanity
    if df.empty or df["close"].isna().all():
        raise RuntimeError("Empty or invalid klines dataframe")

    run_baselines(df, fees_bps=args.fees_bps, slippage_bps=args.slippage_bps, out_dir=out_dir)
    print(f"[OK] Wrote artifacts to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
