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

# Silence pandas FutureWarning about fillna downcasting
pd.set_option('future.no_silent_downcasting', True)

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


# ----------------------------
# HYDRA expert ensemble (6 experts)
# ----------------------------

def rolling_percentile_rank(x: pd.Series, window: int) -> pd.Series:
    """Percentile rank of the latest value within the rolling window (0..1)."""
    def _pr(a: np.ndarray) -> float:
        if a.size == 0:
            return np.nan
        last = a[-1]
        return float(np.sum(a <= last)) / float(a.size)
    return x.rolling(window, min_periods=window).apply(_pr, raw=True)


def hydra_experts(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Compute HYDRA expert signals as continuous scores in [-1, +1].
    6 experts: Trend, MeanRev, Volatility, Microstructure, RelativeValue, RankSpaceMeanRev.
    These are intentionally simple and fast for klines-based screening.
    """
    close = ohlcv["close"]
    ret = close.pct_change().fillna(0.0)
    vol = realized_vol(ret, window=60)

    # Expert-A (Trend): SMA crossover -> signed score
    sma_fast = close.rolling(20).mean()
    sma_slow = close.rolling(80).mean()
    a = np.tanh(((sma_fast - sma_slow) / close).replace([np.inf, -np.inf], np.nan).fillna(0.0) * 50.0)

    # Expert-B (MeanRev): zscore mean reversion
    z = zscore(close, window=60).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    b = np.tanh(-z / 2.0)

    # Expert-C (Volatility): high vol -> risk-off (negative), low vol -> risk-on (positive)
    vol_q = vol.rolling(500).quantile(0.7).replace([np.inf, -np.inf], np.nan)
    c_raw = (vol_q - vol).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    c = np.tanh(c_raw * 20.0)

    # Expert-D (Microstructure proxy): volume+range expansion
    rng = (ohlcv["high"] - ohlcv["low"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    vr = (rng / close).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    v = ohlcv["volume"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    v_z = zscore(v, window=120).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    d = np.tanh((v_z + (vr * 50.0)) / 3.0)

    # Expert-E (RelValue proxy): deviation from rolling VWAP-like mean
    vwap_like = (ohlcv["close"] * ohlcv["volume"]).rolling(120).sum() / (ohlcv["volume"].rolling(120).sum().replace(0.0, np.nan))
    vwap_like = vwap_like.replace([np.inf, -np.inf], np.nan).bfill().fillna(close)
    dev = ((close - vwap_like) / close).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    e = np.tanh(-dev * 80.0)

    # Expert-F (RankSpaceMR): rolling percentile rank -> mean reversion
    pr = rolling_percentile_rank(close, window=240).replace([np.inf, -np.inf], np.nan).fillna(0.5)
    # High rank -> short, low rank -> long
    f = np.tanh((0.5 - pr) * 6.0)

    return pd.DataFrame({
        "A_trend": a,
        "B_meanrev": b,
        "C_vol": c,
        "D_micro": d,
        "E_rel": e,
        "F_rank_mr": f,
    }, index=close.index)


def hydra_ensemble_score(experts: pd.DataFrame, weights: Optional[np.ndarray] = None) -> pd.Series:
    """
    Weighted ensemble score in [-1, +1].
    Default weights are equal.
    """
    X = experts.values
    if weights is None:
        w = np.ones(X.shape[1], dtype=float) / float(X.shape[1])
    else:
        w = np.asarray(weights, dtype=float)
        w = w / (np.sum(np.abs(w)) + 1e-12)
    s = X @ w
    s = np.clip(s, -1.0, 1.0)
    return pd.Series(s, index=experts.index, name="hydra_score")


def score_to_signals(score: pd.Series, threshold: float) -> Tuple[pd.Series, pd.Series]:
    """
    Convert continuous score to entry/exit signals for VectorBT:
    - Long when score > threshold
    - Flat when score falls below 0
    - (Optional) short can be added later; for now we keep long-only for safety.
    """
    long = score > threshold
    exit_ = score < 0.0
    long_prev = long.shift(1).fillna(False).astype(bool)
    entries = long & (~long_prev)
    exits = exit_ & long_prev
    return entries, exits


def fetch_binance_exchangeinfo_specs(symbol: str) -> Tuple[float, float]:
    """
    Fetch (tickSize, stepSize) for symbol from Binance exchangeInfo.
    Returns (tick_size, step_size) as floats.
    """
    url = "https://api.binance.com/api/v3/exchangeInfo"
    r = requests.get(url, params={"symbol": symbol.upper()}, timeout=30)
    r.raise_for_status()
    info = r.json()
    syms = info.get("symbols", [])
    if not syms:
        raise RuntimeError(f"exchangeInfo: symbol not found: {symbol}")
    filters = syms[0].get("filters", [])
    tick = None
    step = None
    for f in filters:
        ft = f.get("filterType")
        if ft == "PRICE_FILTER":
            tick = float(f["tickSize"])
        elif ft == "LOT_SIZE":
            step = float(f["stepSize"])
    if tick is None:
        tick = 0.01
    if step is None:
        step = 1.0
    return tick, step


def qty_scale_from_step(step_size: float) -> int:
    """
    Convert step_size into a power-of-10 fixed-point scale if possible.
    Example: 0.001 -> 1000
    """
    # Represent as string to count decimals similarly to Rust implementation
    s = f"{step_size:.16f}".rstrip("0").rstrip(".")
    if "." not in s:
        return 1
    dec = len(s.split(".")[1])
    return int(10 ** dec)


def emit_orders_json(
    out_path: Path,
    strategy_name: str,
    symbol: str,
    exchange: str,
    side: str,
    quantity_u32: int,
) -> None:
    """
    Emit an OrderFile JSON compatible with kubera-runner backtest-kitesim.
    This emits ONE MultiLegOrder with ONE leg (spot smoke / intent test).
    """
    order = {
        "strategy_name": strategy_name,
        "legs": [
            {
                "tradingsymbol": symbol,
                "exchange": exchange,
                "side": side,
                "quantity": int(quantity_u32),
                "order_type": "Market",
                "price": None,
            }
        ],
        "total_margin_required": 0.0,
    }
    payload = {"strategy_name": strategy_name, "orders": [order]}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))


def run_hydra_quicktest(
    ohlcv: pd.DataFrame,
    fees_bps: float,
    slippage_bps: float,
    out_dir: Path,
    threshold: float,
    emit_orders: bool,
    orders_out: Optional[Path],
    orders_qty_base: float,
    venue_exchange: str,
) -> None:
    """
    HYDRA quick test:
    - compute experts + ensemble score
    - run long-only VBT portfolio using score thresholding
    - emit artifacts and (optionally) one orders.json for KiteSim smoke validation
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    close = ohlcv["close"]
    experts = hydra_experts(ohlcv)
    score = hydra_ensemble_score(experts)

    entries, exits = score_to_signals(score, threshold=threshold)

    fees = (fees_bps + slippage_bps) / 1e4
    pf = vbt.Portfolio.from_signals(close, entries, exits, fees=fees, freq="1T")

    # artifacts
    experts.to_csv(out_dir / "experts.csv", index_label="ts")
    pd.DataFrame({"close": close, "hydra_score": score}).to_csv(out_dir / "score.csv", index_label="ts")
    pd.DataFrame({"entry": entries.astype(int), "exit": exits.astype(int)}).to_csv(out_dir / "signals.csv", index_label="ts")

    eq = pd.DataFrame({"close": close, "equity": pf.value})
    eq.to_csv(out_dir / "equity.csv", index_label="ts")

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "engine": "vectorbtpro",
        "mode": "hydra",
        "fees_bps": fees_bps,
        "slippage_bps": slippage_bps,
        "threshold": threshold,
        "stats": _pf_stats(pf),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    if emit_orders:
        if orders_out is None:
            orders_out = out_dir / "orders.json"
        # Determine trade direction from latest score
        last = float(score.iloc[-1])
        if last > threshold:
            side = "Buy"
        elif last < -threshold:
            side = "Sell"
        else:
            # No clear intent; do not emit orders
            return

        # Determine qty_scale from exchangeInfo, convert base qty to internal u32
        try:
            _, step = fetch_binance_exchangeinfo_specs(symbol=str(ohlcv.attrs.get("symbol", "BTCUSDT")))
        except Exception:
            step = 1.0
        scale = qty_scale_from_step(step)
        qty_u32 = int(round(orders_qty_base * scale))
        if qty_u32 <= 0:
            qty_u32 = scale  # minimum 1 step

        emit_orders_json(
            out_path=orders_out,
            strategy_name=summary.get("mode", "HYDRA").upper(),
            symbol=str(ohlcv.attrs.get("symbol", "BTCUSDT")),
            exchange=venue_exchange,
            side=side,
            quantity_u32=qty_u32,
        )


def emit_intents_json(
    out_path: Path,
    strategy_name: str,
    symbol: str,
    exchange: str,
    entries: pd.Series,
    exits: pd.Series,
    quantity_u32: int,
) -> None:
    """
    Emit an OrderIntentFile JSON compatible with kubera-runner backtest-kitesim --intents.
    Each entry/exit signal becomes a timestamped intent.
    """
    intents = []

    # Find entry points (signal goes True)
    entry_times = entries.index[entries].tolist()
    for ts in entry_times:
        intents.append({
            "ts": ts.isoformat(),
            "order": {
                "strategy_name": strategy_name,
                "legs": [{
                    "tradingsymbol": symbol,
                    "exchange": exchange,
                    "side": "Buy",
                    "quantity": int(quantity_u32),
                    "order_type": "Market",
                    "price": None,
                }],
                "total_margin_required": 0.0,
            }
        })

    # Find exit points (signal goes True)
    exit_times = exits.index[exits].tolist()
    for ts in exit_times:
        intents.append({
            "ts": ts.isoformat(),
            "order": {
                "strategy_name": strategy_name,
                "legs": [{
                    "tradingsymbol": symbol,
                    "exchange": exchange,
                    "side": "Sell",
                    "quantity": int(quantity_u32),
                    "order_type": "Market",
                    "price": None,
                }],
                "total_margin_required": 0.0,
            }
        })

    # Sort by timestamp
    intents.sort(key=lambda x: x["ts"])

    payload = {"strategy_name": strategy_name, "intents": intents}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))


def run_hydra_quicktest_with_intents(
    ohlcv: pd.DataFrame,
    fees_bps: float,
    slippage_bps: float,
    out_dir: Path,
    threshold: float,
    emit_intents: bool,
    intents_out: Optional[Path],
    orders_qty_base: float,
    venue_exchange: str,
) -> None:
    """
    HYDRA quick test with scheduled intents output.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    close = ohlcv["close"]
    experts = hydra_experts(ohlcv)
    score = hydra_ensemble_score(experts)

    entries, exits = score_to_signals(score, threshold=threshold)

    fees = (fees_bps + slippage_bps) / 1e4
    pf = vbt.Portfolio.from_signals(close, entries, exits, fees=fees, freq="1T")

    # artifacts
    experts.to_csv(out_dir / "experts.csv", index_label="ts")
    pd.DataFrame({"close": close, "hydra_score": score}).to_csv(out_dir / "score.csv", index_label="ts")
    pd.DataFrame({"entry": entries.astype(int), "exit": exits.astype(int)}).to_csv(out_dir / "signals.csv", index_label="ts")

    eq = pd.DataFrame({"close": close, "equity": pf.value})
    eq.to_csv(out_dir / "equity.csv", index_label="ts")

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "engine": "vectorbtpro",
        "mode": "hydra",
        "fees_bps": fees_bps,
        "slippage_bps": slippage_bps,
        "threshold": threshold,
        "stats": _pf_stats(pf),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    if emit_intents:
        if intents_out is None:
            intents_out = out_dir / "intents.json"

        # Check if there are any signals
        if not entries.any() and not exits.any():
            print("[WARN] No entry/exit signals found; intents.json not emitted.")
            return

        # Determine qty_scale from exchangeInfo
        try:
            _, step = fetch_binance_exchangeinfo_specs(symbol=str(ohlcv.attrs.get("symbol", "BTCUSDT")))
        except Exception:
            step = 1.0
        scale = qty_scale_from_step(step)
        qty_u32 = int(round(orders_qty_base * scale))
        if qty_u32 <= 0:
            qty_u32 = scale

        emit_intents_json(
            out_path=intents_out,
            strategy_name=summary.get("mode", "HYDRA").upper(),
            symbol=str(ohlcv.attrs.get("symbol", "BTCUSDT")),
            exchange=venue_exchange,
            entries=entries,
            exits=exits,
            quantity_u32=qty_u32,
        )


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
    ap.add_argument("--mode", default="hydra", choices=["hydra", "baselines"], help="Run HYDRA or baseline strategies")
    ap.add_argument("--threshold", type=float, default=0.15, help="HYDRA score threshold for long entry")
    ap.add_argument("--emit-orders", action="store_true", help="Emit orders.json compatible with backtest-kitesim (single-leg intent)")
    ap.add_argument("--emit-intents", action="store_true", help="Emit intents.json with timestamped entry/exit orders for scheduled execution")
    ap.add_argument("--orders-out", default=None, help="Path to write orders.json (default: <out_dir>/orders.json)")
    ap.add_argument("--intents-out", default=None, help="Path to write intents.json (default: <out_dir>/intents.json)")
    ap.add_argument("--orders-qty-base", type=float, default=0.001, help="Base quantity for orders (e.g., 0.001 BTC). Will be quantized by stepSize.")
    ap.add_argument("--venue-exchange", default="BINANCE", help="Exchange field to put into LegOrder.exchange")

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
    df.attrs['symbol'] = cfg.symbol

    # Basic sanity
    if df.empty or df["close"].isna().all():
        raise RuntimeError("Empty or invalid klines dataframe")

    if args.mode == 'baselines':
        run_baselines(df, fees_bps=args.fees_bps, slippage_bps=args.slippage_bps, out_dir=out_dir)
    elif args.emit_intents:
        intents_out = Path(args.intents_out) if args.intents_out is not None else None
        run_hydra_quicktest_with_intents(
            df,
            fees_bps=args.fees_bps,
            slippage_bps=args.slippage_bps,
            out_dir=out_dir,
            threshold=args.threshold,
            emit_intents=True,
            intents_out=intents_out,
            orders_qty_base=args.orders_qty_base,
            venue_exchange=args.venue_exchange,
        )
    else:
        orders_out = Path(args.orders_out) if args.orders_out is not None else None
        run_hydra_quicktest(
            df,
            fees_bps=args.fees_bps,
            slippage_bps=args.slippage_bps,
            out_dir=out_dir,
            threshold=args.threshold,
            emit_orders=args.emit_orders,
            orders_out=orders_out,
            orders_qty_base=args.orders_qty_base,
            venue_exchange=args.venue_exchange,
        )
    print(f"[OK] Wrote artifacts to: {out_dir}")
    if args.emit_orders:
        print("[OK] orders.json emitted (if score exceeded threshold).")
    if args.emit_intents:
        print("[OK] intents.json emitted (if signals found).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
