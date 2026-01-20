#include "kubera_executor.h"

static inline int64_t clamp_i64(int64_t x, int64_t lo, int64_t hi) {
  if (x < lo) return lo;
  if (x > hi) return hi;
  return x;
}

int32_t kubera_simulate_market_fill(
  const kubera_l2_book_t* book,
  const kubera_order_t* order,
  const kubera_exec_cfg_t* cfg,
  kubera_fill_t* out_fill
) {
  if (!book || !order || !cfg || !out_fill) {
    return (int32_t)KUBERA_ERR_BAD_INPUT;
  }

  // Basic validation
  if (book->depth <= 0 || book->depth > DEPTH_MAX) {
    out_fill->rc = (int32_t)KUBERA_ERR_BAD_INPUT;
    return out_fill->rc;
  }
  if (order->qty <= 0) {
    out_fill->rc = (int32_t)KUBERA_ERR_BAD_INPUT;
    return out_fill->rc;
  }
  if (!(order->side == (int32_t)KUBERA_SIDE_BUY || order->side == (int32_t)KUBERA_SIDE_SELL)) {
    out_fill->rc = (int32_t)KUBERA_ERR_BAD_INPUT;
    return out_fill->rc;
  }

  out_fill->decision_ts_ns = order->ts_ns;
  out_fill->fill_ts_ns = order->ts_ns + cfg->latency_ns;
  out_fill->symbol_id = order->symbol_id;
  out_fill->side = order->side;
  out_fill->req_qty = order->qty;
  out_fill->filled_qty = 0;
  out_fill->vwap_px = 0;
  out_fill->notional_ticks = 0;
  out_fill->fee_quote_minor = 0;
  out_fill->last_level_used = -1;
  out_fill->rc = (int32_t)KUBERA_OK;

  int64_t remaining = order->qty;
  int64_t notional = 0;
  int64_t filled = 0;

  if (order->side == (int32_t)KUBERA_SIDE_BUY) {
    for (int i = 0; i < book->depth && remaining > 0; i++) {
      int64_t px = book->ask_px[i];
      int64_t sz = book->ask_sz[i];
      if (px <= 0 || sz <= 0) continue;
      int64_t dq = (remaining < sz) ? remaining : sz;
      notional += px * dq;
      filled += dq;
      remaining -= dq;
      out_fill->last_level_used = i;
    }
  } else {
    for (int i = 0; i < book->depth && remaining > 0; i++) {
      int64_t px = book->bid_px[i];
      int64_t sz = book->bid_sz[i];
      if (px <= 0 || sz <= 0) continue;
      int64_t dq = (remaining < sz) ? remaining : sz;
      notional += px * dq;
      filled += dq;
      remaining -= dq;
      out_fill->last_level_used = i;
    }
  }

  if (filled <= 0) {
    out_fill->rc = (int32_t)KUBERA_ERR_NO_LIQUIDITY;
    return out_fill->rc;
  }

  out_fill->filled_qty = filled;
  out_fill->notional_ticks = notional;
  out_fill->vwap_px = notional / filled; // integer VWAP in ticks

  // Fee model: fee_quote_minor is represented in "ticks*qty" space for now.
  // Rust should convert this using symbol metadata. Here we keep it deterministic:
  // fee = notional * (taker_fee_bps / 10000)
  // Use integer rounding down.
  if (cfg->taker_fee_bps > 0) {
    out_fill->fee_quote_minor = (notional * cfg->taker_fee_bps) / 10000;
  }

  // Partial fill handling
  if (filled < order->qty) {
    if (cfg->allow_partial) {
      out_fill->rc = (int32_t)KUBERA_ERR_PARTIAL_FILL;
    } else {
      // strict mode: reject entirely
      out_fill->filled_qty = 0;
      out_fill->vwap_px = 0;
      out_fill->notional_ticks = 0;
      out_fill->fee_quote_minor = 0;
      out_fill->rc = (int32_t)KUBERA_ERR_NO_LIQUIDITY;
    }
  }

  return out_fill->rc;
}
