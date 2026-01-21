#pragma once

#include <cstdint>

#ifndef DEPTH_MAX
#define DEPTH_MAX 10
#endif

typedef enum {
  KUBERA_SIDE_BUY  = 1,
  KUBERA_SIDE_SELL = 2
} kubera_side_t;

typedef enum {
  KUBERA_ORD_MARKET = 1
} kubera_order_type_t;

typedef enum {
  KUBERA_OK = 0,
  KUBERA_ERR_BAD_INPUT = 1,
  KUBERA_ERR_NO_LIQUIDITY = 2,
  KUBERA_ERR_PARTIAL_FILL = 3,
  KUBERA_ERR_INTERNAL = 100
} kubera_rc_t;

typedef struct {
  int32_t depth;
  int64_t bid_px[DEPTH_MAX];
  int64_t bid_sz[DEPTH_MAX];
  int64_t ask_px[DEPTH_MAX];
  int64_t ask_sz[DEPTH_MAX];
} kubera_l2_book_t;

typedef struct {
  int64_t ts_ns;
  int32_t symbol_id;
  int32_t side;
  int32_t ord_type;
  int64_t qty;
  int64_t max_slippage_bps;
} kubera_order_t;

typedef struct {
  int64_t taker_fee_bps;
  int64_t latency_ns;
  int64_t min_notional_ticks;
  int32_t allow_partial;
} kubera_exec_cfg_t;

typedef struct {
  int64_t decision_ts_ns;
  int64_t fill_ts_ns;
  int32_t symbol_id;
  int32_t side;
  int64_t req_qty;
  int64_t filled_qty;
  int64_t vwap_px;
  int64_t notional_ticks;
  int64_t fee_quote_minor;
  int32_t rc;
  int32_t last_level_used;
} kubera_fill_t;

#ifdef __cplusplus
extern "C" {
#endif

int32_t kubera_simulate_market_fill(
  const kubera_l2_book_t* book,
  const kubera_order_t* order,
  const kubera_exec_cfg_t* cfg,
  kubera_fill_t* out_fill
);

#ifdef __cplusplus
}
#endif
