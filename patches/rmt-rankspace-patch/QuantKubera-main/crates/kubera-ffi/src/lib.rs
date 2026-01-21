//! FFI bindings to the C++ execution simulator.
//!
//! Phase 0 scope:
//! - L2 book snapshot input
//! - Market order multi-level fill
//! - Deterministic fees + latency

use thiserror::Error;

pub const DEPTH_MAX: usize = 10;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct KuberaL2Book {
    pub depth: i32,
    pub bid_px: [i64; DEPTH_MAX],
    pub bid_sz: [i64; DEPTH_MAX],
    pub ask_px: [i64; DEPTH_MAX],
    pub ask_sz: [i64; DEPTH_MAX],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct KuberaOrder {
    pub ts_ns: i64,
    pub symbol_id: i32,
    pub side: i32,
    pub ord_type: i32,
    pub qty: i64,
    pub max_slippage_bps: i64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct KuberaExecCfg {
    pub taker_fee_bps: i64,
    pub latency_ns: i64,
    pub min_notional_ticks: i64,
    pub allow_partial: i32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct KuberaFill {
    pub decision_ts_ns: i64,
    pub fill_ts_ns: i64,
    pub symbol_id: i32,
    pub side: i32,
    pub req_qty: i64,
    pub filled_qty: i64,
    pub vwap_px: i64,
    pub notional_ticks: i64,
    pub fee_quote_minor: i64,
    pub rc: i32,
    pub last_level_used: i32,
}

extern "C" {
    pub fn kubera_simulate_market_fill(
        book: *const KuberaL2Book,
        order: *const KuberaOrder,
        cfg: *const KuberaExecCfg,
        out_fill: *mut KuberaFill,
    ) -> i32;
}

#[derive(Debug, Clone, Copy)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    fn as_i32(self) -> i32 {
        match self {
            Side::Buy => 1,
            Side::Sell => 2,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MarketOrder {
    pub ts_ns: i64,
    pub symbol_id: i32,
    pub side: Side,
    pub qty: i64,
    pub max_slippage_bps: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecConfig {
    pub taker_fee_bps: i64,
    pub latency_ns: i64,
    pub allow_partial: bool,
}

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("bad input")]
    BadInput,
    #[error("no liquidity")]
    NoLiquidity,
    #[error("partial fill")]
    PartialFill,
    #[error("internal")]
    Internal,
}

#[derive(Debug, Clone, Copy)]
pub struct Fill {
    pub decision_ts_ns: i64,
    pub fill_ts_ns: i64,
    pub symbol_id: i32,
    pub side: Side,
    pub req_qty: i64,
    pub filled_qty: i64,
    pub vwap_px_ticks: i64,
    pub notional_ticks: i64,
    pub fee_quote_minor: i64,
    pub rc: i32,
    pub last_level_used: i32,
}

pub fn simulate_market_fill(book: &KuberaL2Book, ord: &MarketOrder, cfg: &ExecConfig) -> Fill {
    let order = KuberaOrder {
        ts_ns: ord.ts_ns,
        symbol_id: ord.symbol_id,
        side: ord.side.as_i32(),
        ord_type: 1,
        qty: ord.qty,
        max_slippage_bps: ord.max_slippage_bps.unwrap_or(-1),
    };

    let exec_cfg = KuberaExecCfg {
        taker_fee_bps: cfg.taker_fee_bps,
        latency_ns: cfg.latency_ns,
        min_notional_ticks: 0,
        allow_partial: if cfg.allow_partial { 1 } else { 0 },
    };

    let mut out = KuberaFill {
        decision_ts_ns: 0,
        fill_ts_ns: 0,
        symbol_id: 0,
        side: 0,
        req_qty: 0,
        filled_qty: 0,
        vwap_px: 0,
        notional_ticks: 0,
        fee_quote_minor: 0,
        rc: 0,
        last_level_used: -1,
    };

    unsafe {
        let _rc = kubera_simulate_market_fill(book as *const _, &order as *const _, &exec_cfg as *const _, &mut out as *mut _);
    }

    Fill {
        decision_ts_ns: out.decision_ts_ns,
        fill_ts_ns: out.fill_ts_ns,
        symbol_id: out.symbol_id,
        side: if out.side == 1 { Side::Buy } else { Side::Sell },
        req_qty: out.req_qty,
        filled_qty: out.filled_qty,
        vwap_px_ticks: out.vwap_px,
        notional_ticks: out.notional_ticks,
        fee_quote_minor: out.fee_quote_minor,
        rc: out.rc,
        last_level_used: out.last_level_used,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_level_vwap_buy() {
        let mut book = KuberaL2Book {
            depth: 2,
            bid_px: [0; DEPTH_MAX],
            bid_sz: [0; DEPTH_MAX],
            ask_px: [0; DEPTH_MAX],
            ask_sz: [0; DEPTH_MAX],
        };
        book.ask_px[0] = 100;
        book.ask_sz[0] = 5;
        book.ask_px[1] = 110;
        book.ask_sz[1] = 5;

        let ord = MarketOrder { ts_ns: 1, symbol_id: 1, side: Side::Buy, qty: 7, max_slippage_bps: None };
        let cfg = ExecConfig { taker_fee_bps: 10, latency_ns: 0, allow_partial: true };

        let fill = simulate_market_fill(&book, &ord, &cfg);
        assert_eq!(fill.filled_qty, 7);
        // notional = 5*100 + 2*110 = 720
        assert_eq!(fill.notional_ticks, 720);
        assert_eq!(fill.vwap_px_ticks, 720/7);
    }
}
