//! # Options Ledger: Position & PnL Tracking
//!
//! Per Z-Phase spec section 8: Required even if settlement is later phase.
//!
//! ## Position Model (8.1)
//! Per instrument:
//! - net_qty (signed, lots)
//! - avg_price
//! - realized_pnl
//! - unrealized_pnl (MTM on mid/microprice)
//!
//! ## Cashflows (8.2)
//! - Buying option: cash outflow (premium paid)
//! - Selling option: cash inflow (premium received)
//! - Fees: placeholder model (interface exists)
//!
//! ## Constraints (8.3)
//! - qty must be multiple of lot size
//! - price must conform to tick size
//! - freeze qty warnings (not enforced yet)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::depth5::Depth5Book;
use crate::nse_specs::NseIndex;

/// Fee model interface per Z-Phase spec.
#[derive(Debug, Clone, Default)]
pub struct FeeModel {
    /// Per-order fixed fee (rupees).
    pub order_fee: f64,
    /// Per-lot fee (rupees).
    pub per_lot_fee: f64,
    /// Brokerage as percentage of premium (e.g., 0.0003 = 0.03%).
    pub brokerage_pct: f64,
    /// STT/CTT as percentage (e.g., 0.0005 = 0.05%).
    pub stt_pct: f64,
    /// Exchange transaction charges (e.g., 0.00053%).
    pub exchange_txn_pct: f64,
    /// SEBI charges (e.g., 0.0000005).
    pub sebi_pct: f64,
    /// Stamp duty (e.g., 0.00003 for buy).
    pub stamp_duty_pct: f64,
    /// GST on brokerage (18%).
    pub gst_pct: f64,
}

impl FeeModel {
    /// Standard NSE F&O fee model (approximate).
    pub fn nse_fno_standard() -> Self {
        Self {
            order_fee: 0.0,
            per_lot_fee: 0.0,
            brokerage_pct: 0.0003,    // 0.03% or Rs 20 max
            stt_pct: 0.000625,        // 0.0625% on sell (intrinsic value)
            exchange_txn_pct: 0.0000495, // 0.00495%
            sebi_pct: 0.000001,       // 0.0001%
            stamp_duty_pct: 0.00003,  // 0.003% (buy side)
            gst_pct: 0.18,            // 18% on brokerage
        }
    }

    /// Calculate total fees for a trade.
    pub fn calculate(&self, premium: f64, qty: u32, is_buy: bool) -> f64 {
        let notional = premium * qty as f64;
        let brokerage = notional * self.brokerage_pct;
        let stt = if is_buy { 0.0 } else { notional * self.stt_pct };
        let exchange = notional * self.exchange_txn_pct;
        let sebi = notional * self.sebi_pct;
        let stamp = if is_buy { notional * self.stamp_duty_pct } else { 0.0 };
        let gst = brokerage * self.gst_pct;

        self.order_fee + (self.per_lot_fee * (qty as f64)) + brokerage + stt + exchange + sebi + stamp + gst
    }
}

/// Single position in the ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Trading symbol.
    pub symbol: String,
    /// Net quantity (positive = long, negative = short). In lots.
    pub net_qty: i32,
    /// Average entry price.
    pub avg_price: f64,
    /// Last mark-to-market price.
    pub mtm_price: f64,
    /// Total premium paid (negative) or received (positive).
    pub premium_cashflow: f64,
    /// Realized PnL from closed positions.
    pub realized_pnl: f64,
    /// Total fees paid.
    pub total_fees: f64,
    /// Last update timestamp.
    pub last_update: DateTime<Utc>,
    /// Lot size for this instrument.
    pub lot_size: u32,
    /// Tick size for this instrument.
    pub tick_size: f64,
}

impl Position {
    /// Create new empty position.
    pub fn new(symbol: &str, lot_size: u32, tick_size: f64) -> Self {
        Self {
            symbol: symbol.to_string(),
            net_qty: 0,
            avg_price: 0.0,
            mtm_price: 0.0,
            premium_cashflow: 0.0,
            realized_pnl: 0.0,
            total_fees: 0.0,
            last_update: Utc::now(),
            lot_size,
            tick_size,
        }
    }

    /// Unrealized PnL based on current MTM price.
    /// For long: (mtm - avg) * qty * lot_size
    /// For short: (avg - mtm) * |qty| * lot_size
    pub fn unrealized_pnl(&self) -> f64 {
        if self.net_qty == 0 || self.avg_price == 0.0 {
            return 0.0;
        }
        let qty_units = self.net_qty.abs() as f64 * self.lot_size as f64;
        if self.net_qty > 0 {
            (self.mtm_price - self.avg_price) * qty_units
        } else {
            (self.avg_price - self.mtm_price) * qty_units
        }
    }

    /// Total PnL = realized + unrealized.
    pub fn total_pnl(&self) -> f64 {
        self.realized_pnl + self.unrealized_pnl()
    }

    /// Net PnL after fees.
    pub fn net_pnl(&self) -> f64 {
        self.total_pnl() - self.total_fees
    }

    /// Check if flat (no position).
    pub fn is_flat(&self) -> bool {
        self.net_qty == 0
    }

    /// Check if long.
    pub fn is_long(&self) -> bool {
        self.net_qty > 0
    }

    /// Check if short.
    pub fn is_short(&self) -> bool {
        self.net_qty < 0
    }

    /// Notional value = |qty| * lot_size * mtm_price.
    pub fn notional(&self) -> f64 {
        self.net_qty.abs() as f64 * self.lot_size as f64 * self.mtm_price
    }
}

/// Fill record for ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerFill {
    /// Timestamp of fill.
    pub ts: DateTime<Utc>,
    /// Trading symbol.
    pub symbol: String,
    /// Side: "BUY" or "SELL".
    pub side: String,
    /// Quantity in lots.
    pub qty: u32,
    /// Fill price.
    pub price: f64,
    /// Fees paid.
    pub fees: f64,
    /// Strategy ID (for attribution).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_id: Option<String>,
    /// Order ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
}

/// Options Ledger per Z-Phase spec section 8.
#[derive(Debug, Default)]
pub struct OptionsLedger {
    /// Per-instrument positions.
    positions: HashMap<String, Position>,
    /// Fee model.
    pub fee_model: FeeModel,
    /// Fill history.
    fills: Vec<LedgerFill>,
    /// Total realized PnL across all positions.
    total_realized: f64,
    /// Total fees paid.
    total_fees: f64,
}

impl OptionsLedger {
    /// Create new ledger with default fee model.
    pub fn new() -> Self {
        Self {
            fee_model: FeeModel::nse_fno_standard(),
            ..Default::default()
        }
    }

    /// Create with custom fee model.
    pub fn with_fee_model(fee_model: FeeModel) -> Self {
        Self {
            fee_model,
            ..Default::default()
        }
    }

    /// Get lot size for symbol (default to BANKNIFTY if unknown).
    fn lot_size_for(&self, symbol: &str) -> u32 {
        // Extract underlying from symbol (simple heuristic)
        if symbol.contains("BANKNIFTY") {
            NseIndex::BankNifty.lot_size()
        } else if symbol.contains("NIFTY") {
            NseIndex::Nifty.lot_size()
        } else if symbol.contains("FINNIFTY") {
            NseIndex::FinNifty.lot_size()
        } else {
            15 // Default to BANKNIFTY lot size
        }
    }

    /// Process a fill (buy or sell).
    ///
    /// Validates:
    /// - qty must be positive
    /// - Applies fees
    /// - Updates position with FIFO-style averaging
    ///
    /// Returns realized PnL from this fill (if closing).
    pub fn on_fill(
        &mut self,
        symbol: &str,
        is_buy: bool,
        qty: u32,
        price: f64,
        strategy_id: Option<&str>,
        order_id: Option<&str>,
    ) -> f64 {
        if qty == 0 {
            return 0.0;
        }

        let lot_size = self.lot_size_for(symbol);
        let tick_size = 0.05; // NSE options tick size

        // Calculate fees
        let fees = self.fee_model.calculate(price, qty * lot_size, is_buy);
        self.total_fees += fees;

        // Get or create position
        let pos = self.positions.entry(symbol.to_string()).or_insert_with(|| {
            Position::new(symbol, lot_size, tick_size)
        });

        let signed_qty = if is_buy { qty as i32 } else { -(qty as i32) };
        let premium_flow = if is_buy {
            -(price * qty as f64 * lot_size as f64) // Pay premium
        } else {
            price * qty as f64 * lot_size as f64   // Receive premium
        };

        let mut realized = 0.0;

        // Position update logic
        if pos.net_qty == 0 {
            // Opening new position
            pos.net_qty = signed_qty;
            pos.avg_price = price;
        } else if (pos.net_qty > 0 && is_buy) || (pos.net_qty < 0 && !is_buy) {
            // Adding to existing position - weighted average
            let old_qty = pos.net_qty.abs() as f64;
            let new_qty = qty as f64;
            pos.avg_price = (pos.avg_price * old_qty + price * new_qty) / (old_qty + new_qty);
            pos.net_qty += signed_qty;
        } else {
            // Closing or reducing position
            let close_qty = qty.min(pos.net_qty.unsigned_abs());
            let remaining = qty - close_qty;

            // Realize PnL on closed portion
            let close_units = close_qty as f64 * lot_size as f64;
            if pos.net_qty > 0 {
                // Was long, now selling
                realized = (price - pos.avg_price) * close_units;
            } else {
                // Was short, now buying
                realized = (pos.avg_price - price) * close_units;
            }

            pos.realized_pnl += realized;
            self.total_realized += realized;

            // Update position
            if is_buy {
                pos.net_qty += close_qty as i32;
            } else {
                pos.net_qty -= close_qty as i32;
            }

            // If flipped to other side
            if remaining > 0 {
                pos.avg_price = price;
                pos.net_qty = if is_buy { remaining as i32 } else { -(remaining as i32) };
            } else if pos.net_qty == 0 {
                pos.avg_price = 0.0;
            }
        }

        pos.premium_cashflow += premium_flow;
        pos.total_fees += fees;
        pos.mtm_price = price;
        pos.last_update = Utc::now();

        // Record fill
        self.fills.push(LedgerFill {
            ts: Utc::now(),
            symbol: symbol.to_string(),
            side: if is_buy { "BUY".to_string() } else { "SELL".to_string() },
            qty,
            price,
            fees,
            strategy_id: strategy_id.map(|s| s.to_string()),
            order_id: order_id.map(|s| s.to_string()),
        });

        realized
    }

    /// Mark-to-market all positions using book mid prices.
    pub fn mark_to_market(&mut self, books: &HashMap<String, Depth5Book>) {
        for (symbol, pos) in self.positions.iter_mut() {
            if let Some(book) = books.get(symbol) {
                if let Some(mid) = book.mid() {
                    pos.mtm_price = mid;
                    pos.last_update = Utc::now();
                }
            }
        }
    }

    /// Mark-to-market using microprice (more accurate).
    pub fn mark_to_market_microprice(&mut self, books: &HashMap<String, Depth5Book>) {
        for (symbol, pos) in self.positions.iter_mut() {
            if let Some(book) = books.get(symbol) {
                if let Some(mp) = book.microprice() {
                    pos.mtm_price = mp;
                    pos.last_update = Utc::now();
                }
            }
        }
    }

    /// Get position for symbol.
    pub fn get_position(&self, symbol: &str) -> Option<&Position> {
        self.positions.get(symbol)
    }

    /// Get all positions.
    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    /// Get non-flat positions.
    pub fn open_positions(&self) -> Vec<&Position> {
        self.positions.values().filter(|p| !p.is_flat()).collect()
    }

    /// Total unrealized PnL across all positions.
    pub fn total_unrealized_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.unrealized_pnl()).sum()
    }

    /// Total realized PnL.
    pub fn total_realized_pnl(&self) -> f64 {
        self.total_realized
    }

    /// Total PnL (realized + unrealized).
    pub fn total_pnl(&self) -> f64 {
        self.total_realized + self.total_unrealized_pnl()
    }

    /// Net PnL after fees.
    pub fn net_pnl(&self) -> f64 {
        self.total_pnl() - self.total_fees
    }

    /// Total fees paid.
    pub fn total_fees(&self) -> f64 {
        self.total_fees
    }

    /// Get fill history.
    pub fn fills(&self) -> &[LedgerFill] {
        &self.fills
    }

    /// Total notional exposure.
    pub fn total_notional(&self) -> f64 {
        self.positions.values().map(|p| p.notional()).sum()
    }

    /// Number of positions.
    pub fn position_count(&self) -> usize {
        self.positions.len()
    }

    /// Number of open (non-flat) positions.
    pub fn open_position_count(&self) -> usize {
        self.positions.values().filter(|p| !p.is_flat()).count()
    }

    /// Clear all positions and fills (for new session).
    pub fn clear(&mut self) {
        self.positions.clear();
        self.fills.clear();
        self.total_realized = 0.0;
        self.total_fees = 0.0;
    }

    /// Export fills as JSON lines (for vectorbtpro).
    pub fn export_fills_jsonl(&self) -> Vec<String> {
        self.fills
            .iter()
            .filter_map(|f| serde_json::to_string(f).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_pnl() {
        let mut pos = Position::new("TEST", 15, 0.05);
        pos.net_qty = 2; // Long 2 lots
        pos.avg_price = 100.0;
        pos.mtm_price = 110.0;
        pos.lot_size = 15;

        // Unrealized: (110 - 100) * 2 * 15 = 300
        assert_eq!(pos.unrealized_pnl(), 300.0);
    }

    #[test]
    fn test_ledger_round_trip() {
        let mut ledger = OptionsLedger::new();
        ledger.fee_model = FeeModel::default(); // Zero fees for test

        // Buy 2 lots @ 100
        ledger.on_fill("BANKNIFTY26JAN59000CE", true, 2, 100.0, None, None);

        let pos = ledger.get_position("BANKNIFTY26JAN59000CE").unwrap();
        assert_eq!(pos.net_qty, 2);
        assert_eq!(pos.avg_price, 100.0);

        // Sell 2 lots @ 110 (close)
        let realized = ledger.on_fill("BANKNIFTY26JAN59000CE", false, 2, 110.0, None, None);

        // Realized: (110 - 100) * 2 * 15 = 300
        assert_eq!(realized, 300.0);

        let pos = ledger.get_position("BANKNIFTY26JAN59000CE").unwrap();
        assert!(pos.is_flat());
        assert_eq!(pos.realized_pnl, 300.0);
    }

    #[test]
    fn test_short_position() {
        let mut ledger = OptionsLedger::new();
        ledger.fee_model = FeeModel::default();

        // Sell 1 lot @ 100 (open short)
        ledger.on_fill("BANKNIFTY26JAN59000PE", false, 1, 100.0, None, None);

        let pos = ledger.get_position("BANKNIFTY26JAN59000PE").unwrap();
        assert_eq!(pos.net_qty, -1);
        assert!(pos.is_short());

        // Buy 1 lot @ 90 (close short for profit)
        let realized = ledger.on_fill("BANKNIFTY26JAN59000PE", true, 1, 90.0, None, None);

        // Realized: (100 - 90) * 1 * 15 = 150
        assert_eq!(realized, 150.0);
    }
}
