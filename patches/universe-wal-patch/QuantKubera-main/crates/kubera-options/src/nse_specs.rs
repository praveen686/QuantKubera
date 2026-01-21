//! # NSE F&O Market Specifications
//!
//! Comprehensive market rules for Indian derivatives trading.
//!
//! ## Description
//! This module defines all the exchange-mandated specifications for trading
//! options on NSE (National Stock Exchange of India), including:
//! - Lot sizes for NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY
//! - Strike price intervals (tick sizes)
//! - Trading hours and market phases
//! - Price tick rules
//!
//! ## References
//! - NSE Circulars on F&O contract specifications
//! - IEEE Std 1016-2009: Software Design Descriptions

use chrono::{NaiveTime, Weekday, NaiveDate, Datelike};
use serde::{Deserialize, Serialize};

/// NSE Index identifiers with their specifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NseIndex {
    /// NIFTY 50 - flagship index
    Nifty,
    /// BANKNIFTY - banking sector index
    BankNifty,
    /// FINNIFTY - financial services index
    FinNifty,
    /// MIDCPNIFTY - midcap index
    MidcpNifty,
    /// SENSEX (BSE) traded on NSE platform
    Sensex,
}

impl NseIndex {
    /// Returns the lot size for this index.
    ///
    /// # Current Lot Sizes (as of 2024)
    /// - NIFTY: 50
    /// - BANKNIFTY: 15
    /// - FINNIFTY: 40
    /// - MIDCPNIFTY: 50
    /// - SENSEX: 10
    pub fn lot_size(&self) -> u32 {
        match self {
            NseIndex::Nifty => 50,
            NseIndex::BankNifty => 15,
            NseIndex::FinNifty => 40,
            NseIndex::MidcpNifty => 50,
            NseIndex::Sensex => 10,
        }
    }

    /// Returns the strike price interval for this index.
    ///
    /// # Strike Intervals
    /// - NIFTY: ₹50 intervals
    /// - BANKNIFTY: ₹100 intervals
    /// - FINNIFTY: ₹50 intervals
    /// - MIDCPNIFTY: ₹25 intervals
    /// - SENSEX: ₹100 intervals
    pub fn strike_interval(&self) -> f64 {
        match self {
            NseIndex::Nifty => 50.0,
            NseIndex::BankNifty => 100.0,
            NseIndex::FinNifty => 50.0,
            NseIndex::MidcpNifty => 25.0,
            NseIndex::Sensex => 100.0,
        }
    }

    /// Returns the minimum tick size for option premiums.
    ///
    /// All NSE index options have a tick size of ₹0.05.
    pub fn tick_size(&self) -> f64 {
        0.05
    }

    /// Returns the weekly expiry day for this index.
    ///
    /// # Expiry Days
    /// - NIFTY: Thursday
    /// - BANKNIFTY: Wednesday
    /// - FINNIFTY: Tuesday
    /// - MIDCPNIFTY: Monday
    /// - SENSEX: Friday
    pub fn expiry_day(&self) -> Weekday {
        match self {
            NseIndex::Nifty => Weekday::Thu,
            NseIndex::BankNifty => Weekday::Wed,
            NseIndex::FinNifty => Weekday::Tue,
            NseIndex::MidcpNifty => Weekday::Mon,
            NseIndex::Sensex => Weekday::Fri,
        }
    }

    /// Detects the index from a trading symbol.
    ///
    /// # Examples
    /// ```
    /// use kubera_options::nse_specs::NseIndex;
    /// assert_eq!(NseIndex::from_symbol("NIFTY2612025400CE"), Some(NseIndex::Nifty));
    /// assert_eq!(NseIndex::from_symbol("BANKNIFTY2611552000CE"), Some(NseIndex::BankNifty));
    /// ```
    pub fn from_symbol(symbol: &str) -> Option<Self> {
        if symbol.starts_with("NIFTY") && !symbol.starts_with("NIFTYIT") {
            if symbol.contains("BANK") {
                Some(NseIndex::BankNifty)
            } else if symbol.contains("FIN") {
                Some(NseIndex::FinNifty)
            } else if symbol.contains("MIDCP") {
                Some(NseIndex::MidcpNifty)
            } else {
                Some(NseIndex::Nifty)
            }
        } else if symbol.starts_with("BANKNIFTY") {
            Some(NseIndex::BankNifty)
        } else if symbol.starts_with("FINNIFTY") {
            Some(NseIndex::FinNifty)
        } else if symbol.starts_with("MIDCPNIFTY") {
            Some(NseIndex::MidcpNifty)
        } else if symbol.starts_with("SENSEX") {
            Some(NseIndex::Sensex)
        } else {
            None
        }
    }

    /// Returns the symbol prefix used in trading symbols.
    pub fn symbol_prefix(&self) -> &'static str {
        match self {
            NseIndex::Nifty => "NIFTY",
            NseIndex::BankNifty => "BANKNIFTY",
            NseIndex::FinNifty => "FINNIFTY",
            NseIndex::MidcpNifty => "MIDCPNIFTY",
            NseIndex::Sensex => "SENSEX",
        }
    }
}

/// NSE trading session phases and times (IST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingPhase {
    /// Pre-market order collection (9:00 - 9:08)
    PreOpen,
    /// Pre-market order matching (9:08 - 9:15)
    PreOpenNoCancel,
    /// Normal trading session (9:15 - 15:30)
    NormalMarket,
    /// Post-close session (15:40 - 16:00)
    PostClose,
    /// Market closed
    Closed,
}

/// NSE trading hours configuration.
pub struct NseTradingHours;

impl NseTradingHours {
    /// Pre-market start time (09:00 IST)
    pub const PRE_OPEN_START: NaiveTime = unsafe { NaiveTime::from_hms_opt(9, 0, 0).unwrap_unchecked() };

    /// Pre-market end, normal trading start (09:15 IST)
    pub const MARKET_OPEN: NaiveTime = unsafe { NaiveTime::from_hms_opt(9, 15, 0).unwrap_unchecked() };

    /// Normal trading end (15:30 IST)
    pub const MARKET_CLOSE: NaiveTime = unsafe { NaiveTime::from_hms_opt(15, 30, 0).unwrap_unchecked() };

    /// Post-close session start (15:40 IST)
    pub const POST_CLOSE_START: NaiveTime = unsafe { NaiveTime::from_hms_opt(15, 40, 0).unwrap_unchecked() };

    /// Post-close session end (16:00 IST)
    pub const POST_CLOSE_END: NaiveTime = unsafe { NaiveTime::from_hms_opt(16, 0, 0).unwrap_unchecked() };

    /// Determines the current trading phase based on time.
    pub fn current_phase(time: NaiveTime) -> TradingPhase {
        if time < Self::PRE_OPEN_START {
            TradingPhase::Closed
        } else if time < Self::MARKET_OPEN {
            if time < unsafe { NaiveTime::from_hms_opt(9, 8, 0).unwrap_unchecked() } {
                TradingPhase::PreOpen
            } else {
                TradingPhase::PreOpenNoCancel
            }
        } else if time <= Self::MARKET_CLOSE {
            TradingPhase::NormalMarket
        } else if time >= Self::POST_CLOSE_START && time <= Self::POST_CLOSE_END {
            TradingPhase::PostClose
        } else {
            TradingPhase::Closed
        }
    }

    /// Checks if the given time is during normal trading hours.
    pub fn is_trading_time(time: NaiveTime) -> bool {
        matches!(Self::current_phase(time), TradingPhase::NormalMarket)
    }

    /// Checks if the given date is a trading day (weekday).
    /// Note: This doesn't account for exchange holidays.
    pub fn is_trading_day(date: NaiveDate) -> bool {
        !matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
    }
}

/// Validates and rounds quantities to lot size multiples.
#[derive(Debug, Clone)]
pub struct LotSizeValidator {
    lot_size: u32,
}

impl LotSizeValidator {
    /// Creates a validator for the given index.
    pub fn for_index(index: NseIndex) -> Self {
        Self {
            lot_size: index.lot_size(),
        }
    }

    /// Creates a validator with a specific lot size.
    pub fn new(lot_size: u32) -> Self {
        Self { lot_size }
    }

    /// Validates that the quantity is a multiple of the lot size.
    pub fn is_valid(&self, quantity: u32) -> bool {
        quantity > 0 && quantity % self.lot_size == 0
    }

    /// Rounds up to the nearest lot multiple.
    pub fn round_up(&self, quantity: f64) -> u32 {
        let lots = (quantity / self.lot_size as f64).ceil() as u32;
        lots * self.lot_size
    }

    /// Rounds down to the nearest lot multiple.
    pub fn round_down(&self, quantity: f64) -> u32 {
        let lots = (quantity / self.lot_size as f64).floor() as u32;
        lots.max(1) * self.lot_size
    }

    /// Returns the number of lots for a given quantity.
    pub fn to_lots(&self, quantity: u32) -> u32 {
        quantity / self.lot_size
    }

    /// Returns the quantity for a given number of lots.
    pub fn from_lots(&self, lots: u32) -> u32 {
        lots * self.lot_size
    }
}

/// Validates and rounds prices to tick size multiples.
#[derive(Debug, Clone)]
pub struct TickSizeValidator {
    tick_size: f64,
}

impl TickSizeValidator {
    /// Creates a validator for the standard NSE option tick size (₹0.05).
    pub fn nse_options() -> Self {
        Self { tick_size: 0.05 }
    }

    /// Creates a validator with a specific tick size.
    pub fn new(tick_size: f64) -> Self {
        Self { tick_size }
    }

    /// Validates that the price is a multiple of the tick size.
    pub fn is_valid(&self, price: f64) -> bool {
        let remainder = price % self.tick_size;
        remainder.abs() < 1e-9 || (self.tick_size - remainder.abs()).abs() < 1e-9
    }

    /// Rounds to the nearest tick.
    pub fn round(&self, price: f64) -> f64 {
        (price / self.tick_size).round() * self.tick_size
    }

    /// Rounds up to the next tick.
    pub fn round_up(&self, price: f64) -> f64 {
        (price / self.tick_size).ceil() * self.tick_size
    }

    /// Rounds down to the previous tick.
    pub fn round_down(&self, price: f64) -> f64 {
        (price / self.tick_size).floor() * self.tick_size
    }
}

/// Comprehensive order validator for NSE F&O.
#[derive(Debug, Clone)]
pub struct NseOrderValidator {
    lot_validator: LotSizeValidator,
    tick_validator: TickSizeValidator,
    index: NseIndex,
}

impl NseOrderValidator {
    /// Creates a validator for the specified index.
    pub fn new(index: NseIndex) -> Self {
        Self {
            lot_validator: LotSizeValidator::for_index(index),
            tick_validator: TickSizeValidator::nse_options(),
            index,
        }
    }

    /// Validates a proposed order and returns any issues.
    pub fn validate(&self, quantity: u32, price: f64) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if !self.lot_validator.is_valid(quantity) {
            errors.push(format!(
                "Quantity {} is not a multiple of lot size {} for {}",
                quantity,
                self.index.lot_size(),
                self.index.symbol_prefix()
            ));
        }

        if price > 0.0 && !self.tick_validator.is_valid(price) {
            errors.push(format!(
                "Price {:.2} is not a multiple of tick size {:.2}",
                price,
                self.tick_validator.tick_size
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Adjusts an order to comply with exchange rules.
    pub fn adjust(&self, quantity: f64, price: f64) -> (u32, f64) {
        let adjusted_qty = self.lot_validator.round_down(quantity);
        let adjusted_price = self.tick_validator.round(price);
        (adjusted_qty, adjusted_price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lot_sizes() {
        assert_eq!(NseIndex::Nifty.lot_size(), 50);
        assert_eq!(NseIndex::BankNifty.lot_size(), 15);
        assert_eq!(NseIndex::FinNifty.lot_size(), 40);
    }

    #[test]
    fn test_strike_intervals() {
        assert_eq!(NseIndex::Nifty.strike_interval(), 50.0);
        assert_eq!(NseIndex::BankNifty.strike_interval(), 100.0);
    }

    #[test]
    fn test_symbol_detection() {
        assert_eq!(NseIndex::from_symbol("NIFTY2612025400CE"), Some(NseIndex::Nifty));
        assert_eq!(NseIndex::from_symbol("BANKNIFTY2611552000CE"), Some(NseIndex::BankNifty));
        assert_eq!(NseIndex::from_symbol("FINNIFTY2612025000PE"), Some(NseIndex::FinNifty));
        assert_eq!(NseIndex::from_symbol("RELIANCE"), None);
    }

    #[test]
    fn test_lot_validator() {
        let validator = LotSizeValidator::for_index(NseIndex::Nifty);

        assert!(validator.is_valid(50));
        assert!(validator.is_valid(100));
        assert!(!validator.is_valid(75));
        assert!(!validator.is_valid(0));

        assert_eq!(validator.round_up(75.0), 100);
        assert_eq!(validator.round_down(75.0), 50);
        assert_eq!(validator.to_lots(100), 2);
        assert_eq!(validator.from_lots(3), 150);
    }

    /// Helper for approximate float comparison
    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn test_tick_validator() {
        let validator = TickSizeValidator::nse_options();

        assert!(validator.is_valid(100.00));
        assert!(validator.is_valid(100.05));
        assert!(validator.is_valid(100.10));
        assert!(!validator.is_valid(100.03));

        assert!(approx_eq(validator.round(100.03), 100.05));
        assert!(approx_eq(validator.round(100.07), 100.05));
        assert!(approx_eq(validator.round_up(100.01), 100.05));
        assert!(approx_eq(validator.round_down(100.09), 100.05));
    }

    #[test]
    fn test_order_validator() {
        let validator = NseOrderValidator::new(NseIndex::Nifty);

        // Valid order
        assert!(validator.validate(50, 100.05).is_ok());

        // Invalid lot size
        let result = validator.validate(75, 100.05);
        assert!(result.is_err());

        // Invalid tick
        let result = validator.validate(50, 100.03);
        assert!(result.is_err());

        // Adjust
        let (qty, price) = validator.adjust(73.0, 100.03);
        assert_eq!(qty, 50);
        assert!(approx_eq(price, 100.05));
    }

    #[test]
    fn test_trading_hours() {
        use chrono::NaiveTime;

        let pre_open = NaiveTime::from_hms_opt(9, 5, 0).unwrap();
        let normal = NaiveTime::from_hms_opt(10, 30, 0).unwrap();
        let closed = NaiveTime::from_hms_opt(16, 30, 0).unwrap();

        assert_eq!(NseTradingHours::current_phase(pre_open), TradingPhase::PreOpen);
        assert_eq!(NseTradingHours::current_phase(normal), TradingPhase::NormalMarket);
        assert_eq!(NseTradingHours::current_phase(closed), TradingPhase::Closed);

        assert!(NseTradingHours::is_trading_time(normal));
        assert!(!NseTradingHours::is_trading_time(closed));
    }
}
