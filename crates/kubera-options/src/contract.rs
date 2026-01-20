//! # Options Contract Module
//!
//! Formal definition of option instruments and expiry calculation logic.
//!
//! ## Description
//! This module defines the core data structures for European-style options
//! as traded on the National Stock Exchange of India (NSE). It includes
//! utilities for building ticker symbols and calculating standard weekly/monthly
//! expiration dates.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use chrono::{NaiveDate, Datelike, Weekday};
use serde::{Deserialize, Serialize};

/// Classification of the option right: Call (CE) or Put (PE).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptionType {
    /// Right to buy.
    Call,
    /// Right to sell.
    Put,
}

impl OptionType {
    /// Static string representation used in NSE ticker symbols.
    pub fn symbol_suffix(&self) -> &'static str {
        match self {
            OptionType::Call => "CE",
            OptionType::Put => "PE",
        }
    }
}

/// Logical model of a single derivative contract.
///
/// # Fields
/// * `underlying` - Target asset (e.g., "NIFTY").
/// * `expiry` - Contract expiration date.
/// * `strike` - Exercise price.
/// * `option_type` - Call or Put.
/// * `instrument_token` - Exchange-specific numeric ID.
/// * `tradingsymbol` - Human-readable ticker (e.g., "NIFTY24O3124500CE").
/// * `lot_size` - Minimum trading unit quantity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionContract {
    pub underlying: String,      
    pub expiry: NaiveDate,
    pub strike: f64,
    pub option_type: OptionType,
    pub instrument_token: u32,
    pub tradingsymbol: String,   
    pub lot_size: u32,
}

impl OptionContract {
    /// Constructs a standardized NSE-style trading symbol from components.
    ///
    /// # Parameters
    /// * `underlying` - Asset identifier.
    /// * `expiry` - Target date.
    /// * `strike` - Numeric strike level.
    /// * `opt_type` - Call/Put variant.
    pub fn build_symbol(underlying: &str, expiry: NaiveDate, strike: f64, opt_type: OptionType) -> String {
        let year = expiry.year() % 100;
        let month_char = match expiry.month() {
            1 => '1', 2 => '2', 3 => '3', 4 => '4', 5 => '5', 6 => '6',
            7 => '7', 8 => '8', 9 => '9', 10 => 'O', 11 => 'N', 12 => 'D',
            _ => 'X',
        };
        let day = expiry.day();
        format!("{}{}{}{}{}{}", underlying, year, month_char, day, strike as u32, opt_type.symbol_suffix())
    }
}

/// Collection of contracts for a specific underlying and expiration.
#[derive(Debug, Clone)]
pub struct OptionChain {
    pub underlying: String,
    pub expiry: NaiveDate,
    /// Reference price of the underlying asset.
    pub spot_price: f64,
    pub calls: Vec<OptionContract>,
    pub puts: Vec<OptionContract>,
}

impl OptionChain {
    /// Instantiates a new, empty option chain.
    pub fn new(underlying: String, expiry: NaiveDate, spot_price: f64) -> Self {
        Self {
            underlying,
            expiry,
            spot_price,
            calls: Vec::new(),
            puts: Vec::new(),
        }
    }

    /// Identifies the "At-The-Money" (ATM) strike price based on current spot.
    ///
    /// # Parameters
    /// * `tick_size` - Interval between strikes (e.g., 50.0 for NIFTY).
    pub fn atm_strike(&self, tick_size: f64) -> f64 {
        (self.spot_price / tick_size).round() * tick_size
    }

    /// Retrieves the ATM call and put pair from the chain.
    ///
    /// # Returns
    /// `Some((Call, Put))` if both variants exist at the ATM strike, else `None`.
    pub fn atm_straddle(&self) -> Option<(OptionContract, OptionContract)> {
        let atm = self.atm_strike(50.0); 
        let call = self.calls.iter().find(|c| (c.strike - atm).abs() < 0.01)?;
        let put = self.puts.iter().find(|p| (p.strike - atm).abs() < 0.01)?;
        Some((call.clone(), put.clone()))
    }
}

/// Calculates the nearest Thursday for weekly contracts.
///
/// # Parameters
/// * `from` - Reference date to begin search.
pub fn next_weekly_expiry(from: NaiveDate) -> NaiveDate {
    let days_until_thursday = (Weekday::Thu.num_days_from_monday() as i64 
        - from.weekday().num_days_from_monday() as i64 + 7) % 7;
    if days_until_thursday == 0 {
        from 
    } else {
        from + chrono::Duration::days(days_until_thursday)
    }
}

/// Identifies the final Thursday of a specific month.
///
/// # Parameters
/// * `year` - 4-digit calendar year.
/// * `month` - 1-12 month index.
pub fn monthly_expiry(year: i32, month: u32) -> NaiveDate {
    let first_of_next = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
    };
    let last_day = first_of_next.pred_opt().unwrap();
    
    let days_since_thursday = (last_day.weekday().num_days_from_monday() as i64 
        - Weekday::Thu.num_days_from_monday() as i64 + 7) % 7;
    last_day - chrono::Duration::days(days_since_thursday)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atm_strike() {
        let chain = OptionChain::new("NIFTY".to_string(), NaiveDate::from_ymd_opt(2024, 12, 26).unwrap(), 25815.55);
        assert_eq!(chain.atm_strike(50.0), 25800.0);
    }

    #[test]
    fn test_next_weekly_expiry() {
        let monday = NaiveDate::from_ymd_opt(2024, 12, 16).unwrap();
        let thursday = next_weekly_expiry(monday);
        assert_eq!(thursday.weekday(), Weekday::Thu);
        assert_eq!(thursday, NaiveDate::from_ymd_opt(2024, 12, 19).unwrap());
    }
}
