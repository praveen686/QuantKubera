//! # Option Greeks Module
//!
//! Re-exports OptionGreeks from kubera_models (the canonical location).
//!
//! This module previously defined OptionGreeks directly, but it has been
//! moved to kubera_models to break dependency cycles and establish proper
//! layering (models → core → options).

// Re-export from the canonical location
pub use kubera_models::greeks::{OptionGreeks, OptionType};

// Also re-export the helper traits/functions if needed for tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atm_straddle_delta() {
        // ATM straddle should have near-zero delta
        let spot = 25800.0;
        let strike = 25800.0;
        let time = 7.0 / 365.0;
        let rate = 0.05;
        let vol = 0.15;

        let call_greeks = OptionGreeks::for_call(spot, strike, time, rate, vol);
        let put_greeks = OptionGreeks::for_put(spot, strike, time, rate, vol);
        let straddle = call_greeks.add(&put_greeks);

        assert!(straddle.delta.abs() < 0.1, "ATM straddle delta should be near zero: {}", straddle.delta);
        assert!(straddle.gamma > 0.0, "Straddle should have positive gamma");
        assert!(straddle.vega > 0.0, "Straddle should have positive vega");
    }

    #[test]
    fn test_call_delta_range() {
        let greeks = OptionGreeks::for_call(25800.0, 25800.0, 30.0/365.0, 0.05, 0.18);
        assert!(greeks.delta > 0.0 && greeks.delta < 1.0, "Call delta should be between 0 and 1");
    }
}
