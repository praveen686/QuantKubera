//! # Option Greeks Module
//!
//! Computes first and second-order sensitivities for European options.
//!
//! ## Description
//! This module calculates the "Greeks" - partial derivatives of option price with
//! respect to underlying parameters. Essential for delta hedging, risk management,
//! and portfolio optimization.
//!
//! ## Greeks Computed
//! | Greek | Symbol | Measures sensitivity to |
//! |-------|--------|------------------------|
//! | Delta | Δ | Underlying price |
//! | Gamma | Γ | Delta (second-order) |
//! | Theta | Θ | Time decay |
//! | Vega  | ν | Volatility |
//! | Rho   | ρ | Interest rate |
//!
//! ## References
//! - Hull, J. C. (2018). Options, Futures, and Other Derivatives, 10th ed.
//! - IEEE Std 1016-2009: Software Design Descriptions

use serde::{Deserialize, Serialize};
use std::f64::consts::PI;

/// Container for option Greeks (first and second-order sensitivities).
///
/// # Description
/// Stores the five primary Greeks computed from the Black-Scholes-Merton model.
/// All values are computed analytically for European options.
///
/// # Fields
/// * `delta` - Price sensitivity to underlying, range: [-1, 1] for single option
/// * `gamma` - Delta sensitivity to underlying, always non-negative
/// * `theta` - Daily time decay in currency units (typically negative for long options)
/// * `vega` - Sensitivity per 1% volatility change in currency units
/// * `rho` - Sensitivity per 1% interest rate change in currency units
///
/// # Examples
/// ```
/// use kubera_options::greeks::OptionGreeks;
///
/// let call_greeks = OptionGreeks::for_call(25800.0, 25800.0, 7.0/365.0, 0.065, 0.15);
/// assert!(call_greeks.delta > 0.0 && call_greeks.delta < 1.0);
/// assert!(call_greeks.gamma > 0.0);
/// ```
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct OptionGreeks {
    /// Delta (Δ): Rate of change of option price with respect to underlying price.
    /// Call delta ∈ [0, 1], Put delta ∈ [-1, 0].
    pub delta: f64,
    
    /// Gamma (Γ): Rate of change of delta with respect to underlying price.
    /// Always positive for long options. Units: 1/currency.
    pub gamma: f64,
    
    /// Theta (Θ): Rate of time decay per calendar day.
    /// Typically negative for long options (time decay). Units: currency/day.
    pub theta: f64,
    
    /// Vega (ν): Sensitivity to 1% change in implied volatility.
    /// Always positive for long options. Units: currency/1% vol.
    pub vega: f64,
    
    /// Rho (ρ): Sensitivity to 1% change in risk-free interest rate.
    /// Positive for calls, negative for puts. Units: currency/1% rate.
    pub rho: f64,
}

impl OptionGreeks {
    /// Calculates all Greeks for a European call option.
    ///
    /// # Description
    /// Computes analytical Greeks using Black-Scholes closed-form solutions.
    ///
    /// # Parameters
    /// * `spot` - Current underlying price S, must be positive
    /// * `strike` - Option strike price K, must be positive
    /// * `time` - Time to expiration in years (e.g., 7.0/365.0 for 7 days)
    /// * `rate` - Risk-free interest rate (e.g., 0.065 = 6.5%)
    /// * `volatility` - Annualized implied volatility (e.g., 0.15 = 15%)
    ///
    /// # Returns
    /// [`OptionGreeks`] struct with all computed sensitivities.
    ///
    /// # Examples
    /// ```
    /// use kubera_options::greeks::OptionGreeks;
    ///
    /// // NIFTY 25800 ATM call, 7 DTE
    /// let greeks = OptionGreeks::for_call(25800.0, 25800.0, 7.0/365.0, 0.065, 0.15);
    /// println!("Delta: {:.4}, Gamma: {:.6}", greeks.delta, greeks.gamma);
    /// ```
    pub fn for_call(spot: f64, strike: f64, time: f64, rate: f64, volatility: f64) -> Self {
        if time <= 0.0 {
            return Self::at_expiry_call(spot, strike);
        }
        
        let (d1, d2) = d1_d2(spot, strike, time, rate, volatility);
        let sqrt_t = time.sqrt();
        let nd1 = norm_cdf(d1);
        let nd2 = norm_cdf(d2);
        let npd1 = norm_pdf(d1);
        let discount = (-rate * time).exp();
        
        Self {
            delta: nd1,
            gamma: npd1 / (spot * volatility * sqrt_t),
            theta: -(spot * npd1 * volatility) / (2.0 * sqrt_t) 
                   - rate * strike * discount * nd2,
            vega: spot * sqrt_t * npd1 / 100.0, // Per 1% vol
            rho: strike * time * discount * nd2 / 100.0, // Per 1% rate
        }
    }

    /// Calculates all Greeks for a European put option.
    ///
    /// # Description
    /// Computes analytical Greeks using Black-Scholes closed-form solutions.
    /// Note: Put delta is negative, put rho is negative.
    ///
    /// # Parameters
    /// * `spot` - Current underlying price S
    /// * `strike` - Option strike price K
    /// * `time` - Time to expiration in years
    /// * `rate` - Risk-free interest rate
    /// * `volatility` - Annualized implied volatility
    ///
    /// # Returns
    /// [`OptionGreeks`] struct with all computed sensitivities.
    pub fn for_put(spot: f64, strike: f64, time: f64, rate: f64, volatility: f64) -> Self {
        if time <= 0.0 {
            return Self::at_expiry_put(spot, strike);
        }
        
        let (d1, d2) = d1_d2(spot, strike, time, rate, volatility);
        let sqrt_t = time.sqrt();
        let nd1_neg = norm_cdf(-d1);
        let nd2_neg = norm_cdf(-d2);
        let npd1 = norm_pdf(d1);
        let discount = (-rate * time).exp();
        
        Self {
            delta: nd1_neg - 1.0, // Put delta is negative
            gamma: npd1 / (spot * volatility * sqrt_t),
            theta: -(spot * npd1 * volatility) / (2.0 * sqrt_t) 
                   + rate * strike * discount * nd2_neg,
            vega: spot * sqrt_t * npd1 / 100.0, // Same as call
            rho: -strike * time * discount * nd2_neg / 100.0,
        }
    }

    /// Returns Greeks at expiration for a call option.
    fn at_expiry_call(spot: f64, strike: f64) -> Self {
        Self {
            delta: if spot > strike { 1.0 } else { 0.0 },
            gamma: 0.0,
            theta: 0.0,
            vega: 0.0,
            rho: 0.0,
        }
    }

    /// Returns Greeks at expiration for a put option.
    fn at_expiry_put(spot: f64, strike: f64) -> Self {
        Self {
            delta: if spot < strike { -1.0 } else { 0.0 },
            gamma: 0.0,
            theta: 0.0,
            vega: 0.0,
            rho: 0.0,
        }
    }

    /// Combines Greeks from two positions (addition).
    ///
    /// # Description
    /// Aggregates Greeks for portfolio-level risk management.
    /// Useful for multi-leg strategies like straddles and spreads.
    ///
    /// # Parameters
    /// * `other` - Another [`OptionGreeks`] to add
    ///
    /// # Returns
    /// New [`OptionGreeks`] with summed values.
    ///
    /// # Examples
    /// ```
    /// use kubera_options::greeks::OptionGreeks;
    ///
    /// let call = OptionGreeks::for_call(25800.0, 25800.0, 7.0/365.0, 0.065, 0.15);
    /// let put = OptionGreeks::for_put(25800.0, 25800.0, 7.0/365.0, 0.065, 0.15);
    /// let straddle = call.add(&put);
    /// assert!(straddle.delta.abs() < 0.1); // Near delta-neutral
    /// ```
    pub fn add(&self, other: &Self) -> Self {
        Self {
            delta: self.delta + other.delta,
            gamma: self.gamma + other.gamma,
            theta: self.theta + other.theta,
            vega: self.vega + other.vega,
            rho: self.rho + other.rho,
        }
    }

    /// Scales Greeks by a quantity multiplier.
    ///
    /// # Description
    /// Multiplies all Greeks by a scalar for position sizing.
    /// Negative quantities represent short positions.
    ///
    /// # Parameters
    /// * `quantity` - Multiplier (can be negative for shorts)
    ///
    /// # Returns
    /// New [`OptionGreeks`] with scaled values.
    pub fn scale(&self, quantity: f64) -> Self {
        Self {
            delta: self.delta * quantity,
            gamma: self.gamma * quantity,
            theta: self.theta * quantity,
            vega: self.vega * quantity,
            rho: self.rho * quantity,
        }
    }
}

// Helper functions (same as pricing.rs)
fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / (2.0_f64).sqrt()))
}

fn norm_pdf(x: f64) -> f64 {
    (-(x * x) / 2.0).exp() / (2.0 * PI).sqrt()
}

fn erf(x: f64) -> f64 {
    let a1 =  0.254829592;
    let a2 = -0.284496736;
    let a3 =  1.421413741;
    let a4 = -1.453152027;
    let a5 =  1.061405429;
    let p  =  0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}

fn d1_d2(spot: f64, strike: f64, time: f64, rate: f64, volatility: f64) -> (f64, f64) {
    let d1 = ((spot / strike).ln() + (rate + volatility * volatility / 2.0) * time) 
             / (volatility * time.sqrt());
    let d2 = d1 - volatility * time.sqrt();
    (d1, d2)
}

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
