//! # Options Pricing Module
//!
//! Implements Black-Scholes option pricing model and implied volatility calculation.
//!
//! ## Description
//! This module provides European option pricing functions based on the Black-Scholes-Merton
//! model (1973). It includes analytical solutions for call and put options, as well as
//! Newton-Raphson based implied volatility solver.
//!
//! ## References
//! - Black, F., & Scholes, M. (1973). The Pricing of Options and Corporate Liabilities.
//!   Journal of Political Economy, 81(3), 637-654.
//! - IEEE Std 1016-2009: Software Design Descriptions
//!
//! ## Module Structure
//! - [`black_scholes_call`] - European call option price
//! - [`black_scholes_put`] - European put option price  
//! - [`implied_volatility`] - Newton-Raphson IV solver

use std::f64::consts::PI;

/// Computes the standard normal cumulative distribution function.
///
/// # Description
/// Calculates Φ(x) = P(Z ≤ x) where Z ~ N(0,1) using the error function.
///
/// # Parameters
/// * `x` - The upper bound of integration, range: (-∞, +∞)
///
/// # Returns
/// Probability value in range [0.0, 1.0]
///
/// # References
/// - Abramowitz, M., & Stegun, I. A. (1964). Handbook of Mathematical Functions.
fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / (2.0_f64).sqrt()))
}

/// Computes the standard normal probability density function.
///
/// # Description
/// Calculates φ(x) = (1/√2π) * e^(-x²/2) for the standard normal distribution.
///
/// # Parameters
/// * `x` - The point at which to evaluate the PDF, range: (-∞, +∞)
///
/// # Returns
/// Density value, always positive, maximum at x=0
fn norm_pdf(x: f64) -> f64 {
    (-(x * x) / 2.0).exp() / (2.0 * PI).sqrt()
}

/// Computes the error function using Abramowitz & Stegun approximation.
///
/// # Description
/// Approximates erf(x) = (2/√π) ∫₀ˣ e^(-t²) dt with maximum error < 1.5×10⁻⁷.
///
/// # Parameters
/// * `x` - Input value, range: (-∞, +∞)
///
/// # Returns
/// Error function value in range [-1.0, 1.0]
///
/// # References
/// - Abramowitz & Stegun, Formula 7.1.26
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

/// Calculates d₁ and d₂ parameters for Black-Scholes formula.
///
/// # Description
/// Computes the standardized log-moneyness adjusted for drift and volatility:
/// - d₁ = [ln(S/K) + (r + σ²/2)T] / (σ√T)
/// - d₂ = d₁ - σ√T
///
/// # Parameters
/// * `spot` - Current underlying price S, must be positive
/// * `strike` - Option strike price K, must be positive
/// * `time` - Time to expiration T in years, must be positive
/// * `rate` - Risk-free interest rate r (e.g., 0.05 = 5%)
/// * `volatility` - Annualized volatility σ (e.g., 0.20 = 20%)
///
/// # Returns
/// Tuple (d₁, d₂) for use in Black-Scholes pricing
fn d1_d2(spot: f64, strike: f64, time: f64, rate: f64, volatility: f64) -> (f64, f64) {
    let d1 = ((spot / strike).ln() + (rate + volatility * volatility / 2.0) * time) 
             / (volatility * time.sqrt());
    let d2 = d1 - volatility * time.sqrt();
    (d1, d2)
}

/// Calculates European call option price using Black-Scholes model.
///
/// # Description
/// Computes the theoretical fair value of a European call option:
/// C = S·N(d₁) - K·e^(-rT)·N(d₂)
///
/// # Parameters
/// * `spot` - Current underlying price S in currency units (e.g., 25800.0 for NIFTY)
/// * `strike` - Strike price K in same currency units
/// * `time` - Time to expiration T in years (e.g., 7.0/365.0 for 7 days)
/// * `rate` - Continuously compounded risk-free rate (e.g., 0.065 = 6.5%)
/// * `volatility` - Annualized implied volatility (e.g., 0.15 = 15%)
///
/// # Returns
/// Call option premium in currency units. Returns intrinsic value max(S-K, 0) if time ≤ 0.
///
/// # Examples
/// ```
/// use kubera_options::pricing::black_scholes_call;
/// 
/// // NIFTY ATM call, 7 days to expiry, 15% IV
/// let premium = black_scholes_call(25800.0, 25800.0, 7.0/365.0, 0.065, 0.15);
/// assert!(premium > 100.0 && premium < 300.0);
/// ```
///
/// # References
/// - Black-Scholes (1973), Equation 13
pub fn black_scholes_call(spot: f64, strike: f64, time: f64, rate: f64, volatility: f64) -> f64 {
    if time <= 0.0 {
        return (spot - strike).max(0.0); // Intrinsic value at expiry
    }
    
    let (d1, d2) = d1_d2(spot, strike, time, rate, volatility);
    spot * norm_cdf(d1) - strike * (-rate * time).exp() * norm_cdf(d2)
}

/// Calculates European put option price using Black-Scholes model.
///
/// # Description
/// Computes the theoretical fair value of a European put option:
/// P = K·e^(-rT)·N(-d₂) - S·N(-d₁)
///
/// # Parameters
/// * `spot` - Current underlying price S in currency units
/// * `strike` - Strike price K in same currency units
/// * `time` - Time to expiration T in years
/// * `rate` - Continuously compounded risk-free rate
/// * `volatility` - Annualized implied volatility
///
/// # Returns
/// Put option premium in currency units. Returns intrinsic value max(K-S, 0) if time ≤ 0.
///
/// # References
/// - Black-Scholes (1973), derived via put-call parity
pub fn black_scholes_put(spot: f64, strike: f64, time: f64, rate: f64, volatility: f64) -> f64 {
    if time <= 0.0 {
        return (strike - spot).max(0.0); // Intrinsic value at expiry
    }
    
    let (d1, d2) = d1_d2(spot, strike, time, rate, volatility);
    strike * (-rate * time).exp() * norm_cdf(-d2) - spot * norm_cdf(-d1)
}

/// Calculates implied volatility using Newton-Raphson iteration.
///
/// # Description
/// Solves for σ in the Black-Scholes equation given observed market price.
/// Uses Newton-Raphson method: σₙ₊₁ = σₙ - (C(σₙ) - Cₘₐᵣₖₑₜ) / vega(σₙ)
///
/// # Parameters
/// * `market_price` - Observed option price in market (must be positive)
/// * `spot` - Current underlying price S
/// * `strike` - Strike price K
/// * `time` - Time to expiration T in years (must be positive)
/// * `rate` - Risk-free interest rate r
/// * `is_call` - `true` for call option, `false` for put option
///
/// # Returns
/// * `Some(volatility)` - Implied volatility if convergence achieved (tolerance: 1e-6)
/// * `None` - If failed to converge within 100 iterations or invalid inputs
///
/// # Examples
/// ```
/// use kubera_options::pricing::{black_scholes_call, implied_volatility};
///
/// let spot = 25800.0;
/// let strike = 25800.0;
/// let time = 7.0 / 365.0;
/// let rate = 0.065;
/// let known_vol = 0.15;
///
/// let price = black_scholes_call(spot, strike, time, rate, known_vol);
/// let iv = implied_volatility(price, spot, strike, time, rate, true).unwrap();
/// assert!((iv - known_vol).abs() < 0.001);
/// ```
///
/// # References
/// - Newton-Raphson numerical method
/// - Hull, J. (2018). Options, Futures, and Other Derivatives, 10th ed.
pub fn implied_volatility(
    market_price: f64,
    spot: f64,
    strike: f64,
    time: f64,
    rate: f64,
    is_call: bool,
) -> Option<f64> {
    const MAX_ITERATIONS: u32 = 100;
    const TOLERANCE: f64 = 1e-6;
    
    let mut vol = 0.20; // Initial guess: 20%
    
    for _ in 0..MAX_ITERATIONS {
        let price = if is_call {
            black_scholes_call(spot, strike, time, rate, vol)
        } else {
            black_scholes_put(spot, strike, time, rate, vol)
        };
        
        let diff = price - market_price;
        
        if diff.abs() < TOLERANCE {
            return Some(vol);
        }
        
        // Vega (sensitivity to volatility)
        let (d1, _) = d1_d2(spot, strike, time, rate, vol);
        let vega = spot * time.sqrt() * norm_pdf(d1);
        
        if vega.abs() < 1e-10 {
            return None; // Avoid division by zero
        }
        
        vol = vol - diff / vega;
        
        // Keep vol in reasonable bounds
        if vol <= 0.001 {
            vol = 0.001;
        } else if vol > 5.0 {
            vol = 5.0;
        }
    }
    
    None // Failed to converge
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_black_scholes_call() {
        // NIFTY at 25800, strike 25800, 7 days, 5% rate, 15% vol
        let price = black_scholes_call(25800.0, 25800.0, 7.0/365.0, 0.05, 0.15);
        assert!(price > 100.0 && price < 300.0, "ATM call should be around 200-250: {}", price);
    }

    #[test]
    fn test_put_call_parity() {
        // Put-Call Parity: C - P = S - K*e^(-rT)
        let spot = 25800.0;
        let strike = 25800.0;
        let time = 30.0 / 365.0;
        let rate = 0.05;
        let vol = 0.18;
        
        let call = black_scholes_call(spot, strike, time, rate, vol);
        let put = black_scholes_put(spot, strike, time, rate, vol);
        let parity = call - put;
        let expected = spot - strike * (-rate * time).exp();
        
        assert!((parity - expected).abs() < 1.0, "Put-call parity violated: {} vs {}", parity, expected);
    }

    #[test]
    fn test_implied_volatility() {
        let spot = 25800.0;
        let strike = 25800.0;
        let time = 7.0 / 365.0;
        let rate = 0.05;
        let vol = 0.15;
        
        let price = black_scholes_call(spot, strike, time, rate, vol);
        let iv = implied_volatility(price, spot, strike, time, rate, true).unwrap();
        
        assert!((iv - vol).abs() < 0.001, "IV should match original vol: {} vs {}", iv, vol);
    }
}
