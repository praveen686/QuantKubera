use crate::strategy::OptionsStrategy;
use serde::{Deserialize, Serialize};

/// Options margin calculation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionsMargin {
    /// SPAN-style margin (worst-case scenario)
    pub span_margin: f64,
    /// Exposure margin (additional buffer)
    pub exposure_margin: f64,
    /// Total margin required
    pub total_margin: f64,
    /// Premium margin (for short options)
    pub premium_margin: f64,
}

impl OptionsMargin {
    /// Calculate margin for a single option position
    /// Based on simplified SPAN methodology
    pub fn for_option(
        spot_price: f64,
        strike: f64,
        is_call: bool,
        is_short: bool,
        quantity: i32,
        lot_size: u32,
        volatility: f64,
    ) -> Self {
        let notional = spot_price * quantity.abs() as f64 * lot_size as f64;
        
        // SPAN parameters (NSE-like)
        let price_scan_range = 0.035; // 3.5% price move
        let vol_scan_range = 0.25;    // 25% vol move
        
        // Calculate potential loss under stress scenarios
        let stress_up = spot_price * (1.0 + price_scan_range);
        let stress_down = spot_price * (1.0 - price_scan_range);
        
        let intrinsic_up = if is_call { (stress_up - strike).max(0.0) } else { (strike - stress_up).max(0.0) };
        let intrinsic_down = if is_call { (stress_down - strike).max(0.0) } else { (strike - stress_down).max(0.0) };
        
        // SPAN margin = worst case loss
        let span = if is_short {
            let worst_loss = intrinsic_up.max(intrinsic_down) * quantity.abs() as f64 * lot_size as f64;
            worst_loss * (1.0 + vol_scan_range * volatility)
        } else {
            0.0 // Long options have limited risk
        };
        
        // Exposure margin = 3% of notional
        let exposure = if is_short { notional * 0.03 } else { 0.0 };
        
        // Premium margin for short options
        let premium = if is_short {
            let option_price = {
                let time = 7.0 / 365.0; // Assume 1 week
                let rate = 0.065;
                if is_call {
                    crate::pricing::black_scholes_call(spot_price, strike, time, rate, volatility)
                } else {
                    crate::pricing::black_scholes_put(spot_price, strike, time, rate, volatility)
                }
            };
            option_price * quantity.abs() as f64 * lot_size as f64
        } else {
            0.0
        };
        
        Self {
            span_margin: span,
            exposure_margin: exposure,
            total_margin: span + exposure + premium,
            premium_margin: premium,
        }
    }

    /// Calculate margin for a multi-leg strategy (with hedge benefit)
    pub fn for_strategy(strategy: &OptionsStrategy, lot_size: u32, volatility: f64) -> Self {
        let mut total_span = 0.0;
        let mut total_exposure = 0.0;
        let mut total_premium = 0.0;
        
        // Calculate individual leg margins
        for leg in &strategy.legs {
            let is_call = matches!(leg.contract.option_type, crate::contract::OptionType::Call);
            let margin = Self::for_option(
                strategy.underlying_price,
                leg.contract.strike,
                is_call,
                leg.quantity < 0,
                leg.quantity,
                lot_size,
                volatility,
            );
            
            total_span += margin.span_margin;
            total_exposure += margin.exposure_margin;
            total_premium += margin.premium_margin;
        }
        
        // Apply hedge benefit (30% reduction for spreads)
        let hedge_benefit = match strategy.strategy_type {
            crate::strategy::StrategyType::IronCondor => 0.30,
            crate::strategy::StrategyType::BullCallSpread | 
            crate::strategy::StrategyType::BearPutSpread => 0.30,
            _ => 0.0,
        };
        
        let adjusted_span = total_span * (1.0 - hedge_benefit);
        
        Self {
            span_margin: adjusted_span,
            exposure_margin: total_exposure,
            total_margin: adjusted_span + total_exposure + total_premium,
            premium_margin: total_premium,
        }
    }
}

/// Portfolio-level Greeks aggregation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PortfolioGreeks {
    pub total_delta: f64,
    pub total_gamma: f64,
    pub total_theta: f64,
    pub total_vega: f64,
    pub delta_dollars: f64,  // Delta * spot * lot_size
    pub gamma_dollars: f64,  // Gamma * spot^2 * lot_size / 100
}

impl PortfolioGreeks {
    /// Aggregate Greeks from multiple strategies
    pub fn from_strategies(
        strategies: &[OptionsStrategy],
        spot_price: f64,
        time: f64,
        rate: f64,
        volatility: f64,
        lot_size: u32,
    ) -> Self {
        let mut total = Self::default();
        
        for strategy in strategies {
            let greeks = strategy.greeks(spot_price, time, rate, volatility);
            
            // Multiply by total quantity in strategy
            let total_qty: i32 = strategy.legs.iter().map(|l| l.quantity.abs()).sum();
            
            total.total_delta += greeks.delta * total_qty as f64;
            total.total_gamma += greeks.gamma * total_qty as f64;
            total.total_theta += greeks.theta * total_qty as f64;
            total.total_vega += greeks.vega * total_qty as f64;
        }
        
        // Calculate dollar values
        total.delta_dollars = total.total_delta * spot_price * lot_size as f64;
        total.gamma_dollars = total.total_gamma * spot_price * spot_price * lot_size as f64 / 100.0;
        
        total
    }

    /// Check if portfolio exceeds risk limits
    pub fn check_limits(&self, max_delta: f64, max_gamma: f64, max_vega: f64) -> Vec<String> {
        let mut violations = Vec::new();
        
        if self.total_delta.abs() > max_delta {
            violations.push(format!("Delta limit exceeded: {:.2} > {:.2}", self.total_delta.abs(), max_delta));
        }
        
        if self.total_gamma.abs() > max_gamma {
            violations.push(format!("Gamma limit exceeded: {:.4} > {:.4}", self.total_gamma.abs(), max_gamma));
        }
        
        if self.total_vega.abs() > max_vega {
            violations.push(format!("Vega limit exceeded: {:.2} > {:.2}", self.total_vega.abs(), max_vega));
        }
        
        violations
    }
}

/// Value at Risk (VaR) calculator for options positions
#[derive(Debug, Clone)]
pub struct OptionsVaR {
    /// Confidence level (e.g., 0.99 for 99%)
    confidence_level: f64,
    /// Time horizon in days
    time_horizon: u32,
    /// Historical volatility
    volatility: f64,
}

/// VaR calculation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaRResult {
    /// VaR amount (potential loss)
    pub var_amount: f64,
    /// Confidence level used
    pub confidence_level: f64,
    /// Time horizon in days
    pub time_horizon: u32,
    /// Expected shortfall (CVaR) - average loss beyond VaR
    pub expected_shortfall: f64,
    /// Delta VaR contribution
    pub delta_var: f64,
    /// Gamma VaR contribution
    pub gamma_var: f64,
    /// Vega VaR contribution
    pub vega_var: f64,
}

impl OptionsVaR {
    pub fn new(confidence_level: f64, time_horizon: u32, volatility: f64) -> Self {
        Self {
            confidence_level,
            time_horizon,
            volatility,
        }
    }

    /// Calculate parametric VaR using delta-gamma approximation
    pub fn calculate(
        &self,
        portfolio_greeks: &PortfolioGreeks,
        spot_price: f64,
        lot_size: u32,
    ) -> VaRResult {
        // Z-score for confidence level (e.g., 2.33 for 99%)
        let z_score = self.z_score_for_confidence(self.confidence_level);
        
        // Time scaling factor
        let time_factor = (self.time_horizon as f64 / 252.0).sqrt();
        
        // Expected price move
        let price_move = spot_price * self.volatility * time_factor * z_score;
        
        // Delta VaR: |Delta| * price_move
        let delta_var = portfolio_greeks.total_delta.abs() * price_move * lot_size as f64;
        
        // Gamma VaR: 0.5 * Gamma * price_move^2
        let gamma_var = 0.5 * portfolio_greeks.total_gamma.abs() * price_move.powi(2) * lot_size as f64;
        
        // Vega VaR: Assume 20% vol move
        let vol_move = self.volatility * 0.20;
        let vega_var = portfolio_greeks.total_vega.abs() * vol_move * lot_size as f64 * 100.0;
        
        // Total VaR (using quadratic approximation)
        let var_amount = (delta_var.powi(2) + gamma_var.powi(2) + vega_var.powi(2)).sqrt();
        
        // Expected Shortfall (CVaR) ≈ 1.27 * VaR for normal distribution at 99%
        let es_multiplier = 1.0 + z_score / 10.0; // Simplified
        let expected_shortfall = var_amount * es_multiplier;
        
        VaRResult {
            var_amount,
            confidence_level: self.confidence_level,
            time_horizon: self.time_horizon,
            expected_shortfall,
            delta_var,
            gamma_var,
            vega_var,
        }
    }

    /// Get z-score for a given confidence level
    fn z_score_for_confidence(&self, confidence: f64) -> f64 {
        match confidence {
            c if c >= 0.99 => 2.33,
            c if c >= 0.975 => 1.96,
            c if c >= 0.95 => 1.645,
            c if c >= 0.90 => 1.28,
            _ => 1.645,
        }
    }

    /// Historical VaR using scenario analysis
    pub fn historical_var(
        &self,
        portfolio_greeks: &PortfolioGreeks,
        spot_price: f64,
        historical_returns: &[f64],
        lot_size: u32,
    ) -> f64 {
        if historical_returns.is_empty() {
            return 0.0;
        }
        
        // Calculate P&L for each historical return
        let mut pnls: Vec<f64> = historical_returns.iter().map(|ret| {
            let price_change = spot_price * ret;
            let delta_pnl = portfolio_greeks.total_delta * price_change * lot_size as f64;
            let gamma_pnl = 0.5 * portfolio_greeks.total_gamma * price_change.powi(2) * lot_size as f64;
            delta_pnl + gamma_pnl
        }).collect();
        
        // Sort and find VaR percentile
        pnls.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let var_index = ((1.0 - self.confidence_level) * pnls.len() as f64).floor() as usize;
        
        // Return the negative of the VaR percentile (loss is positive)
        -pnls.get(var_index).copied().unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_option_margin() {
        let margin = OptionsMargin::for_option(
            25800.0, // spot
            25800.0, // strike (ATM)
            true,    // call
            true,    // short
            1,       // quantity
            50,      // lot size
            0.15,    // volatility
        );
        
        assert!(margin.total_margin > 0.0, "Short option should require margin");
        assert!(margin.span_margin > 0.0, "SPAN margin should be positive");
    }

    #[test]
    fn test_long_option_no_margin() {
        let margin = OptionsMargin::for_option(
            25800.0,
            25800.0,
            true,
            false, // long
            1,
            50,
            0.15,
        );
        
        assert_eq!(margin.span_margin, 0.0, "Long option should have no SPAN margin");
        assert_eq!(margin.total_margin, 0.0, "Long option should have no margin requirement");
    }

    #[test]
    fn test_portfolio_delta_limit() {
        let mut greeks = PortfolioGreeks::default();
        greeks.total_delta = 15.0;
        
        let violations = greeks.check_limits(10.0, 1.0, 1000.0);
        assert!(!violations.is_empty(), "Should detect delta limit violation");
    }

    #[test]
    fn test_options_var_calculation() {
        let var_calc = OptionsVaR::new(0.99, 1, 0.15);
        
        let mut greeks = PortfolioGreeks::default();
        greeks.total_delta = 5.0;  // 5 lots equivalent
        greeks.total_gamma = 0.05;
        greeks.total_vega = 50.0;
        
        let var_result = var_calc.calculate(&greeks, 25800.0, 50);
        
        assert!(var_result.var_amount > 0.0, "VaR should be positive");
        assert!(var_result.delta_var > 0.0, "Delta VaR should be positive");
        assert_eq!(var_result.confidence_level, 0.99);
    }

    #[test]
    fn test_historical_var() {
        let var_calc = OptionsVaR::new(0.99, 1, 0.15);
        
        let mut greeks = PortfolioGreeks::default();
        greeks.total_delta = 5.0;
        
        // Historical returns simulating actual market moves
        let returns = vec![-0.02, -0.01, 0.005, 0.01, -0.015, 0.02, -0.025, 0.003, -0.008, 0.012];
        
        let hist_var = var_calc.historical_var(&greeks, 25800.0, &returns, 50);
        
        assert!(hist_var >= 0.0, "Historical VaR should be non-negative");
    }
}
