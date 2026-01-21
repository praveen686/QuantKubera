//! # Options Strategy Module
//!
//! Multi-leg option strategy definitions and risk/reward modeling.
//!
//! ## Description
//! Provides structures and logic for composing complex options strategies
//! (e.g., Straddles, Iron Condors). Implements aggregate risk metrics 
//! and profit/loss surface calculations.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use crate::contract::{OptionContract, OptionType, OptionChain};
use crate::greeks::OptionGreeks;
use serde::{Deserialize, Serialize};

/// Classification of common options multi-leg patterns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StrategyType {
    /// Simultaneous Buy/Sell of Call and Put at same strike.
    Straddle,
    /// Simultaneous Buy/Sell of Call and Put at different strikes.
    Strangle,
    /// Neutral strategy with capped profit and loss using four strikes.
    IronCondor,
    /// Long stock position with short call.
    CoveredCall,
    /// Long stock position with long put.
    ProtectivePut,
    /// Long call at lower strike, short call at higher strike.
    BullCallSpread,
    /// Long put at higher strike, short put at lower strike.
    BearPutSpread,
}

/// Individual component (leg) of a multi-leg strategy.
///
/// # Fields
/// * `contract` - The specific option instrument.
/// * `quantity` - Signed weight (Positive = Buy, Negative = Sell).
/// * `entry_price` - Average fill price for the leg.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyLeg {
    pub contract: OptionContract,
    pub quantity: i32,  
    pub entry_price: f64,
}

impl StrategyLeg {
    /// Returns true if the leg is a purchase.
    pub fn is_long(&self) -> bool {
        self.quantity > 0
    }

    /// Returns true if the leg is a sale.
    pub fn is_short(&self) -> bool {
        self.quantity < 0
    }
}

/// Logical composite of multiple option legs forming a single intent.
#[derive(Debug, Clone)]
pub struct OptionsStrategy {
    pub name: String,
    pub strategy_type: StrategyType,
    pub legs: Vec<StrategyLeg>,
    /// Underlying benchmark price at time of creation.
    pub underlying_price: f64,
}

impl OptionsStrategy {
    /// Calculates net cash flow required for the strategy.
    ///
    /// # Returns
    /// Value > 0: Net debit (payment).
    /// Value < 0: Net credit (receipt).
    pub fn net_premium(&self) -> f64 {
        self.legs.iter()
            .map(|leg| leg.entry_price * leg.quantity as f64)
            .sum()
    }

    /// Theoretical maximum gain if profit is bounded.
    ///
    /// # Returns
    /// `Some(value)` for defined-risk, `None` for unlimited profit strategies.
    pub fn max_profit(&self) -> Option<f64> {
        match self.strategy_type {
            StrategyType::Straddle => None, 
            StrategyType::IronCondor => {
                let premium = -self.net_premium(); 
                Some(premium)
            }
            _ => None,
        }
    }

    /// Theoretical maximum loss if loss is bounded.
    pub fn max_loss(&self) -> Option<f64> {
        match self.strategy_type {
            StrategyType::Straddle => {
                Some(self.net_premium())
            }
            StrategyType::IronCondor => {
                if self.legs.len() == 4 {
                    let put_spread_width = (self.legs[1].contract.strike - self.legs[0].contract.strike).abs();
                    let premium = -self.net_premium();
                    Some(put_spread_width - premium)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Computes aggregate Greeks by summing weighted leg sensitivities.
    ///
    /// # Parameters
    /// * `spot` - Underlying price.
    /// * `time` - Time to expiry.
    /// * `rate` - Interest rate.
    /// * `volatility` - Implied volatility.
    pub fn greeks(&self, spot: f64, time: f64, rate: f64, volatility: f64) -> OptionGreeks {
        let mut total = OptionGreeks::default();
        
        for leg in &self.legs {
            let leg_greeks = match leg.contract.option_type {
                OptionType::Call => OptionGreeks::for_call(spot, leg.contract.strike, time, rate, volatility),
                OptionType::Put => OptionGreeks::for_put(spot, leg.contract.strike, time, rate, volatility),
            };
            total = total.add(&leg_greeks.scale(leg.quantity as f64));
        }
        
        total
    }
}

/// Builder for constructing an At-The-Money (ATM) Straddle.
pub fn build_straddle(chain: &OptionChain, quantity: i32) -> Option<OptionsStrategy> {
    let (call, put) = chain.atm_straddle()?;
    
    Some(OptionsStrategy {
        name: format!("{} Straddle {}", chain.underlying, chain.expiry),
        strategy_type: StrategyType::Straddle,
        legs: vec![
            StrategyLeg { contract: call, quantity, entry_price: 0.0 },
            StrategyLeg { contract: put, quantity, entry_price: 0.0 },
        ],
        underlying_price: chain.spot_price,
    })
}

/// Builder for constructing a neutral Iron Condor strategy.
pub fn build_iron_condor(
    chain: &OptionChain,
    put_sell_strike: f64,
    put_buy_strike: f64,
    call_sell_strike: f64,
    call_buy_strike: f64,
    quantity: i32,
) -> Option<OptionsStrategy> {
    let put_buy = chain.puts.iter().find(|p| (p.strike - put_buy_strike).abs() < 0.01)?;
    let put_sell = chain.puts.iter().find(|p| (p.strike - put_sell_strike).abs() < 0.01)?;
    let call_sell = chain.calls.iter().find(|c| (c.strike - call_sell_strike).abs() < 0.01)?;
    let call_buy = chain.calls.iter().find(|c| (c.strike - call_buy_strike).abs() < 0.01)?;
    
    Some(OptionsStrategy {
        name: format!("{} Iron Condor {}", chain.underlying, chain.expiry),
        strategy_type: StrategyType::IronCondor,
        legs: vec![
            StrategyLeg { contract: put_buy.clone(), quantity, entry_price: 0.0 },
            StrategyLeg { contract: put_sell.clone(), quantity: -quantity, entry_price: 0.0 },
            StrategyLeg { contract: call_sell.clone(), quantity: -quantity, entry_price: 0.0 },
            StrategyLeg { contract: call_buy.clone(), quantity, entry_price: 0.0 },
        ],
        underlying_price: chain.spot_price,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_straddle_delta_near_zero() {
        // ATM straddle should have near-zero delta
        let spot = 25800.0;
        let time = 7.0 / 365.0;
        let rate = 0.05;
        let vol = 0.15;

        let straddle_greeks = OptionGreeks::for_call(spot, spot, time, rate, vol)
            .add(&OptionGreeks::for_put(spot, spot, time, rate, vol));
        
        assert!(straddle_greeks.delta.abs() < 0.1, "Straddle delta should be near zero");
    }
}
