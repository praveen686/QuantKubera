use crate::chain::IVSurface;
use crate::strategy::StrategyType;
use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};

/// Signal types for options trading
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalType {
    /// IV is at low percentile - good for buying options/straddles
    IVLow,
    /// IV is at high percentile - good for selling options
    IVHigh,
    /// IV skew is extreme - potential mean reversion opportunity
    SkewExtreme,
    /// IV term structure inverted - near-term uncertainty
    TermStructureInverted,
    /// Delta-neutral opportunity detected
    DeltaNeutral,
}

/// A trading signal for options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionsSignal {
    pub timestamp: DateTime<Utc>,
    pub underlying: String,
    pub signal_type: SignalType,
    pub strength: f64,  // 0.0 to 1.0
    pub suggested_strategy: StrategyType,
    pub expiry: NaiveDate,
    pub details: String,
}

/// Options signal generator based on IV analysis
pub struct OptionsSignalGenerator {
    /// Historical IV range for percentile calculation
    iv_history: Vec<(DateTime<Utc>, f64)>,
    /// Thresholds
    iv_low_percentile: f64,
    iv_high_percentile: f64,
    skew_threshold: f64,
}

impl OptionsSignalGenerator {
    pub fn new() -> Self {
        Self {
            iv_history: Vec::new(),
            iv_low_percentile: 20.0,
            iv_high_percentile: 80.0,
            skew_threshold: 0.03, // 3% skew is significant
        }
    }

    /// Add IV observation to history
    pub fn add_iv_observation(&mut self, timestamp: DateTime<Utc>, iv: f64) {
        self.iv_history.push((timestamp, iv));
        // Keep last 252 trading days (1 year)
        if self.iv_history.len() > 252 {
            self.iv_history.remove(0);
        }
    }

    /// Calculate IV percentile
    pub fn iv_percentile(&self, current_iv: f64) -> f64 {
        if self.iv_history.is_empty() {
            return 50.0;
        }
        
        let count_below = self.iv_history.iter()
            .filter(|(_, iv)| *iv < current_iv)
            .count();
        
        (count_below as f64 / self.iv_history.len() as f64) * 100.0
    }

    /// Generate signals from IV surface
    pub fn generate_signals(&self, surface: &IVSurface, expiry: NaiveDate) -> Vec<OptionsSignal> {
        let mut signals = Vec::new();
        
        // Get ATM IV
        if let Some(atm_iv) = surface.atm_iv(expiry) {
            let percentile = self.iv_percentile(atm_iv);
            
            // Low IV signal - buy straddles
            if percentile < self.iv_low_percentile {
                signals.push(OptionsSignal {
                    timestamp: Utc::now(),
                    underlying: surface.underlying.clone(),
                    signal_type: SignalType::IVLow,
                    strength: (self.iv_low_percentile - percentile) / self.iv_low_percentile,
                    suggested_strategy: StrategyType::Straddle,
                    expiry,
                    details: format!("IV at {:.1}% percentile ({:.1}% IV). Consider buying straddle.", 
                                   percentile, atm_iv * 100.0),
                });
            }
            
            // High IV signal - sell straddles/iron condors
            if percentile > self.iv_high_percentile {
                signals.push(OptionsSignal {
                    timestamp: Utc::now(),
                    underlying: surface.underlying.clone(),
                    signal_type: SignalType::IVHigh,
                    strength: (percentile - self.iv_high_percentile) / (100.0 - self.iv_high_percentile),
                    suggested_strategy: StrategyType::IronCondor,
                    expiry,
                    details: format!("IV at {:.1}% percentile ({:.1}% IV). Consider selling premium.", 
                                   percentile, atm_iv * 100.0),
                });
            }
        }
        
        // Check IV skew
        if let Some(skew) = surface.iv_skew(expiry, 200.0) {
            if skew.abs() > self.skew_threshold {
                signals.push(OptionsSignal {
                    timestamp: Utc::now(),
                    underlying: surface.underlying.clone(),
                    signal_type: SignalType::SkewExtreme,
                    strength: (skew.abs() - self.skew_threshold) / self.skew_threshold,
                    suggested_strategy: if skew > 0.0 { 
                        StrategyType::BullCallSpread 
                    } else { 
                        StrategyType::BearPutSpread 
                    },
                    expiry,
                    details: format!("IV skew at {:.1}%. {} puts more expensive.", 
                                   skew * 100.0, if skew > 0.0 { "OTM" } else { "OTM" }),
                });
            }
        }
        
        signals
    }
}

impl Default for OptionsSignalGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::IVSurface;
    use crate::contract::OptionType;
    use crate::chain::IVPoint;

    #[test]
    fn test_iv_percentile() {
        let mut generator = OptionsSignalGenerator::new();
        
        // Add some historical IV data
        for i in 0..100 {
            generator.add_iv_observation(Utc::now(), 0.10 + (i as f64 * 0.001));
        }
        
        // Current IV at 15% should be around 50th percentile
        let percentile = generator.iv_percentile(0.15);
        assert!(percentile > 40.0 && percentile < 60.0, "Percentile should be near 50: {}", percentile);
    }

    #[test]
    fn test_low_iv_signal() {
        let mut generator = OptionsSignalGenerator::new();
        
        // Add high IV history
        for i in 0..100 {
            generator.add_iv_observation(Utc::now(), 0.20 + (i as f64 * 0.001));
        }
        
        // Create surface with low IV
        let expiry = NaiveDate::from_ymd_opt(2024, 12, 26).unwrap();
        let mut surface = IVSurface::new("NIFTY".to_string(), 25800.0);
        surface.points.push(IVPoint {
            strike: 25800.0,
            expiry,
            option_type: OptionType::Call,
            iv: 0.12, // Low IV
            delta: 0.5,
            last_price: 150.0,
        });
        
        let signals = generator.generate_signals(&surface, expiry);
        assert!(!signals.is_empty(), "Should generate low IV signal");
        assert!(matches!(signals[0].signal_type, SignalType::IVLow));
    }
}
