//! # Options Analytics Module
//!
//! Provides advanced analytical tools for options trading and risk management.
//!
//! ## Description
//! Implements high-level analytical components including:
//! - **Greeks Decay Projection**: Temporal simulation of option sensitivities.
//! - **IV Mean Reversion**: Statistical signal generation for volatility trading.
//! - **Visualization Data**: Heatmaps and 3D surface generators for frontend integration.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Black-Scholes-Merton (1973) model extensions

use crate::greeks::OptionGreeks;
use crate::chain::IVSurface;
use crate::contract::OptionType;
use chrono::{NaiveDate, DateTime, Utc, Duration};
use serde::Serialize;
use std::collections::HashMap;

/// Simulator for temporal evolution of option Greeks.
///
/// # Description
/// Projects how delta, gamma, theta, and vega change as time to expiry decreases,
/// holding other parameters constant.
///
/// # Fields
/// * `spot_price` - Current price of the underlying asset.
/// * `rate` - Risk-free interest rate (annualized).
/// * `volatility` - Implied volatility (annualized).
#[derive(Debug, Clone)]
pub struct GreeksDecayProjector {
    spot_price: f64,
    rate: f64,
    volatility: f64,
}

/// Snapshot of projected Greeks at a specific future interval.
#[derive(Debug, Clone, Serialize)]
pub struct GreeksProjection {
    /// Number of days from project start.
    pub days_forward: i32,
    /// Calendar date of the projection point.
    pub date: NaiveDate,
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub option_value: f64,
}

impl GreeksDecayProjector {
    /// Constructs a new projector with market environment parameters.
    pub fn new(spot_price: f64, rate: f64, volatility: f64) -> Self {
        Self { spot_price, rate, volatility }
    }

    /// Projects decay for a single option contract.
    ///
    /// # Parameters
    /// * `strike` - Option strike price.
    /// * `expiry` - Contract expiration date.
    /// * `option_type` - Call or Put.
    /// * `start_date` - Beginning of projection.
    /// * `days_forward` - Duration of projection in days.
    ///
    /// # Returns
    /// Vector of projections for each daily interval.
    pub fn project_decay(
        &self,
        strike: f64,
        expiry: NaiveDate,
        option_type: OptionType,
        start_date: NaiveDate,
        days_forward: i32,
    ) -> Vec<GreeksProjection> {
        let mut projections = Vec::new();
        let is_call = matches!(option_type, OptionType::Call);
        
        for day in 0..=days_forward {
            let current_date = start_date + Duration::days(day as i64);
            let days_to_expiry = (expiry - current_date).num_days() as f64;
            
            if days_to_expiry <= 0.0 {
                break;
            }
            
            let time = days_to_expiry / 365.0;
            
            let greeks = if is_call {
                OptionGreeks::for_call(self.spot_price, strike, time, self.rate, self.volatility)
            } else {
                OptionGreeks::for_put(self.spot_price, strike, time, self.rate, self.volatility)
            };
            
            let option_value = if is_call {
                crate::pricing::black_scholes_call(self.spot_price, strike, time, self.rate, self.volatility)
            } else {
                crate::pricing::black_scholes_put(self.spot_price, strike, time, self.rate, self.volatility)
            };
            
            projections.push(GreeksProjection {
                days_forward: day,
                date: current_date,
                delta: greeks.delta,
                gamma: greeks.gamma,
                theta: greeks.theta,
                vega: greeks.vega,
                option_value,
            });
        }
        
        projections
    }

    /// Projects aggregate decay for a straddle (Equal Call + Put).
    pub fn project_straddle_decay(
        &self,
        strike: f64,
        expiry: NaiveDate,
        start_date: NaiveDate,
        days_forward: i32,
    ) -> Vec<GreeksProjection> {
        let call_decay = self.project_decay(strike, expiry, OptionType::Call, start_date, days_forward);
        let put_decay = self.project_decay(strike, expiry, OptionType::Put, start_date, days_forward);
        
        call_decay.iter()
            .zip(put_decay.iter())
            .map(|(c, p)| GreeksProjection {
                days_forward: c.days_forward,
                date: c.date,
                delta: c.delta + p.delta,
                gamma: c.gamma + p.gamma,
                theta: c.theta + p.theta,
                vega: c.vega + p.vega,
                option_value: c.option_value + p.option_value,
            })
            .collect()
    }
}

/// Statistical signal generator for volatility mean reversion.
///
/// # Description
/// Uses Z-score analysis of historical implied volatility to identify
/// over-extended IV relative to a specified lookback window.
#[derive(Debug, Clone)]
pub struct IVMeanReversionSignal {
    lookback_period: usize,
    iv_history: Vec<(DateTime<Utc>, f64)>,
    z_score_threshold: f64,
}

/// Descriptive signal output for mean-reverting alpha.
#[derive(Debug, Clone, Serialize)]
pub struct MeanReversionSignal {
    pub timestamp: DateTime<Utc>,
    pub current_iv: f64,
    pub mean_iv: f64,
    pub std_iv: f64,
    pub z_score: f64,
    pub signal: MeanReversionDirection,
    /// Normalized signal confidence (0.0 to 1.0).
    pub strength: f64, 
}

/// Directional intent for volatility trading.
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub enum MeanReversionDirection {
    /// IV is statistically low; favoring Long Vol.
    BuyVolatility,
    /// IV is statistically high; favoring Short Vol.
    SellVolatility,
    /// Within normal bounds.
    Neutral,
}

impl IVMeanReversionSignal {
    /// Constructs a new signal generator.
    ///
    /// # Parameters
    /// * `lookback_period` - Number of observations for moving average.
    /// * `z_score_threshold` - Standard deviation boundary for triggers.
    pub fn new(lookback_period: usize, z_score_threshold: f64) -> Self {
        Self {
            lookback_period,
            iv_history: Vec::new(),
            z_score_threshold,
        }
    }

    /// Ingests a new Implied Volatility data point.
    pub fn add_observation(&mut self, timestamp: DateTime<Utc>, iv: f64) {
        self.iv_history.push((timestamp, iv));
        if self.iv_history.len() > self.lookback_period {
            self.iv_history.remove(0);
        }
    }

    /// Evaluates current IV against historical distribution.
    ///
    /// # Returns
    /// `Some(MeanReversionSignal)` if calculation is possible, `None` if data is insufficient.
    pub fn generate_signal(&self, current_iv: f64) -> Option<MeanReversionSignal> {
        if self.iv_history.len() < 20 {
            return None; 
        }

        let ivs: Vec<f64> = self.iv_history.iter().map(|(_, iv)| *iv).collect();
        let mean = ivs.iter().sum::<f64>() / ivs.len() as f64;
        let variance = ivs.iter().map(|iv| (iv - mean).powi(2)).sum::<f64>() / ivs.len() as f64;
        let std = variance.sqrt();
        
        if std < 0.001 {
            return None; 
        }
        
        let z_score = (current_iv - mean) / std;
        
        let (signal, strength) = if z_score < -self.z_score_threshold {
            (MeanReversionDirection::BuyVolatility, (z_score.abs() - self.z_score_threshold) / self.z_score_threshold)
        } else if z_score > self.z_score_threshold {
            (MeanReversionDirection::SellVolatility, (z_score.abs() - self.z_score_threshold) / self.z_score_threshold)
        } else {
            (MeanReversionDirection::Neutral, 0.0)
        };
        
        Some(MeanReversionSignal {
            timestamp: Utc::now(),
            current_iv,
            mean_iv: mean,
            std_iv: std,
            z_score,
            signal,
            strength: strength.min(1.0),
        })
    }
}

/// Matrix representation of Greeks across strikes for a single expiry.
#[derive(Debug, Clone, Serialize)]
pub struct GreeksHeatmap {
    pub underlying: String,
    pub expiry: NaiveDate,
    pub spot_price: f64,
    pub strikes: Vec<f64>,
    pub data: Vec<GreeksHeatmapRow>,
}

/// Single row in a Greeks heatmap.
#[derive(Debug, Clone, Serialize)]
pub struct GreeksHeatmapRow {
    pub strike: f64,
    pub call_delta: f64,
    pub call_gamma: f64,
    pub call_theta: f64,
    pub call_vega: f64,
    pub call_iv: Option<f64>,
    pub put_delta: f64,
    pub put_gamma: f64,
    pub put_theta: f64,
    pub put_vega: f64,
    pub put_iv: Option<f64>,
}

impl GreeksHeatmap {
    /// Generates a heatmap derived from an IV surface snapshot.
    pub fn from_iv_surface(surface: &IVSurface, expiry: NaiveDate, rate: f64) -> Self {
        let time = (expiry - Utc::now().date_naive()).num_days() as f64 / 365.0;
        
        let mut strikes: Vec<f64> = surface.points.iter()
            .filter(|p| p.expiry == expiry)
            .map(|p| p.strike)
            .collect();
        strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
        strikes.dedup();
        
        let mut data = Vec::new();
        
        for strike in &strikes {
            let call_point = surface.points.iter()
                .find(|p| p.expiry == expiry && (p.strike - strike).abs() < 0.01 
                      && p.option_type == OptionType::Call);
            
            let put_point = surface.points.iter()
                .find(|p| p.expiry == expiry && (p.strike - strike).abs() < 0.01 
                      && p.option_type == OptionType::Put);
            
            let call_iv = call_point.map(|p| p.iv).unwrap_or(0.15);
            let put_iv = put_point.map(|p| p.iv).unwrap_or(0.15);
            
            let call_greeks = OptionGreeks::for_call(surface.spot_price, *strike, time, rate, call_iv);
            let put_greeks = OptionGreeks::for_put(surface.spot_price, *strike, time, rate, put_iv);
            
            data.push(GreeksHeatmapRow {
                strike: *strike,
                call_delta: call_greeks.delta,
                call_gamma: call_greeks.gamma,
                call_theta: call_greeks.theta,
                call_vega: call_greeks.vega,
                call_iv: call_point.map(|p| p.iv),
                put_delta: put_greeks.delta,
                put_gamma: put_greeks.gamma,
                put_theta: put_greeks.theta,
                put_vega: put_greeks.vega,
                put_iv: put_point.map(|p| p.iv),
            });
        }
        
        Self {
            underlying: surface.underlying.clone(),
            expiry,
            spot_price: surface.spot_price,
            strikes,
            data,
        }
    }

    /// Serializes current heatmap to a JSON format for UI rendering.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "underlying": self.underlying,
            "expiry": self.expiry.to_string(),
            "spot_price": self.spot_price,
            "strikes": self.strikes,
            "data": self.data,
        })
    }
}

/// 3D topology structure for Implied Volatility surfaces.
///
/// # Description
/// Defines a grid-based representation of IV across Strike (X) and Expiry (Y).
#[derive(Debug, Clone, Serialize)]
pub struct IVSurface3D {
    pub underlying: String,
    pub spot_price: f64,
    /// X-axis coordinate vector (Strikes).
    pub strikes: Vec<f64>,
    /// Y-axis coordinate vector (Expiration dates).
    pub expiries: Vec<NaiveDate>,
    /// Z-axis matrix [Expiry index][Strike index] of IV values.
    pub iv_matrix: Vec<Vec<f64>>,
    /// Z-axis matrix of Delta values for skew analysis.
    pub delta_matrix: Vec<Vec<f64>>, 
}

impl IVSurface3D {
    /// Constructs a 3D surface from a collection of IV data points.
    pub fn from_multi_expiry(
        underlying: &str,
        spot_price: f64,
        strikes: &[f64],
        expiries: &[NaiveDate],
        iv_data: &HashMap<(NaiveDate, i64), f64>,
        rate: f64,
    ) -> Self {
        let mut iv_matrix = Vec::new();
        let mut delta_matrix = Vec::new();
        
        for expiry in expiries {
            let mut iv_row = Vec::new();
            let mut delta_row = Vec::new();
            let time = (*expiry - Utc::now().date_naive()).num_days() as f64 / 365.0;
            
            for strike in strikes {
                let strike_key = (*strike * 100.0) as i64;
                let iv = iv_data.get(&(*expiry, strike_key)).copied().unwrap_or(0.15);
                iv_row.push(iv);
                
                let greeks = OptionGreeks::for_call(spot_price, *strike, time.max(0.01), rate, iv);
                delta_row.push(greeks.delta);
            }
            
            iv_matrix.push(iv_row);
            delta_matrix.push(delta_row);
        }
        
        Self {
            underlying: underlying.to_string(),
            spot_price,
            strikes: strikes.to_vec(),
            expiries: expiries.to_vec(),
            iv_matrix,
            delta_matrix,
        }
    }

    /// Interpolates IV at a specific point on the surface.
    pub fn get_iv(&self, strike: f64, expiry: NaiveDate) -> Option<f64> {
        let strike_idx = self.strikes.iter().position(|&s| (s - strike).abs() < 25.0)?;
        let expiry_idx = self.expiries.iter().position(|&e| e == expiry)?;
        
        self.iv_matrix.get(expiry_idx)?.get(strike_idx).copied()
    }

    /// Generates a plotting-friendly JSON structure.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "underlying": self.underlying,
            "spot_price": self.spot_price,
            "x_axis": {
                "label": "Strike",
                "values": self.strikes,
            },
            "y_axis": {
                "label": "Expiry",
                "values": self.expiries.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            },
            "z_axis": {
                "label": "IV",
                "values": self.iv_matrix,
            },
            "delta_surface": self.delta_matrix,
        })
    }

    /// Extracts the term structure (time-series) of IV for a specific strike.
    pub fn term_structure(&self, strike: f64) -> Vec<(NaiveDate, f64)> {
        let strike_idx = match self.strikes.iter().position(|&s| (s - strike).abs() < 25.0) {
            Some(i) => i,
            None => return Vec::new(),
        };
        
        self.expiries.iter()
            .zip(self.iv_matrix.iter())
            .filter_map(|(expiry, row)| {
                row.get(strike_idx).map(|iv| (*expiry, *iv))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_greeks_decay_projection() {
        let projector = GreeksDecayProjector::new(25800.0, 0.065, 0.15);
        let expiry = NaiveDate::from_ymd_opt(2024, 12, 26).unwrap();
        let start = NaiveDate::from_ymd_opt(2024, 12, 18).unwrap();
        
        let projections = projector.project_decay(25800.0, expiry, OptionType::Call, start, 7);
        
        assert_eq!(projections.len(), 8);
        // Theta should increase as we approach expiry (more negative)
        assert!(projections.last().unwrap().theta < projections.first().unwrap().theta);
    }

    #[test]
    fn test_mean_reversion_signal() {
        let mut signal_gen = IVMeanReversionSignal::new(30, 1.5);
        
        // Add historical IV data around 15%
        for i in 0..30 {
            signal_gen.add_observation(Utc::now(), 0.15 + (i as f64 * 0.001));
        }
        
        // Test low IV signal
        let signal = signal_gen.generate_signal(0.10);
        assert!(signal.is_some());
        assert_eq!(signal.unwrap().signal, MeanReversionDirection::BuyVolatility);
        
        // Test high IV signal
        let signal = signal_gen.generate_signal(0.25);
        assert!(signal.is_some());
        assert_eq!(signal.unwrap().signal, MeanReversionDirection::SellVolatility);
    }

    #[test]
    fn test_iv_surface_3d() {
        let strikes = vec![25600.0, 25700.0, 25800.0, 25900.0, 26000.0];
        let expiries = vec![
            NaiveDate::from_ymd_opt(2024, 12, 26).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
        ];
        
        let mut iv_data = HashMap::new();
        for expiry in &expiries {
            for strike in &strikes {
                let strike_key = (*strike * 100.0) as i64;
                iv_data.insert((*expiry, strike_key), 0.15);
            }
        }
        
        let surface = IVSurface3D::from_multi_expiry("NIFTY", 25800.0, &strikes, &expiries, &iv_data, 0.065);
        
        assert_eq!(surface.strikes.len(), 5);
        assert_eq!(surface.expiries.len(), 2);
        assert_eq!(surface.iv_matrix.len(), 2);
    }
}
