//! # Follow Through Index (FTI)
//! Zero-lag regime detection and trend persistence monitoring.
//! Ported and optimized from Timothy Masters' methodology.

use std::collections::VecDeque;
use std::f64::consts::PI;

/// Follow Through Index (FTI) engine.
#[derive(Debug, Clone)]
pub struct FtiEngine {
    min_period: usize,
    max_period: usize,
    half_length: usize,
    lookback: usize,
    beta: f64,      // Quantile for width (e.g. 0.9)
    noise_cut: f64, // Noise threshold (e.g. 0.2)
    
    // Predetermined Otnes filter coefficients
    // Map: period -> coefficients
    coef_table: Vec<Vec<f64>>,
}

impl FtiEngine {
    pub fn new(
        min_period: usize,
        max_period: usize,
        half_length: usize,
        lookback: usize,
        beta: f64,
        noise_cut: f64,
    ) -> Self {
        let mut coef_table = Vec::with_capacity(max_period - min_period + 1);
        for period in min_period..=max_period {
            coef_table.push(Self::compute_otnes_coefs(period, half_length));
        }

        Self {
            min_period,
            max_period,
            half_length,
            lookback,
            beta,
            noise_cut,
            coef_table,
        }
    }

    /// Computes Otnes FIR filter coefficients.
    fn compute_otnes_coefs(period: usize, half_length: usize) -> Vec<f64> {
        let mut c = vec![0.0; half_length + 1];
        let d = [0.35577019, 0.2436983, 0.07211497, 0.00630165];
        
        let fact = 2.0 / period as f64;
        c[0] = fact;

        let fact_pi = fact * PI;
        for i in 1..=half_length {
            c[i] = (i as f64 * fact_pi).sin() / (i as f64 * PI);
        }

        // Taper the end point
        c[half_length] *= 0.5;

        let mut sumg = c[0];
        for i in 1..=half_length {
            let mut sum = d[0];
            let f = i as f64 * PI / half_length as f64;
            for j in 1..=3 {
                sum += 2.0 * d[j] * (j as f64 * f).cos();
            }
            c[i] *= sum;
            sumg += 2.0 * c[i];
        }

        for val in c.iter_mut() {
            *val /= sumg;
        }

        c
    }

    /// Performs Zero-Lag projection using Least-Squares and calculates FTI.
    /// Returns the average FTI across all periods.
    pub fn calculate(&self, prices: &VecDeque<f64>) -> Option<f64> {
        if prices.len() < self.lookback + self.half_length {
            return None;
        }

        // 1. Prepare chronological data buffer with extra space for projection
        let mut y = Vec::with_capacity(self.lookback + self.half_length);
        for i in (0..self.lookback).rev() {
            y.push(prices[i]);
        }

        // 2. Least-Squares Projection (The "Kludge")
        // Fit to the most recent half_length+1 points
        let n_fit = self.half_length + 1;
        let x_mean = -0.5 * self.half_length as f64;
        let mut y_mean = 0.0;
        for i in 0..n_fit {
            y_mean += y[self.lookback - 1 - i];
        }
        y_mean /= n_fit as f64;

        let mut xsq = 0.0;
        let mut xy = 0.0;
        for i in 0..n_fit {
            let x_diff = -(i as f64) - x_mean;
            let y_diff = y[self.lookback - 1 - i] - y_mean;
            xsq += x_diff * x_diff;
            xy += x_diff * y_diff;
        }
        let slope = xy / (xsq + 1e-12);
        
        // Extend y by half_length further with projected values
        for i in 0..self.half_length {
            let projected = (i as f64 + 1.0 - x_mean) * slope + y_mean;
            y.push(projected);
        }

        // 3. Main Loop: Each period
        let mut fti_sum = 0.0;
        let mut count = 0;

        for (idx, _period) in (self.min_period..=self.max_period).enumerate() {
            let coefs = &self.coef_table[idx];
            let mut filtered_block = Vec::with_capacity(self.lookback - self.half_length);
            let mut diff_work = Vec::with_capacity(self.lookback - self.half_length);

            // Apply symmetric filter convolution
            for iy in self.half_length..self.lookback {
                let mut sum = coefs[0] * y[iy];
                for i in 1..=self.half_length {
                    sum += coefs[i] * (y[iy + i] + y[iy - i]);
                }
                filtered_block.push(sum);
                diff_work.push((y[iy] - sum).abs());
            }

            // Calculate Channel Width (Quantile)
            diff_work.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let q_idx = (self.beta * (diff_work.len() as f64)).round() as usize;
            let width = if q_idx < diff_work.len() { diff_work[q_idx] } else { *diff_work.last().unwrap_or(&0.0) };

            // Calculate Legs in Filtered Price
            let mut legs = Vec::new();
            let mut extreme_val = filtered_block[0];
            let mut extreme_type = 0; // 0=None, 1=High, -1=Low
            let mut prior = extreme_val;
            let mut longest_leg = 0.0;

            for &val in filtered_block.iter().skip(1) {
                if extreme_type == 0 {
                    if val > extreme_val { extreme_type = -1; }
                    else if val < extreme_val { extreme_type = 1; }
                } else if val == *filtered_block.last().unwrap() {
                    let leg = (extreme_val - val).abs();
                    legs.push(leg);
                    if leg > longest_leg { longest_leg = leg; }
                } else {
                    if extreme_type == 1 && val > prior {
                        let leg = (extreme_val - prior).abs();
                        legs.push(leg);
                        if leg > longest_leg { longest_leg = leg; }
                        extreme_type = -1;
                        extreme_val = prior;
                    } else if extreme_type == -1 && val < prior {
                        let leg = (prior - extreme_val).abs();
                        legs.push(leg);
                        if leg > longest_leg { longest_leg = leg; }
                        extreme_type = 1;
                        extreme_val = prior;
                    }
                }
                prior = val;
            }

            // Mean leg size normalized by noise
            let noise_level = self.noise_cut * longest_leg;
            let mut leg_sum = 0.0;
            let mut leg_n = 0;
            for &leg in &legs {
                if leg > noise_level {
                    leg_sum += leg;
                    leg_n += 1;
                }
            }

            if leg_n > 0 && width > 0.0 {
                let fti = (leg_sum / leg_n as f64) / (width + 1e-7);
                fti_sum += fti;
                count += 1;
            }
        }

        if count > 0 {
            Some(fti_sum / count as f64)
        } else {
            None
        }
    }
}
