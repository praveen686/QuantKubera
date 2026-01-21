//! # AEON Statistical Kernels
//! Kernels for information-theoretic market analysis.
//! Ported and optimized from Timothy Masters' methodology.

use std::collections::VecDeque;

/// Shannon Entropy calculator for detecting market randomness/efficiency.
#[derive(Debug, Clone)]
pub struct ShannonEntropy {
    word_len: usize,
    nbins: usize,
}

impl ShannonEntropy {
    pub fn new(word_len: usize) -> Self {
        let nbins = 2_usize.pow(word_len as u32);
        Self { word_len, nbins }
    }

    /// Calculates normalized entropy (0.0 to 1.0).
    /// 1.0 = Pure noise / Efficiency.
    /// 0.0 = Pure structure / Predictability.
    pub fn calculate(&self, prices: &VecDeque<f64>) -> f64 {
        if prices.len() <= self.word_len {
            return 1.0;
        }

        let mut bins = vec![0; self.nbins];
        let n = prices.len() - self.word_len;

        for i in self.word_len..prices.len() {
            let mut k = 0;
            for j in 0..self.word_len {
                // Form a binary word based on price direction
                // Masters logic: x[i-j-1] > x[i-j]
                if prices[i - j - 1] > prices[i - j] {
                    k |= 1 << j;
                }
            }
            if k < self.nbins {
                bins[k] += 1;
            }
        }

        let mut ent = 0.0;
        let n_f = n as f64;
        for &count in &bins {
            if count > 0 {
                let p = count as f64 / n_f;
                ent -= p * p.ln();
            }
        }

        // Normalize by log(nbins) to keep it between 0 and 1
        ent / (self.nbins as f64).ln()
    }
}

/// Mutual Information calculator for measuring predictive dependency.
#[derive(Debug, Clone)]
pub struct MutualInformation {
    word_len: usize,
    nbins: usize, 
}

impl MutualInformation {
    pub fn new(word_len: usize) -> Self {
        // nb = 2^(wordlen + 1)
        let nbins = 2_usize.pow((word_len + 1) as u32);
        Self { word_len, nbins }
    }

    pub fn calculate(&self, prices: &VecDeque<f64>) -> f64 {
        if prices.len() <= self.word_len + 1 {
            return 0.0;
        }

        let mut bins = vec![0; self.nbins];
        let mut dep_marg = [0.0; 2];
        let n = prices.len() - self.word_len - 1;
        let m = self.nbins / 2; // History categories

        for i in 0..n {
            // Current value (dependent variable)
            let current_k = if prices[i] > prices[i+1] { 1 } else { 0 };
            dep_marg[current_k] += 1.0;

            let mut k = current_k;
            for j in 1..=self.word_len {
                k <<= 1;
                if prices[i + j] > prices[i + j + 1] {
                    k |= 1;
                }
            }
            if k < self.nbins {
                bins[k] += 1;
            }
        }

        let n_f = n as f64;
        dep_marg[0] /= n_f;
        dep_marg[1] /= n_f;

        let mut mi = 0.0;
        for i in 0..m {
            // History marginal
            let hist_marg = (bins[i] + bins[i + m]) as f64 / n_f;
            if hist_marg <= 0.0 { continue; }

            // p for current=0
            let p0 = bins[i] as f64 / n_f;
            if p0 > 0.0 && dep_marg[0] > 0.0 {
                mi += p0 * (p0 / (hist_marg * dep_marg[0])).ln();
            }

            // p for current=1
            let p1 = bins[i + m] as f64 / n_f;
            if p1 > 0.0 && dep_marg[1] > 0.0 {
                mi += p1 * (p1 / (hist_marg * dep_marg[1])).ln();
            }
        }

        mi
    }
}
