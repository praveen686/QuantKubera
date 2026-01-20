pub mod kernels;
pub mod fti;
pub mod strategy;

pub use self::strategy::AeonStrategy;

use std::collections::VecDeque;
use self::kernels::{ShannonEntropy, MutualInformation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AeonConfig {
    pub entropy_word_len: usize,
    pub mi_word_len: usize,
    pub lookback: usize,
    
    // FTI Config
    pub fti_min_period: usize,
    pub fti_max_period: usize,
    pub fti_half_length: usize,
    pub fti_beta: f64,
    pub fti_noise_cut: f64,
    
    // Gating thresholds
    pub bif_predictability_threshold: f64, // e.g. 0.4
    pub fti_trend_threshold: f64,          // e.g. 2.0
    pub fti_mean_rev_threshold: f64,       // e.g. 0.8
}

impl Default for AeonConfig {
    fn default() -> Self {
        Self {
            entropy_word_len: 4,
            mi_word_len: 4,
            lookback: 64,
            fti_min_period: 5,
            fti_max_period: 65,
            fti_half_length: 32,
            fti_beta: 0.9,
            fti_noise_cut: 0.2,
            bif_predictability_threshold: 0.35,
            fti_trend_threshold: 1.8,
            fti_mean_rev_threshold: 0.9,
        }
    }
}

pub struct BifIndicator {
    entropy: ShannonEntropy,
    mi: MutualInformation,
}

impl BifIndicator {
    pub fn new(config: &AeonConfig) -> Self {
        Self {
            entropy: ShannonEntropy::new(config.entropy_word_len),
            mi: MutualInformation::new(config.mi_word_len),
        }
    }

    /// Calculates the Bayesian Information Flux.
    /// Returns 0.0 to 1.0 (Higher = more structure/predictability).
    pub fn calculate(&self, prices: &VecDeque<f64>) -> f64 {
        let ent = self.entropy.calculate(prices);
        let mi = self.mi.calculate(prices);
        
        // Predictability signal: High MI relative to Entropy
        // Normalizing: (MI / log(nbins_mi)) / Entropy
        // For simplicity: (MI / (ent + 0.1)) captured as a score.
        let score = mi / (ent + 0.05);
        
        // Clamp and normalize to 0-1 range
        score.clamp(0.0, 1.0)
    }
}
