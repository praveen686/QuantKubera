//! # Project AEON: Adaptive Entropy-Optimized Network
//! 
//! AEON is a hierarchical flagship strategy that fuses information theory (BIF),
//! structural analysis (FTI), and online meta-allocation (HYDRA).

use std::sync::Arc;
use std::collections::VecDeque;
use tracing::{info, debug};
use kubera_models::{MarketEvent, MarketPayload, Side, SignalEvent};

use crate::{Strategy, EventBus};
use crate::aeon::{AeonConfig, BifIndicator};
use crate::aeon::fti::FtiEngine;
use crate::hydra::{HydraStrategy, HydraConfig}; // Reuse HYDRA's expert management

pub struct AeonStrategy {
    name: String,
    bus: Option<Arc<EventBus>>,
    config: AeonConfig,
    
    // Core Engines
    bif_engine: BifIndicator,
    fti_engine: FtiEngine,
    
    // Internal state
    prices: VecDeque<f64>,
    position: f64,
    
    // Expert Management (Reusing HYDRA's ensemble)
    hydra: HydraStrategy, 
}

impl AeonStrategy {
    pub fn new(config: AeonConfig, hydra_config: HydraConfig) -> Self {
        let bif_engine = BifIndicator::new(&config);
        let fti_engine = FtiEngine::new(
            config.fti_min_period,
            config.fti_max_period,
            config.fti_half_length,
            config.lookback,
            config.fti_beta,
            config.fti_noise_cut,
        );

        Self {
            name: "AEON-v1".to_string(),
            bus: None,
            bif_engine,
            fti_engine,
            prices: VecDeque::with_capacity(config.lookback + config.fti_half_length + 1),
            position: 0.0,
            hydra: HydraStrategy::with_config(hydra_config),
            config,
        }
    }

    fn emit_signal(&mut self, timestamp: i64, symbol: &str, side: Side, price: f64) {
        if let Some(bus) = &self.bus {
            let signal = SignalEvent {
                timestamp: chrono::DateTime::from_timestamp(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000)
                    .unwrap_or_else(|| chrono::Utc::now()),
                strategy_id: self.name.clone(),
                symbol: symbol.to_string(),
                side,
                price,
                quantity: 1.0, 
                intent_id: None,
            };
            let _ = bus.publish_signal_sync(signal);
        }
    }
}

impl Strategy for AeonStrategy {
    fn on_start(&mut self, bus: Arc<EventBus>) {
        info!("[AEON] Sentry Online. Gating threshold: {}", self.config.bif_predictability_threshold);
        self.bus = Some(bus.clone());
        self.hydra.on_start(bus);
    }

    fn on_tick(&mut self, event: &MarketEvent) {
        let price = match &event.payload {
            MarketPayload::Tick { price, .. } => *price,
            MarketPayload::Trade { price, .. } => *price,
            _ => {
                // Pass-through other events (like L2) to HYDRA without gating
                self.hydra.on_tick(event);
                return;
            }
        };

        self.prices.push_back(price);
        if self.prices.len() > self.config.lookback + self.config.fti_half_length {
            self.prices.pop_front();
        }

        if self.prices.len() < self.config.lookback {
            return;
        }

        // --- LEVEL 1: INFORMATIONAL SENTRY (BIF) ---
        let predictability = self.bif_engine.calculate(&self.prices);
        if predictability < self.config.bif_predictability_threshold {
            debug!("[AEON] Market High Entropy ({:.3}). Sentry active - idling.", predictability);
            return;
        }

        // --- LEVEL 2: STRUCTURAL FILTER (FTI) ---
        let fti_score = match self.fti_engine.calculate(&self.prices) {
            Some(s) => s,
            None => return,
        };

        // --- LEVEL 3: DYNAMIC EXPERT ORCHESTRATION ---
        // Propagate predictability to HYDRA for entropy-adjusted scaling
        self.hydra.set_aeon_bif_score(predictability);
        
        // Update HYDRA experts
        self.hydra.on_tick(event);

        // Gating Logic based on FTI
        let is_trend = fti_score > self.config.fti_trend_threshold;
        let is_mean_rev = fti_score < self.config.fti_mean_rev_threshold;

        debug!("[AEON] Predictability: {:.3} | FTI: {:.2} | Trend: {} | MeanRev: {}", 
            predictability, fti_score, is_trend, is_mean_rev);

        if !is_trend && !is_mean_rev {
            debug!("[AEON] Structural Noise. Skipping.");
            return;
        }
    }

    fn on_bar(&mut self, event: &MarketEvent) {
        self.hydra.on_bar(event);
    }

    fn on_fill(&mut self, fill: &kubera_models::OrderEvent) {
        self.hydra.on_fill(fill);
    }

    fn on_stop(&mut self) {
        info!("[AEON] Shutting down. Final Predictability State: Active");
        self.hydra.on_stop();
    }

    fn name(&self) -> &str {
        &self.name
    }
}
