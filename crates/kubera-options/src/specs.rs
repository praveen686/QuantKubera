//! Instrument validation rules (lot size, tick size)

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentSpec {
    pub lot_size: i64,
    pub tick_size: f64,
    /// Fixed-point quantity scale. Interprets order quantity units as qty/qty_scale base units.
    /// For options (integer lots), keep qty_scale = 1.
    /// For Binance spot, a common choice is qty_scale = 1_000_000 (micro-units).
    pub qty_scale: u32,
}

#[derive(Default)]
pub struct SpecStore {
    specs: HashMap<String, InstrumentSpec>,
}

impl SpecStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, sym: &str, lot: i64, tick: f64) {
        self.insert_with_scale(sym, lot, tick, 1);
    }

    pub fn insert_with_scale(&mut self, sym: &str, lot: i64, tick: f64, qty_scale: u32) {
        self.specs.insert(sym.to_string(), InstrumentSpec {
            lot_size: lot,
            tick_size: tick,
            qty_scale,
        });
    }

    pub fn valid_qty(&self, sym: &str, qty: i64) -> bool {
        self.specs.get(sym).map(|s| qty % s.lot_size == 0).unwrap_or(true)
    }

    pub fn valid_price(&self, sym: &str, px: f64) -> bool {
        self.specs.get(sym).map(|s| ((px / s.tick_size).round() * s.tick_size - px).abs() < 1e-6).unwrap_or(true)
    }

    pub fn qty_scale(&self, sym: &str) -> u32 {
        self.specs.get(sym).map(|s| s.qty_scale).unwrap_or(1)
    }

    /// Convert internal integer quantity into base units using the symbol's qty_scale.
    pub fn qty_to_f64(&self, sym: &str, qty: u32) -> f64 {
        let scale = self.qty_scale(sym) as f64;
        (qty as f64) / scale.max(1.0)
    }
}
