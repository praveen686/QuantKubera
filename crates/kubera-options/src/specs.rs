
//! Instrument validation rules (lot size, tick size)

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct InstrumentSpec {
    pub lot_size: i64,
    pub tick_size: f64,
}

#[derive(Default)]
pub struct SpecStore {
    specs: HashMap<String, InstrumentSpec>,
}

impl SpecStore {
    pub fn insert(&mut self, sym: &str, lot: i64, tick: f64) {
        self.specs.insert(sym.to_string(), InstrumentSpec{lot_size: lot, tick_size: tick});
    }

    pub fn valid_qty(&self, sym: &str, qty: i64) -> bool {
        self.specs.get(sym).map(|s| qty % s.lot_size == 0).unwrap_or(true)
    }

    pub fn valid_price(&self, sym: &str, px: f64) -> bool {
        self.specs.get(sym).map(|s| ((px / s.tick_size).round() * s.tick_size - px).abs() < 1e-6).unwrap_or(true)
    }
}
