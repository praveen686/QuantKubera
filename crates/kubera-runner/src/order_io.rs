//! Order IO: load/store offline intent for KiteSim backtests.
//!
//! We keep this in the runner crate to avoid coupling the core strategy engine
//! to any specific backtest serialization format.

use serde::{Deserialize, Serialize};
use kubera_options::execution::MultiLegOrder;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFile {
    pub strategy_name: String,
    pub orders: Vec<MultiLegOrder>,
}
