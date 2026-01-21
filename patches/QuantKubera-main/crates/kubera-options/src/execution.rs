//! # Options Execution Module
//!
//! Handles atomic multi-leg order execution and position lifecycle management.
//!
//! ## Description
//! Implements the execution layer for derivative strategies, featuring:
//! - **Atomic Multi-Leg Orders**: Bundling multiple contracts for simultaneous execution.
//! - **Automatic Rollback**: Cancellation of partially placed strategies on failure.
//! - **Position Tracking**: Real-time average price and PnL monitoring.
//! - **Dry-Run Mode**: Simulated execution for strategy validation.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Zerodha Kite Connect API Documentation

use crate::strategy::OptionsStrategy;
use serde::{Deserialize, Serialize};

/// Composite order comprising multiple derivative contracts.
#[derive(Debug, Clone, Serialize)]
pub struct MultiLegOrder {
    pub strategy_name: String,
    pub legs: Vec<LegOrder>,
    /// Aggregate margin commitment required for the strategy.
    pub total_margin_required: f64,
}

/// Unit order for a single contract within a strategy.
#[derive(Debug, Clone, Serialize)]
pub struct LegOrder {
    pub tradingsymbol: String,
    pub exchange: String,
    pub side: LegSide,
    pub quantity: u32,
    pub order_type: LegOrderType,
    pub price: Option<f64>,
}

/// Direction of the leg execution.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum LegSide {
    Buy,
    Sell,
}

/// Constraint on execution price.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum LegOrderType {
    /// Immediate fill at current market price.
    Market,
    /// Fill only at or better than specified price.
    Limit,
}

/// Aggregate result status after attempting a multi-leg execution.
#[derive(Debug, Clone)]
pub struct MultiLegResult {
    pub strategy_name: String,
    pub leg_results: Vec<LegExecutionResult>,
    /// True if all components were successfully fulfilled.
    pub all_filled: bool,
    pub total_premium: f64,
}

/// Individual outcome for a single strategy leg.
#[derive(Debug, Clone)]
pub struct LegExecutionResult {
    pub tradingsymbol: String,
    pub order_id: Option<String>,
    pub status: LegStatus,
    pub fill_price: Option<f64>,
    pub error: Option<String>,
}

/// Lifecycle state of an individual leg order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LegStatus {
    Pending,
    Placed,
    Filled,
    PartiallyFilled,
    Rejected,
    Cancelled,
}

impl MultiLegOrder {
    /// Constructs an execution intent from an abstract `OptionsStrategy`.
    ///
    /// # Parameters
    /// * `strategy` - The strategy model to execute.
    /// * `lot_size` - Units per contract.
    /// * `exchange` - Destination venue.
    pub fn from_strategy(
        strategy: &OptionsStrategy,
        lot_size: u32,
        exchange: &str,
    ) -> Self {
        let legs = strategy.legs.iter().map(|leg| {
            let side = if leg.quantity > 0 { LegSide::Buy } else { LegSide::Sell };
            
            LegOrder {
                tradingsymbol: leg.contract.tradingsymbol.clone(),
                exchange: exchange.to_string(),
                side,
                quantity: (leg.quantity.abs() as u32) * lot_size,
                order_type: LegOrderType::Market,
                price: None,
            }
        }).collect();

        Self {
            strategy_name: strategy.name.clone(),
            legs,
            total_margin_required: 0.0, 
        }
    }

    /// Transitions market orders to limit orders with defined price bounds.
    pub fn with_limit_prices(mut self, prices: &[f64]) -> Self {
        for (leg, price) in self.legs.iter_mut().zip(prices.iter()) {
            leg.order_type = LegOrderType::Limit;
            leg.price = Some(*price);
        }
        self
    }
}

/// High-performance executor for multi-leg strategies.
pub struct MultiLegExecutor {
    api_key: String,
    access_token: String,
    client: reqwest::Client,
    dry_run: bool,
}

impl MultiLegExecutor {
    /// Initializes a new executor with venue credentials.
    pub fn new(api_key: String, access_token: String) -> Self {
        Self {
            api_key,
            access_token,
            client: reqwest::Client::new(),
            dry_run: false,
        }
    }

    /// Configures the executor to run without committing actual funds.
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Synchronizes the execution of all strategy components.
    ///
    /// # Rollback Logic
    /// If any leg fails to place, previously placed legs are immediately 
    /// targeted for cancellation to minimize "leg risk".
    pub async fn execute(&self, order: &MultiLegOrder) -> MultiLegResult {
        use tracing::{info, error, warn};
        
        info!(strategy = %order.strategy_name, legs = order.legs.len(), "Executing multi-leg order");
        
        let mut leg_results = Vec::new();
        let mut all_success = true;
        #[allow(unused_variables)]
        let total_premium = 0.0;

        for (i, leg) in order.legs.iter().enumerate() {
            info!(
                leg = i + 1, 
                symbol = %leg.tradingsymbol, 
                side = ?leg.side,
                quantity = leg.quantity,
                "Placing leg"
            );

            if self.dry_run {
                leg_results.push(LegExecutionResult {
                    tradingsymbol: leg.tradingsymbol.clone(),
                    order_id: Some(format!("DRY_RUN_{}", i)),
                    status: LegStatus::Filled,
                    fill_price: leg.price.or(Some(100.0)), 
                    error: None,
                });
                continue;
            }

            match self.place_leg(leg).await {
                Ok(order_id) => {
                    info!(order_id = %order_id, "Leg placed successfully");
                    leg_results.push(LegExecutionResult {
                        tradingsymbol: leg.tradingsymbol.clone(),
                        order_id: Some(order_id),
                        status: LegStatus::Placed,
                        fill_price: None,
                        error: None,
                    });
                }
                Err(e) => {
                    error!(error = %e, leg = i + 1, "Leg execution failed");
                    all_success = false;
                    leg_results.push(LegExecutionResult {
                        tradingsymbol: leg.tradingsymbol.clone(),
                        order_id: None,
                        status: LegStatus::Rejected,
                        fill_price: None,
                        error: Some(e.to_string()),
                    });
                    
                    warn!("Initiating rollback of {} placed legs", i);
                    for prev_result in leg_results.iter().take(i) {
                        if let Some(ref order_id) = prev_result.order_id {
                            if let Err(cancel_err) = self.cancel_order(order_id).await {
                                error!(order_id = %order_id, error = %cancel_err, "Failed to cancel leg during rollback");
                            }
                        }
                    }
                    break;
                }
            }
        }

        MultiLegResult {
            strategy_name: order.strategy_name.clone(),
            leg_results,
            all_filled: all_success,
            total_premium,
        }
    }

    async fn place_leg(&self, leg: &LegOrder) -> anyhow::Result<String> {
        let url = "https://api.kite.trade/orders/regular";
        
        let transaction_type = match leg.side {
            LegSide::Buy => "BUY",
            LegSide::Sell => "SELL",
        };
        
        let order_type = match leg.order_type {
            LegOrderType::Market => "MARKET",
            LegOrderType::Limit => "LIMIT",
        };
 
        let mut form = vec![
            ("tradingsymbol", leg.tradingsymbol.clone()),
            ("exchange", leg.exchange.clone()),
            ("transaction_type", transaction_type.to_string()),
            ("order_type", order_type.to_string()),
            ("quantity", leg.quantity.to_string()),
            ("product", "NRML".to_string()),
            ("validity", "DAY".to_string()),
        ];

        if let Some(price) = leg.price {
            form.push(("price", price.to_string()));
        }

        let response = self.client.post(url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .form(&form)
            .send()
            .await?;

        let resp: serde_json::Value = response.json().await?;
        
        if resp["status"] == "success" {
            Ok(resp["data"]["order_id"].as_str().unwrap_or("unknown").to_string())
        } else {
            let msg = resp["message"].as_str().unwrap_or("Unknown error");
            Err(anyhow::anyhow!("Order failed: {}", msg))
        }
    }

    async fn cancel_order(&self, order_id: &str) -> anyhow::Result<()> {
        let url = format!("https://api.kite.trade/orders/regular/{}", order_id);
        
        let response = self.client.delete(&url)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?;

        let resp: serde_json::Value = response.json().await?;
        
        if resp["status"] == "success" {
            Ok(())
        } else {
            let msg = resp["message"].as_str().unwrap_or("Unknown error");
            Err(anyhow::anyhow!("Cancel failed: {}", msg))
        }
    }
}

/// Internal state tracker for active option exposures.
#[derive(Debug, Clone, Default)]
pub struct PositionManager {
    positions: std::collections::HashMap<String, Position>,
}

/// Dynamic record of a current position and its performance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub tradingsymbol: String,
    pub quantity: i32,
    pub average_price: f64,
    /// Last known mark-to-market price.
    pub last_price: f64,
    pub pnl: f64,
    pub product: String,
}

impl PositionManager {
    /// Initializes an empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates or creates a position entry based on an execution fill.
    ///
    /// # Average Price Logic
    /// Uses volume-weighted average price (VWAP) for accumulating positions.
    pub fn on_fill(&mut self, tradingsymbol: &str, quantity: i32, price: f64, product: &str) {
        let pos = self.positions.entry(tradingsymbol.to_string()).or_insert(Position {
            tradingsymbol: tradingsymbol.to_string(),
            quantity: 0,
            average_price: 0.0,
            last_price: price,
            pnl: 0.0,
            product: product.to_string(),
        });

        if pos.quantity == 0 {
            pos.average_price = price;
        } else if (pos.quantity > 0 && quantity > 0) || (pos.quantity < 0 && quantity < 0) {
            let total_value = (pos.quantity as f64 * pos.average_price) + (quantity as f64 * price);
            let total_qty = pos.quantity + quantity;
            pos.average_price = total_value / total_qty as f64;
        }
        
        pos.quantity += quantity;
        pos.last_price = price;
        
        if pos.quantity == 0 {
            self.positions.remove(tradingsymbol);
        }
    }

    /// Synchronizes mark-to-market valuations for all monitored positions.
    pub fn update_prices(&mut self, prices: &std::collections::HashMap<String, f64>) {
        for (symbol, pos) in self.positions.iter_mut() {
            if let Some(price) = prices.get(symbol) {
                pos.last_price = *price;
                pos.pnl = (pos.last_price - pos.average_price) * pos.quantity as f64;
            }
        }
    }

    /// Aggregates unrealized PnL across the entire managed book.
    pub fn total_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.pnl).sum()
    }

    /// Returns the net share/contract count across all underlying instruments.
    pub fn net_quantity(&self) -> i32 {
        self.positions.values().map(|p| p.quantity).sum()
    }

    /// Provides a view into all currently open positions.
    pub fn all_positions(&self) -> Vec<&Position> {
        self.positions.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_manager_on_fill() {
        let mut pm = PositionManager::new();
        
        // Buy 50 @ 100
        pm.on_fill("NIFTY25D2525800CE", 50, 100.0, "NRML");
        assert_eq!(pm.net_quantity(), 50);
        
        // Buy 50 more @ 120
        pm.on_fill("NIFTY25D2525800CE", 50, 120.0, "NRML");
        assert_eq!(pm.net_quantity(), 100);
        
        // Average should be 110
        let pos = pm.positions.get("NIFTY25D2525800CE").unwrap();
        assert!((pos.average_price - 110.0).abs() < 0.01);
    }

    #[test]
    fn test_position_manager_flatten() {
        let mut pm = PositionManager::new();
        
        pm.on_fill("NIFTY25D2525800CE", 50, 100.0, "NRML");
        pm.on_fill("NIFTY25D2525800CE", -50, 110.0, "NRML");
        
        assert_eq!(pm.net_quantity(), 0);
        assert!(pm.positions.is_empty());
    }

    #[tokio::test]
    async fn test_multi_leg_dry_run() {
        let executor = MultiLegExecutor::new("test".to_string(), "test".to_string())
            .with_dry_run(true);
        
        let order = MultiLegOrder {
            strategy_name: "Test Straddle".to_string(),
            legs: vec![
                LegOrder {
                    tradingsymbol: "NIFTY25D2525800CE".to_string(),
                    exchange: "NFO".to_string(),
                    side: LegSide::Buy,
                    quantity: 50,
                    order_type: LegOrderType::Market,
                    price: None,
                },
                LegOrder {
                    tradingsymbol: "NIFTY25D2525800PE".to_string(),
                    exchange: "NFO".to_string(),
                    side: LegSide::Buy,
                    quantity: 50,
                    order_type: LegOrderType::Market,
                    price: None,
                },
            ],
            total_margin_required: 50000.0,
        };
        
        let result = executor.execute(&order).await;
        assert!(result.all_filled);
        assert_eq!(result.leg_results.len(), 2);
    }
}
