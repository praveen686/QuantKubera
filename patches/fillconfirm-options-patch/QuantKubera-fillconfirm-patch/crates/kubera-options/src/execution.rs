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

    // ======= Live fill confirmation (production safety) =======
    // We do NOT assume fills in live mode. We poll the broker until the order
    // reaches a terminal state, then record the actual avg fill price.
    const KITE_BASE_URL: &'static str = "https://api.kite.trade";
    const DEFAULT_POLL_INTERVAL_MS: u64 = 200;
    const DEFAULT_POLL_TIMEOUT_MS: u64 = 15_000;

    async fn fetch_order_history(&self, order_id: &str) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}/orders/{}", Self::KITE_BASE_URL, order_id);

        let response = self
            .client
            .get(&url)
            .header("X-Kite-Version", "3")
            .header(
                "Authorization",
                format!("token {}:{}", self.api_key, self.access_token),
            )
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            anyhow::bail!("Kite order history failed: status={} body={}", status, body);
        }

        Ok(serde_json::from_str::<serde_json::Value>(&body)?)
    }

    /// Poll Kite until the order is COMPLETE / REJECTED / CANCELLED.
    /// Returns (status, avg_price, filled_qty).
    async fn poll_fill_confirmation(
        &self,
        order_id: &str,
        expected_qty: u32,
        timeout_ms: u64,
        interval_ms: u64,
    ) -> anyhow::Result<(LegStatus, Option<f64>, u32)> {
        use tokio::time::{sleep, Duration, Instant};

        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));

        loop {
            let v = self.fetch_order_history(order_id).await?;

            // Kite typically returns: { "status":"success", "data":[ ...order history... ] }
            let data = v.get("data").cloned().unwrap_or(serde_json::Value::Null);

            let last = match data {
                serde_json::Value::Array(arr) if !arr.is_empty() => arr[arr.len() - 1].clone(),
                serde_json::Value::Object(_) => data,
                _ => serde_json::Value::Null,
            };

            let status_str = last.get("status").and_then(|x| x.as_str()).unwrap_or("");
            let filled_qty = last
                .get("filled_quantity")
                .and_then(|x| x.as_u64())
                .unwrap_or(0) as u32;

            let avg_price = last
                .get("average_price")
                .and_then(|x| x.as_f64())
                .or_else(|| {
                    last.get("average_price")
                        .and_then(|x| x.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                });

            let terminal = status_str.eq_ignore_ascii_case("COMPLETE")
                || status_str.eq_ignore_ascii_case("REJECTED")
                || status_str.eq_ignore_ascii_case("CANCELLED")
                || status_str.eq_ignore_ascii_case("CANCELED");

            if terminal {
                let st = if status_str.eq_ignore_ascii_case("COMPLETE") {
                    if filled_qty >= expected_qty {
                        LegStatus::Filled
                    } else {
                        LegStatus::PartiallyFilled
                    }
                } else if status_str.eq_ignore_ascii_case("REJECTED") {
                    LegStatus::Rejected
                } else {
                    LegStatus::Cancelled
                };

                return Ok((st, avg_price, filled_qty));
            }

            if Instant::now() >= deadline {
                // Not terminal: fail-safe; caller decides whether to cancel or abort.
                return Ok((LegStatus::Placed, avg_price, filled_qty));
            }

            sleep(Duration::from_millis(interval_ms.max(50))).await;
        }
    }

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

                    // ======= LIVE FILL CONFIRMATION =======
                    // Do not assume fill. Poll broker until terminal status.
                    let mut final_status = LegStatus::Placed;
                    let mut final_fill_price: Option<f64> = None;
                    let mut final_error: Option<String> = None;

                    match self.poll_fill_confirmation(
                        &order_id,
                        leg.quantity,
                        Self::DEFAULT_POLL_TIMEOUT_MS,
                        Self::DEFAULT_POLL_INTERVAL_MS,
                    ).await {
                        Ok((st, avg_price, filled_qty)) => {
                            final_status = st;
                            final_fill_price = avg_price;
                            if !matches!(st, LegStatus::Filled) {
                                final_error = Some(format!("Order not fully filled: status={:?} filled_qty={} expected_qty={}", st, filled_qty, leg.quantity));
                            }
                        }
                        Err(e) => {
                            final_error = Some(format!("Fill confirmation failed: {}", e));
                        }
                    }

                    if final_error.is_some() {
                        all_success = false;
                        warn!(order_id = %order_id, "Leg not fully filled; cancelling and rolling back");
                        // Best-effort cancel
                        let _ = self.cancel_order(&order_id).await;
                        // Rollback previous successful legs
                        for prev in &leg_results {
                            if let Some(prev_id) = &prev.order_id {
                                let _ = self.cancel_order(prev_id).await;
                            }
                        }
                    }

                    leg_results.push(LegExecutionResult {
                        tradingsymbol: leg.tradingsymbol.clone(),
                        order_id: Some(order_id),
                        status: final_status,
                        fill_price: final_fill_price,
                        error: final_error,
                    });
                }
