//! # Circuit Breakers & Rate Limiters
//!
//! Production-grade safety mechanisms for trading system protection.
//!
//! ## Description
//! This module implements multiple layers of protection:
//! - Rate limiting for signals and orders
//! - Latency-based circuit breakers
//! - Drawdown-based emergency stops
//! - Order flow anomaly detection
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{warn, error, info};

/// Rate limiter using token bucket algorithm
pub struct RateLimiter {
    /// Maximum tokens (burst capacity)
    max_tokens: u32,
    /// Current available tokens
    tokens: f64,
    /// Refill rate (tokens per second)
    refill_rate: f64,
    /// Last refill timestamp
    last_refill: Instant,
    /// Name for logging
    name: String,
}

impl RateLimiter {
    pub fn new(name: &str, max_per_second: f64, burst_capacity: u32) -> Self {
        Self {
            max_tokens: burst_capacity,
            tokens: burst_capacity as f64,
            refill_rate: max_per_second,
            last_refill: Instant::now(),
            name: name.to_string(),
        }
    }

    /// Try to consume one token. Returns true if allowed.
    pub fn try_acquire(&mut self) -> bool {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            warn!("[RATE_LIMIT] {} exceeded - request rejected", self.name);
            false
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        let new_tokens = elapsed * self.refill_rate;
        self.tokens = (self.tokens + new_tokens).min(self.max_tokens as f64);
        self.last_refill = Instant::now();
    }

    /// Get current token count
    pub fn available(&self) -> f64 {
        self.tokens
    }
}

/// Latency monitor with circuit breaker behavior
pub struct LatencyCircuitBreaker {
    /// Recent latency samples (in milliseconds)
    samples: VecDeque<f64>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Threshold for triggering (percentile latency)
    threshold_ms: f64,
    /// Percentile to use (e.g., 0.99 for p99)
    percentile: f64,
    /// Is breaker currently tripped?
    is_tripped: AtomicBool,
    /// Cool-down period after tripping
    cooldown: Duration,
    /// Time when breaker was tripped
    tripped_at: Option<Instant>,
    /// Name for logging
    name: String,
}

impl LatencyCircuitBreaker {
    pub fn new(name: &str, threshold_ms: f64, percentile: f64, cooldown_secs: u64) -> Self {
        Self {
            samples: VecDeque::with_capacity(1000),
            max_samples: 1000,
            threshold_ms,
            percentile,
            is_tripped: AtomicBool::new(false),
            cooldown: Duration::from_secs(cooldown_secs),
            tripped_at: None,
            name: name.to_string(),
        }
    }

    /// Record a latency sample and check threshold
    pub fn record(&mut self, latency_ms: f64) -> bool {
        self.samples.push_back(latency_ms);
        if self.samples.len() > self.max_samples {
            self.samples.pop_front();
        }

        // Check if we should auto-reset after cooldown
        if let Some(tripped_at) = self.tripped_at {
            if tripped_at.elapsed() > self.cooldown {
                self.reset();
            }
        }

        if self.is_tripped.load(Ordering::Relaxed) {
            return false;
        }

        // Calculate percentile latency
        if self.samples.len() >= 100 {
            let percentile_latency = self.calculate_percentile();
            if percentile_latency > self.threshold_ms {
                self.trip(format!(
                    "p{:.0} latency {:.1}ms exceeds threshold {:.1}ms",
                    self.percentile * 100.0, percentile_latency, self.threshold_ms
                ));
                return false;
            }
        }

        true
    }

    fn calculate_percentile(&self) -> f64 {
        let mut sorted: Vec<f64> = self.samples.iter().cloned().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let index = ((sorted.len() as f64 * self.percentile) as usize).min(sorted.len() - 1);
        sorted[index]
    }

    fn trip(&mut self, reason: String) {
        self.is_tripped.store(true, Ordering::SeqCst);
        self.tripped_at = Some(Instant::now());
        error!("[CIRCUIT_BREAKER] {} TRIPPED: {}", self.name, reason);
    }

    pub fn reset(&mut self) {
        self.is_tripped.store(false, Ordering::SeqCst);
        self.tripped_at = None;
        info!("[CIRCUIT_BREAKER] {} RESET", self.name);
    }

    pub fn is_tripped(&self) -> bool {
        self.is_tripped.load(Ordering::Relaxed)
    }

    /// Get current p99 latency
    pub fn current_percentile_latency(&self) -> Option<f64> {
        if self.samples.len() >= 10 {
            Some(self.calculate_percentile())
        } else {
            None
        }
    }
}

/// Order flow anomaly detector
pub struct OrderFlowMonitor {
    /// Recent order timestamps
    order_times: VecDeque<Instant>,
    /// Window size for monitoring
    window: Duration,
    /// Maximum orders per window
    max_orders: u32,
    /// Signal timestamps
    signal_times: VecDeque<Instant>,
    /// Maximum signals per window
    max_signals: u32,
    /// Is tripped?
    is_tripped: bool,
    /// Trip reason
    trip_reason: Option<String>,
}

impl OrderFlowMonitor {
    pub fn new(window_secs: u64, max_orders: u32, max_signals: u32) -> Self {
        Self {
            order_times: VecDeque::with_capacity(max_orders as usize * 2),
            window: Duration::from_secs(window_secs),
            max_orders,
            signal_times: VecDeque::with_capacity(max_signals as usize * 2),
            max_signals,
            is_tripped: false,
            trip_reason: None,
        }
    }

    /// Record an order and check limits
    pub fn record_order(&mut self) -> bool {
        self.prune_old();

        if self.is_tripped {
            return false;
        }

        self.order_times.push_back(Instant::now());

        if self.order_times.len() > self.max_orders as usize {
            self.is_tripped = true;
            self.trip_reason = Some(format!(
                "Order rate exceeded: {} orders in {:?} window",
                self.order_times.len(), self.window
            ));
            error!("[ORDER_FLOW] {}", self.trip_reason.as_ref().unwrap());
            return false;
        }

        true
    }

    /// Record a signal and check limits
    pub fn record_signal(&mut self) -> bool {
        self.prune_old();

        if self.is_tripped {
            return false;
        }

        self.signal_times.push_back(Instant::now());

        if self.signal_times.len() > self.max_signals as usize {
            self.is_tripped = true;
            self.trip_reason = Some(format!(
                "Signal rate exceeded: {} signals in {:?} window",
                self.signal_times.len(), self.window
            ));
            error!("[ORDER_FLOW] {}", self.trip_reason.as_ref().unwrap());
            return false;
        }

        true
    }

    fn prune_old(&mut self) {
        let cutoff = Instant::now() - self.window;

        while let Some(front) = self.order_times.front() {
            if *front < cutoff {
                self.order_times.pop_front();
            } else {
                break;
            }
        }

        while let Some(front) = self.signal_times.front() {
            if *front < cutoff {
                self.signal_times.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn reset(&mut self) {
        self.is_tripped = false;
        self.trip_reason = None;
        self.order_times.clear();
        self.signal_times.clear();
        info!("[ORDER_FLOW] Monitor reset");
    }

    pub fn is_tripped(&self) -> bool {
        self.is_tripped
    }
}

/// Drawdown-based circuit breaker
pub struct DrawdownCircuitBreaker {
    /// Peak equity observed
    peak_equity: f64,
    /// Maximum allowed drawdown percentage
    max_drawdown_pct: f64,
    /// Warning threshold (percentage of max)
    warning_threshold: f64,
    /// Is breaker tripped?
    is_tripped: bool,
    /// Has warning been issued?
    warned: bool,
}

impl DrawdownCircuitBreaker {
    pub fn new(initial_equity: f64, max_drawdown_pct: f64) -> Self {
        Self {
            peak_equity: initial_equity,
            max_drawdown_pct,
            warning_threshold: 0.7, // Warn at 70% of max drawdown
            is_tripped: false,
            warned: false,
        }
    }

    /// Update with current equity and check thresholds
    pub fn update(&mut self, current_equity: f64) -> bool {
        if current_equity > self.peak_equity {
            self.peak_equity = current_equity;
            self.warned = false; // Reset warning on new high
        }

        if self.is_tripped {
            return false;
        }

        let drawdown_pct = if self.peak_equity > 0.0 {
            (self.peak_equity - current_equity) / self.peak_equity * 100.0
        } else {
            0.0
        };

        // Check warning threshold
        if !self.warned && drawdown_pct >= self.max_drawdown_pct * self.warning_threshold {
            warn!(
                "[DRAWDOWN] Warning: {:.2}% drawdown approaching limit of {:.2}%",
                drawdown_pct, self.max_drawdown_pct
            );
            self.warned = true;
        }

        // Check trip threshold
        if drawdown_pct >= self.max_drawdown_pct {
            self.is_tripped = true;
            error!(
                "[DRAWDOWN] CIRCUIT BREAKER TRIPPED: {:.2}% drawdown exceeds {:.2}% limit",
                drawdown_pct, self.max_drawdown_pct
            );
            return false;
        }

        true
    }

    pub fn reset(&mut self, current_equity: f64) {
        self.peak_equity = current_equity;
        self.is_tripped = false;
        self.warned = false;
        info!("[DRAWDOWN] Circuit breaker reset");
    }

    pub fn is_tripped(&self) -> bool {
        self.is_tripped
    }

    pub fn current_drawdown_pct(&self, current_equity: f64) -> f64 {
        if self.peak_equity > 0.0 {
            (self.peak_equity - current_equity) / self.peak_equity * 100.0
        } else {
            0.0
        }
    }
}

/// Composite circuit breaker managing all safety mechanisms
pub struct TradingCircuitBreakers {
    /// Signal rate limiter
    pub signal_limiter: RateLimiter,
    /// Order rate limiter
    pub order_limiter: RateLimiter,
    /// Latency monitor
    pub latency_breaker: LatencyCircuitBreaker,
    /// Order flow monitor
    pub order_flow: OrderFlowMonitor,
    /// Drawdown breaker
    pub drawdown_breaker: DrawdownCircuitBreaker,
    /// Global kill switch
    kill_switch: Arc<AtomicBool>,
    /// Trip count for telemetry
    trip_count: AtomicU64,
}

impl TradingCircuitBreakers {
    pub fn new(initial_equity: f64, kill_switch: Arc<AtomicBool>) -> Self {
        Self {
            // Allow 10 signals per second with burst of 20
            signal_limiter: RateLimiter::new("Signals", 10.0, 20),
            // Allow 5 orders per second with burst of 10
            order_limiter: RateLimiter::new("Orders", 5.0, 10),
            // Trip if p99 latency exceeds 100ms, 30 second cooldown
            latency_breaker: LatencyCircuitBreaker::new("Latency", 100.0, 0.99, 30),
            // Max 100 orders and 200 signals per minute
            order_flow: OrderFlowMonitor::new(60, 100, 200),
            // Max 10% drawdown
            drawdown_breaker: DrawdownCircuitBreaker::new(initial_equity, 10.0),
            kill_switch,
            trip_count: AtomicU64::new(0),
        }
    }

    /// Configuration for Indian F&O markets
    pub fn for_indian_fno(initial_equity: f64, kill_switch: Arc<AtomicBool>) -> Self {
        Self {
            // Lower rates for Indian market (lower liquidity)
            signal_limiter: RateLimiter::new("Signals", 5.0, 10),
            order_limiter: RateLimiter::new("Orders", 2.0, 5),
            // Higher latency tolerance for Indian infrastructure
            latency_breaker: LatencyCircuitBreaker::new("Latency", 200.0, 0.99, 30),
            // Lower order limits (NSE rate limits)
            order_flow: OrderFlowMonitor::new(60, 50, 100),
            // Tighter drawdown for options (theta risk)
            drawdown_breaker: DrawdownCircuitBreaker::new(initial_equity, 8.0),
            kill_switch,
            trip_count: AtomicU64::new(0),
        }
    }

    /// Check if signal is allowed
    pub fn allow_signal(&mut self) -> bool {
        if self.kill_switch.load(Ordering::Relaxed) {
            return false;
        }

        if self.latency_breaker.is_tripped() || self.order_flow.is_tripped() || self.drawdown_breaker.is_tripped() {
            self.trip_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let rate_ok = self.signal_limiter.try_acquire();
        let flow_ok = self.order_flow.record_signal();

        rate_ok && flow_ok
    }

    /// Check if order is allowed
    pub fn allow_order(&mut self) -> bool {
        if self.kill_switch.load(Ordering::Relaxed) {
            return false;
        }

        if self.latency_breaker.is_tripped() || self.order_flow.is_tripped() || self.drawdown_breaker.is_tripped() {
            self.trip_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let rate_ok = self.order_limiter.try_acquire();
        let flow_ok = self.order_flow.record_order();

        rate_ok && flow_ok
    }

    /// Record latency sample
    pub fn record_latency(&mut self, latency_ms: f64) {
        self.latency_breaker.record(latency_ms);
    }

    /// Update equity for drawdown monitoring
    pub fn update_equity(&mut self, equity: f64) {
        self.drawdown_breaker.update(equity);
    }

    /// Check if any breaker is tripped
    pub fn is_any_tripped(&self) -> bool {
        self.kill_switch.load(Ordering::Relaxed)
            || self.latency_breaker.is_tripped()
            || self.order_flow.is_tripped()
            || self.drawdown_breaker.is_tripped()
    }

    /// Get status summary
    pub fn status_summary(&self, current_equity: f64) -> String {
        let mut status = Vec::new();

        if self.kill_switch.load(Ordering::Relaxed) {
            status.push("KILL_SWITCH".to_string());
        }
        if self.latency_breaker.is_tripped() {
            status.push("LATENCY".to_string());
        }
        if self.order_flow.is_tripped() {
            status.push("ORDER_FLOW".to_string());
        }
        if self.drawdown_breaker.is_tripped() {
            status.push(format!("DRAWDOWN({:.1}%)", self.drawdown_breaker.current_drawdown_pct(current_equity)));
        }

        if status.is_empty() {
            "OK".to_string()
        } else {
            format!("TRIPPED: {}", status.join(", "))
        }
    }

    /// Reset all breakers (manual intervention)
    pub fn reset_all(&mut self, current_equity: f64) {
        self.latency_breaker.reset();
        self.order_flow.reset();
        self.drawdown_breaker.reset(current_equity);
        info!("[CIRCUIT_BREAKERS] All breakers reset");
    }

    /// Get total trip count
    pub fn trip_count(&self) -> u64 {
        self.trip_count.load(Ordering::Relaxed)
    }

    /// Get current signal rate limiter availability (tokens remaining)
    pub fn signal_rate_available(&self) -> f64 {
        self.signal_limiter.available()
    }

    /// Get current order rate limiter availability (tokens remaining)
    pub fn order_rate_available(&self) -> f64 {
        self.order_limiter.available()
    }

    /// Get current p99 latency if available
    pub fn current_latency_p99(&self) -> Option<f64> {
        self.latency_breaker.current_percentile_latency()
    }

    /// Get detailed status for TUI display
    pub fn detailed_status(&self, current_equity: f64) -> CircuitBreakerStatus {
        CircuitBreakerStatus {
            is_tripped: self.is_any_tripped(),
            kill_switch_active: self.kill_switch.load(Ordering::Relaxed),
            latency_tripped: self.latency_breaker.is_tripped(),
            order_flow_tripped: self.order_flow.is_tripped(),
            drawdown_tripped: self.drawdown_breaker.is_tripped(),
            signal_tokens: self.signal_limiter.available(),
            order_tokens: self.order_limiter.available(),
            p99_latency_ms: self.latency_breaker.current_percentile_latency(),
            current_drawdown_pct: self.drawdown_breaker.current_drawdown_pct(current_equity),
            trip_count: self.trip_count.load(Ordering::Relaxed),
        }
    }
}

/// Detailed circuit breaker status for display
#[derive(Debug, Clone)]
pub struct CircuitBreakerStatus {
    pub is_tripped: bool,
    pub kill_switch_active: bool,
    pub latency_tripped: bool,
    pub order_flow_tripped: bool,
    pub drawdown_tripped: bool,
    pub signal_tokens: f64,
    pub order_tokens: f64,
    pub p99_latency_ms: Option<f64>,
    pub current_drawdown_pct: f64,
    pub trip_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_basic() {
        let mut limiter = RateLimiter::new("test", 10.0, 5);

        // Should allow 5 requests (burst capacity)
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }

        // 6th should fail
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_drawdown_breaker() {
        let mut breaker = DrawdownCircuitBreaker::new(100000.0, 10.0);

        // Normal operation
        assert!(breaker.update(99000.0)); // 1% dd
        assert!(breaker.update(95000.0)); // 5% dd

        // Should trip at 10%
        assert!(!breaker.update(89000.0)); // 11% dd
        assert!(breaker.is_tripped());
    }

    #[test]
    fn test_order_flow_monitor() {
        let mut monitor = OrderFlowMonitor::new(1, 5, 10);

        // Should allow 5 orders
        for _ in 0..5 {
            assert!(monitor.record_order());
        }

        // 6th should fail
        assert!(!monitor.record_order());
        assert!(monitor.is_tripped());
    }
}
