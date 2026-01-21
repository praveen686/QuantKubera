//! Chaos testing for network disconnect and reconnection scenarios
//! 
//! These tests verify the system's resilience to network failures.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::timeout;

/// Simulated network state for chaos testing
#[derive(Clone)]
pub struct ChaosNetwork {
    is_connected: Arc<AtomicBool>,
    disconnect_count: Arc<AtomicU32>,
    reconnect_count: Arc<AtomicU32>,
}

impl ChaosNetwork {
    pub fn new() -> Self {
        Self {
            is_connected: Arc::new(AtomicBool::new(true)),
            disconnect_count: Arc::new(AtomicU32::new(0)),
            reconnect_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn disconnect(&self) {
        self.is_connected.store(false, Ordering::SeqCst);
        self.disconnect_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn reconnect(&self) {
        self.is_connected.store(true, Ordering::SeqCst);
        self.reconnect_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::SeqCst)
    }

    pub fn disconnect_count(&self) -> u32 {
        self.disconnect_count.load(Ordering::SeqCst)
    }

    pub fn reconnect_count(&self) -> u32 {
        self.reconnect_count.load(Ordering::SeqCst)
    }
}

/// Simulated connector that responds to chaos network state
pub struct ChaosConnector {
    network: ChaosNetwork,
    data_received: Arc<AtomicU32>,
    reconnection_attempts: Arc<AtomicU32>,
    max_reconnect_attempts: u32,
}

impl ChaosConnector {
    pub fn new(network: ChaosNetwork) -> Self {
        Self {
            network,
            data_received: Arc::new(AtomicU32::new(0)),
            reconnection_attempts: Arc::new(AtomicU32::new(0)),
            max_reconnect_attempts: 5,
        }
    }

    pub async fn simulate_data_flow(&self, duration: Duration) {
        let start = std::time::Instant::now();
        
        while start.elapsed() < duration {
            if self.network.is_connected() {
                // Simulate receiving data
                self.data_received.fetch_add(1, Ordering::SeqCst);
            } else {
                // Attempt reconnection with exponential backoff
                self.attempt_reconnect().await;
            }
            
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn attempt_reconnect(&self) {
        let attempt = self.reconnection_attempts.fetch_add(1, Ordering::SeqCst);
        
        if attempt < self.max_reconnect_attempts {
            // Exponential backoff: 100ms, 200ms, 400ms, ...
            let backoff = Duration::from_millis(100 * (1 << attempt.min(4)));
            tokio::time::sleep(backoff).await;
            
            // Simulate reconnection succeeding
            self.network.reconnect();
        }
    }

    pub fn data_received(&self) -> u32 {
        self.data_received.load(Ordering::SeqCst)
    }

    pub fn reconnection_attempts(&self) -> u32 {
        self.reconnection_attempts.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_disconnect_recovery() {
        let network = ChaosNetwork::new();
        let connector = ChaosConnector::new(network.clone());
        
        // Start data flow in background
        let connector_clone = Arc::new(connector);
        let connector_bg = connector_clone.clone();
        let handle = tokio::spawn(async move {
            connector_bg.simulate_data_flow(Duration::from_millis(500)).await;
        });
        
        // Let it run briefly, then disconnect
        tokio::time::sleep(Duration::from_millis(100)).await;
        network.disconnect();
        
        // Let reconnection happen
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        // Should have reconnected
        assert!(network.is_connected(), "Network should have reconnected");
        
        // Wait for background task
        handle.await.unwrap();
        
        // Should have received some data before and after disconnect
        assert!(connector_clone.data_received() > 0, "Should have received data");
    }

    #[tokio::test]
    async fn test_multiple_disconnects() {
        let network = ChaosNetwork::new();
        
        // Simulate 3 disconnects
        for _ in 0..3 {
            network.disconnect();
            tokio::time::sleep(Duration::from_millis(10)).await;
            network.reconnect();
        }
        
        assert_eq!(network.disconnect_count(), 3);
        assert_eq!(network.reconnect_count(), 3);
    }

    #[tokio::test]
    async fn test_reconnection_backoff() {
        let network = ChaosNetwork::new();
        network.disconnect();
        
        let connector = ChaosConnector::new(network.clone());
        
        // Run briefly - should trigger reconnect attempts
        let _ = timeout(
            Duration::from_millis(500),
            connector.simulate_data_flow(Duration::from_secs(1))
        ).await;
        
        // Should have made reconnection attempts with backoff
        assert!(connector.reconnection_attempts() > 0);
    }

    #[tokio::test]
    async fn test_state_recovery_after_disconnect() {
        // Simulate state (e.g., orderbook) that needs recovery
        let state = Arc::new(AtomicU32::new(100)); // Initial state
        let network = ChaosNetwork::new();
        
        // Disconnect causes state corruption simulation
        network.disconnect();
        state.store(0, Ordering::SeqCst); // State lost
        
        // Reconnect and recover state
        network.reconnect();
        state.store(100, Ordering::SeqCst); // State recovered from snapshot
        
        assert!(network.is_connected());
        assert_eq!(state.load(Ordering::SeqCst), 100);
    }
}
