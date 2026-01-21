//! # Binance REST API Client
//!
//! Synchronous data retrieval for orderbook initialization.
//!
//! ## Description
//! Provides the fallback REST-based orchestration for retrieving large
//! scale orderbook snapshots that cannot be efficiently delivered over 
//! WebSocket streams alone.
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Binance API v3 documentation

use serde::Deserialize;
use tracing::info;

const REST_API_URL: &str = "https://api.binance.com";

/// Raw JSON representation of an L2 Depth response.
#[derive(Debug, Deserialize)]
pub struct DepthResponse {
    /// Final sequence ID applied to this snapshot.
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    /// Collection of [Price, Quantity] string pairs for bids.
    pub bids: Vec<[String; 2]>,
    /// Collection of [Price, Quantity] string pairs for asks.
    pub asks: Vec<[String; 2]>,
}

/// Retrieves a full L2 snapshot for a specific instrument.
///
/// # Parameters
/// * `symbol` - Asset identifier (e.g., "BTCUSDT").
/// * `limit` - Number of levels to fetch (Default: 1000).
pub async fn fetch_depth_snapshot(symbol: &str, limit: u32) -> anyhow::Result<DepthResponse> {
    let url = format!("{}/api/v3/depth?symbol={}&limit={}", REST_API_URL, symbol.to_uppercase(), limit);
    info!("Fetching depth snapshot: {}", url);
    
    let client = reqwest::Client::new();
    let response = client.get(&url)
        .send()
        .await?
        .error_for_status()?;
    
    let depth: DepthResponse = response.json().await?;
    info!("Received depth snapshot: lastUpdateId={}, {} bids, {} asks", 
          depth.last_update_id, depth.bids.len(), depth.asks.len());
    Ok(depth)
}

/// Deserializes raw string-based API response into numeric `L2Snapshot`.
pub fn depth_to_snapshot(depth: &DepthResponse) -> kubera_models::L2Snapshot {
    let bids: Vec<kubera_models::L2Level> = depth.bids.iter()
        .filter_map(|[p, q]| {
            Some(kubera_models::L2Level {
                price: p.parse().ok()?,
                size: q.parse().ok()?,
            })
        })
        .collect();
    
    let asks: Vec<kubera_models::L2Level> = depth.asks.iter()
        .filter_map(|[p, q]| {
            Some(kubera_models::L2Level {
                price: p.parse().ok()?,
                size: q.parse().ok()?,
            })
        })
        .collect();
    
    kubera_models::L2Snapshot {
        bids,
        asks,
        update_id: depth.last_update_id,
    }
}
