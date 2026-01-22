//! # vectorbtpro Export Pipeline
//!
//! Per Z-Phase spec section 10: Export two artifacts for vectorbtpro.
//!
//! ## Market Data Frame (10.1)
//! Per instrument time series:
//! - mid, best bid/ask, microprice, spread, imbalance
//!
//! ## Trades / Orders / Fills (10.1)
//! Canonical fill ledger with:
//! - timestamp, instrument, side, qty, price, fees, strategy_id, order_id
//!
//! ## How vectorbtpro consumes it (10.2)
//! Option A: Portfolio from orders/fills (recommended - preserves fill realism)
//! Option B: Signal-based backtest (less faithful for multi-leg options)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::depth5::Depth5Book;
use crate::ledger::{LedgerFill, OptionsLedger};

/// Market data point for time series export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataPoint {
    /// Timestamp.
    pub ts: DateTime<Utc>,
    /// Trading symbol.
    pub symbol: String,
    /// Mid price.
    pub mid: f64,
    /// Best bid.
    pub bid: f64,
    /// Best ask.
    pub ask: f64,
    /// Microprice.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub microprice: Option<f64>,
    /// Spread in basis points.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread_bps: Option<f64>,
    /// Top-of-book imbalance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imbalance: Option<f64>,
    /// Total bid depth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bid_depth: Option<u32>,
    /// Total ask depth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ask_depth: Option<u32>,
}

impl MarketDataPoint {
    /// Create from Depth5Book.
    pub fn from_book(book: &Depth5Book, ts: DateTime<Utc>) -> Option<Self> {
        let mid = book.mid()?;
        let bid = book.best_bid()?;
        let ask = book.best_ask()?;

        Some(Self {
            ts,
            symbol: book.symbol.clone(),
            mid,
            bid,
            ask,
            microprice: book.microprice(),
            spread_bps: book.spread_bps(),
            imbalance: Some(book.imbalance_top1()),
            bid_depth: Some(book.total_bid_depth()),
            ask_depth: Some(book.total_ask_depth()),
        })
    }
}

/// Fill record for vectorbtpro export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportFill {
    /// Timestamp (ISO 8601).
    pub timestamp: String,
    /// Trading symbol.
    pub instrument: String,
    /// Side: "BUY" or "SELL".
    pub side: String,
    /// Quantity (lots).
    pub qty: u32,
    /// Fill price.
    pub price: f64,
    /// Fees paid.
    pub fees: f64,
    /// Strategy ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_id: Option<String>,
    /// Order ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
}

impl From<&LedgerFill> for ExportFill {
    fn from(fill: &LedgerFill) -> Self {
        Self {
            timestamp: fill.ts.to_rfc3339(),
            instrument: fill.symbol.clone(),
            side: fill.side.clone(),
            qty: fill.qty,
            price: fill.price,
            fees: fill.fees,
            strategy_id: fill.strategy_id.clone(),
            order_id: fill.order_id.clone(),
        }
    }
}

/// Export configuration.
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Output directory.
    pub out_dir: String,
    /// Strategy name for attribution.
    pub strategy_name: String,
    /// Include extended market data (microprice, imbalance).
    pub include_extended: bool,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            out_dir: "artifacts/export".to_string(),
            strategy_name: "UNKNOWN".to_string(),
            include_extended: true,
        }
    }
}

/// Market data collector for time series export.
#[derive(Debug, Default)]
pub struct MarketDataCollector {
    /// Time series per symbol.
    data: HashMap<String, Vec<MarketDataPoint>>,
}

impl MarketDataCollector {
    /// Create new collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a snapshot from books.
    pub fn record(&mut self, books: &HashMap<String, Depth5Book>, ts: DateTime<Utc>) {
        for (symbol, book) in books {
            if let Some(point) = MarketDataPoint::from_book(book, ts) {
                self.data.entry(symbol.clone()).or_default().push(point);
            }
        }
    }

    /// Get data for symbol.
    pub fn get(&self, symbol: &str) -> Option<&Vec<MarketDataPoint>> {
        self.data.get(symbol)
    }

    /// Get all symbols.
    pub fn symbols(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Total data points.
    pub fn total_points(&self) -> usize {
        self.data.values().map(|v| v.len()).sum()
    }
}

/// Export market data and fills to vectorbtpro-ready format.
pub struct VectorbtExporter {
    config: ExportConfig,
}

impl VectorbtExporter {
    /// Create new exporter.
    pub fn new(config: ExportConfig) -> Self {
        Self { config }
    }

    /// Export market data to CSV.
    pub fn export_market_csv(&self, collector: &MarketDataCollector) -> anyhow::Result<String> {
        let out_dir = Path::new(&self.config.out_dir);
        std::fs::create_dir_all(out_dir)?;

        let path = out_dir.join("market_data.csv");
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        // Header
        if self.config.include_extended {
            writeln!(writer, "timestamp,symbol,mid,bid,ask,microprice,spread_bps,imbalance,bid_depth,ask_depth")?;
        } else {
            writeln!(writer, "timestamp,symbol,mid,bid,ask")?;
        }

        // Data (sorted by timestamp)
        let mut all_points: Vec<&MarketDataPoint> = collector.data.values().flatten().collect();
        all_points.sort_by_key(|p| p.ts);

        for p in all_points {
            if self.config.include_extended {
                writeln!(
                    writer,
                    "{},{},{:.4},{:.4},{:.4},{:.4},{:.2},{:.4},{},{}",
                    p.ts.to_rfc3339(),
                    p.symbol,
                    p.mid,
                    p.bid,
                    p.ask,
                    p.microprice.unwrap_or(p.mid),
                    p.spread_bps.unwrap_or(0.0),
                    p.imbalance.unwrap_or(0.0),
                    p.bid_depth.unwrap_or(0),
                    p.ask_depth.unwrap_or(0),
                )?;
            } else {
                writeln!(
                    writer,
                    "{},{},{:.4},{:.4},{:.4}",
                    p.ts.to_rfc3339(),
                    p.symbol,
                    p.mid,
                    p.bid,
                    p.ask,
                )?;
            }
        }

        writer.flush()?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Export market data to JSONL.
    pub fn export_market_jsonl(&self, collector: &MarketDataCollector) -> anyhow::Result<String> {
        let out_dir = Path::new(&self.config.out_dir);
        std::fs::create_dir_all(out_dir)?;

        let path = out_dir.join("market_data.jsonl");
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        let mut all_points: Vec<&MarketDataPoint> = collector.data.values().flatten().collect();
        all_points.sort_by_key(|p| p.ts);

        for p in all_points {
            writeln!(writer, "{}", serde_json::to_string(p)?)?;
        }

        writer.flush()?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Export fills to CSV.
    pub fn export_fills_csv(&self, ledger: &OptionsLedger) -> anyhow::Result<String> {
        let out_dir = Path::new(&self.config.out_dir);
        std::fs::create_dir_all(out_dir)?;

        let path = out_dir.join("fills.csv");
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        writeln!(writer, "timestamp,instrument,side,qty,price,fees,strategy_id,order_id")?;

        for fill in ledger.fills() {
            writeln!(
                writer,
                "{},{},{},{},{:.4},{:.4},{},{}",
                fill.ts.to_rfc3339(),
                fill.symbol,
                fill.side,
                fill.qty,
                fill.price,
                fill.fees,
                fill.strategy_id.as_deref().unwrap_or(""),
                fill.order_id.as_deref().unwrap_or(""),
            )?;
        }

        writer.flush()?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Export fills to JSONL.
    pub fn export_fills_jsonl(&self, ledger: &OptionsLedger) -> anyhow::Result<String> {
        let out_dir = Path::new(&self.config.out_dir);
        std::fs::create_dir_all(out_dir)?;

        let path = out_dir.join("fills.jsonl");
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);

        for fill in ledger.fills() {
            let export: ExportFill = fill.into();
            writeln!(writer, "{}", serde_json::to_string(&export)?)?;
        }

        writer.flush()?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Export summary report.
    pub fn export_summary(&self, ledger: &OptionsLedger) -> anyhow::Result<String> {
        let out_dir = Path::new(&self.config.out_dir);
        std::fs::create_dir_all(out_dir)?;

        let path = out_dir.join("summary.json");

        let summary = serde_json::json!({
            "strategy": self.config.strategy_name,
            "total_trades": ledger.fills().len(),
            "total_pnl": ledger.total_pnl(),
            "realized_pnl": ledger.total_realized_pnl(),
            "unrealized_pnl": ledger.total_unrealized_pnl(),
            "total_fees": ledger.total_fees(),
            "net_pnl": ledger.net_pnl(),
            "open_positions": ledger.open_position_count(),
            "total_notional": ledger.total_notional(),
        });

        std::fs::write(&path, serde_json::to_string_pretty(&summary)?)?;
        Ok(path.to_string_lossy().to_string())
    }

    /// Export all artifacts.
    pub fn export_all(
        &self,
        collector: &MarketDataCollector,
        ledger: &OptionsLedger,
    ) -> anyhow::Result<ExportManifest> {
        let market_csv = self.export_market_csv(collector)?;
        let market_jsonl = self.export_market_jsonl(collector)?;
        let fills_csv = self.export_fills_csv(ledger)?;
        let fills_jsonl = self.export_fills_jsonl(ledger)?;
        let summary = self.export_summary(ledger)?;

        let manifest = ExportManifest {
            strategy: self.config.strategy_name.clone(),
            created_at: Utc::now().to_rfc3339(),
            files: vec![
                ("market_data.csv".to_string(), market_csv),
                ("market_data.jsonl".to_string(), market_jsonl),
                ("fills.csv".to_string(), fills_csv),
                ("fills.jsonl".to_string(), fills_jsonl),
                ("summary.json".to_string(), summary),
            ],
            market_points: collector.total_points(),
            fill_count: ledger.fills().len(),
        };

        // Write manifest
        let manifest_path = Path::new(&self.config.out_dir).join("manifest.json");
        std::fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;

        Ok(manifest)
    }
}

/// Export manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportManifest {
    pub strategy: String,
    pub created_at: String,
    pub files: Vec<(String, String)>,
    pub market_points: usize,
    pub fill_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::DepthLevelF64;

    #[test]
    fn test_market_data_collector() {
        let mut collector = MarketDataCollector::new();
        let mut books = HashMap::new();

        let mut book = Depth5Book::new("TEST");
        book.bids = vec![DepthLevelF64 { price: 100.0, qty: 100 }];
        book.asks = vec![DepthLevelF64 { price: 101.0, qty: 100 }];
        books.insert("TEST".to_string(), book);

        collector.record(&books, Utc::now());

        assert_eq!(collector.total_points(), 1);
        let data = collector.get("TEST").unwrap();
        assert_eq!(data[0].mid, 100.5);
    }
}
