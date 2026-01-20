//! # Parquet Exporter for VectorBT Pro Integration
//!
//! Exports equity curves, signals, and trade data to Parquet format
//! for analysis in VectorBT Pro and other Python-based tools.

use arrow::array::{Float64Array, Int64Array, StringArray, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;
use tracing::info;

/// A single data point in the equity curve
#[derive(Debug, Clone)]
pub struct EquityPoint {
    pub timestamp_ms: i64,
    pub price: f64,
    pub equity: f64,
    pub position: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

/// A signal event for VectorBT
#[derive(Debug, Clone)]
pub struct SignalRecord {
    pub timestamp_ms: i64,
    pub symbol: String,
    pub signal: f64,       // -1.0 to 1.0
    pub direction: i64,    // -1 = sell, 0 = hold, 1 = buy
    pub price: f64,
    pub expert: String,
}

/// A fill/trade record for VectorBT
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub timestamp_ms: i64,
    pub symbol: String,
    pub side: String,      // "BUY" or "SELL"
    pub quantity: f64,
    pub price: f64,
    pub commission: f64,
    pub pnl: f64,
}

/// Parquet exporter for VectorBT Pro integration
pub struct ParquetExporter {
    equity_curve: Vec<EquityPoint>,
    signals: Vec<SignalRecord>,
    trades: Vec<TradeRecord>,
    output_dir: String,
}

impl ParquetExporter {
    /// Create a new exporter with the specified output directory
    pub fn new(output_dir: &str) -> Self {
        Self {
            equity_curve: Vec::new(),
            signals: Vec::new(),
            trades: Vec::new(),
            output_dir: output_dir.to_string(),
        }
    }

    /// Record an equity point
    pub fn record_equity(&mut self, point: EquityPoint) {
        self.equity_curve.push(point);
    }

    /// Record a signal
    pub fn record_signal(&mut self, signal: SignalRecord) {
        self.signals.push(signal);
    }

    /// Record a trade/fill
    pub fn record_trade(&mut self, trade: TradeRecord) {
        self.trades.push(trade);
    }

    /// Export equity curve to Parquet
    pub fn export_equity(&self, filename: &str) -> anyhow::Result<()> {
        if self.equity_curve.is_empty() {
            info!("No equity data to export");
            return Ok(());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("equity", DataType::Float64, false),
            Field::new("position", DataType::Float64, false),
            Field::new("unrealized_pnl", DataType::Float64, false),
            Field::new("realized_pnl", DataType::Float64, false),
        ]));

        let timestamps: Vec<i64> = self.equity_curve.iter().map(|p| p.timestamp_ms).collect();
        let prices: Vec<f64> = self.equity_curve.iter().map(|p| p.price).collect();
        let equities: Vec<f64> = self.equity_curve.iter().map(|p| p.equity).collect();
        let positions: Vec<f64> = self.equity_curve.iter().map(|p| p.position).collect();
        let unrealized: Vec<f64> = self.equity_curve.iter().map(|p| p.unrealized_pnl).collect();
        let realized: Vec<f64> = self.equity_curve.iter().map(|p| p.realized_pnl).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
                Arc::new(Float64Array::from(equities)) as ArrayRef,
                Arc::new(Float64Array::from(positions)) as ArrayRef,
                Arc::new(Float64Array::from(unrealized)) as ArrayRef,
                Arc::new(Float64Array::from(realized)) as ArrayRef,
            ],
        )?;

        let path = format!("{}/{}", self.output_dir, filename);
        let file = File::create(&path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!("Exported {} equity points to {}", self.equity_curve.len(), path);
        Ok(())
    }

    /// Export signals to Parquet
    pub fn export_signals(&self, filename: &str) -> anyhow::Result<()> {
        if self.signals.is_empty() {
            info!("No signal data to export");
            return Ok(());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("signal", DataType::Float64, false),
            Field::new("direction", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("expert", DataType::Utf8, false),
        ]));

        let timestamps: Vec<i64> = self.signals.iter().map(|s| s.timestamp_ms).collect();
        let symbols: Vec<&str> = self.signals.iter().map(|s| s.symbol.as_str()).collect();
        let signals: Vec<f64> = self.signals.iter().map(|s| s.signal).collect();
        let directions: Vec<i64> = self.signals.iter().map(|s| s.direction).collect();
        let prices: Vec<f64> = self.signals.iter().map(|s| s.price).collect();
        let experts: Vec<&str> = self.signals.iter().map(|s| s.expert.as_str()).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(symbols)) as ArrayRef,
                Arc::new(Float64Array::from(signals)) as ArrayRef,
                Arc::new(Int64Array::from(directions)) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
                Arc::new(StringArray::from(experts)) as ArrayRef,
            ],
        )?;

        let path = format!("{}/{}", self.output_dir, filename);
        let file = File::create(&path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!("Exported {} signals to {}", self.signals.len(), path);
        Ok(())
    }

    /// Export trades to Parquet
    pub fn export_trades(&self, filename: &str) -> anyhow::Result<()> {
        if self.trades.is_empty() {
            info!("No trade data to export");
            return Ok(());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("quantity", DataType::Float64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("commission", DataType::Float64, false),
            Field::new("pnl", DataType::Float64, false),
        ]));

        let timestamps: Vec<i64> = self.trades.iter().map(|t| t.timestamp_ms).collect();
        let symbols: Vec<&str> = self.trades.iter().map(|t| t.symbol.as_str()).collect();
        let sides: Vec<&str> = self.trades.iter().map(|t| t.side.as_str()).collect();
        let quantities: Vec<f64> = self.trades.iter().map(|t| t.quantity).collect();
        let prices: Vec<f64> = self.trades.iter().map(|t| t.price).collect();
        let commissions: Vec<f64> = self.trades.iter().map(|t| t.commission).collect();
        let pnls: Vec<f64> = self.trades.iter().map(|t| t.pnl).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(StringArray::from(symbols)) as ArrayRef,
                Arc::new(StringArray::from(sides)) as ArrayRef,
                Arc::new(Float64Array::from(quantities)) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
                Arc::new(Float64Array::from(commissions)) as ArrayRef,
                Arc::new(Float64Array::from(pnls)) as ArrayRef,
            ],
        )?;

        let path = format!("{}/{}", self.output_dir, filename);
        let file = File::create(&path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!("Exported {} trades to {}", self.trades.len(), path);
        Ok(())
    }

    /// Export all data to Parquet files
    pub fn export_all(&self, prefix: &str) -> anyhow::Result<()> {
        self.export_equity(&format!("{}_equity.parquet", prefix))?;
        self.export_signals(&format!("{}_signals.parquet", prefix))?;
        self.export_trades(&format!("{}_trades.parquet", prefix))?;
        Ok(())
    }

    /// Get summary stats
    pub fn summary(&self) -> String {
        format!(
            "ParquetExporter: {} equity points, {} signals, {} trades",
            self.equity_curve.len(),
            self.signals.len(),
            self.trades.len()
        )
    }
}

impl Default for ParquetExporter {
    fn default() -> Self {
        Self::new(".")
    }
}
