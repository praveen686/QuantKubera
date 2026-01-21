//! # Parquet Data Exporter Module
//!
//! Provides high-performance persistence for market data using Apache Parquet.
//!
//! ## Description
//! Implements a buffered exporter that converts internal `MarketEvent` streams
//! into columnar Parquet files via Apache Arrow record batches. Optimized for
//! large-scale backtesting research and cold-storage analytics.
//!
//! ## Technical Specifications
//! - **Schema Ticks**: [exchange_time, local_time, symbol, price, size]
//! - **Schema Bars**: [timestamp, symbol, open, high, low, close, volume, interval_ms]
//! - **Encoding**: Snappy compression (default)
//! - **Concurrency**: Thread-hostile during write (mutex required for shared use)
//!
//! ## References
//! - IEEE Std 1016-2009: Software Design Descriptions
//! - Apache Parquet Columnar Format Spec

use kubera_models::{MarketEvent, MarketPayload};
use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::arrow_writer::ArrowWriter;
use std::fs::File;
use std::sync::Arc;
use std::path::Path;

/// High-throughput Parquet storage engine.
///
/// # Description
/// Manages dual-buffered storage for ticks and aggregated bars. Implements
/// automatic batching for optimal columnar compression.
///
/// # Fields
/// * `ticks_buffer` - Pending tick events
/// * `bars_buffer` - Pending bar events
/// * `batch_size` - Threshold for flushing to disk (in rows)
pub struct ParquetExporter {
    ticks_buffer: Vec<MarketEvent>,
    bars_buffer: Vec<MarketEvent>,
    batch_size: usize,
}

impl ParquetExporter {
    /// Creates a new exporter instance.
    ///
    /// # Parameters
    /// * `batch_size` - Ideal row count per Parquet page/batch
    pub fn new(batch_size: usize) -> Self {
        Self {
            ticks_buffer: Vec::with_capacity(batch_size),
            bars_buffer: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    /// Appends an event to the appropriate internal buffer.
    ///
    /// # Parameters
    /// * `event` - Market event to persist
    pub fn add_event(&mut self, event: MarketEvent) {
        match event.payload {
            MarketPayload::Tick { .. } => self.ticks_buffer.push(event),
            MarketPayload::Bar { .. } => self.bars_buffer.push(event),
            _ => {}
        }
    }

    /// Serializes buffered ticks to a Parquet file.
    ///
    /// # Parameters
    /// * `path` - Destination file path (.parquet extension recommended)
    ///
    /// # Returns
    /// `Ok(())` on success, `Err` on I/O or Arrow schema violations.
    pub fn write_ticks<P: AsRef<Path>>(&mut self, path: P) -> anyhow::Result<()> {
        if self.ticks_buffer.is_empty() {
            return Ok(());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("exchange_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("local_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::Float64, false),
        ]));

        let mut exchange_times = Vec::new();
        let mut local_times = Vec::new();
        let mut symbols = Vec::new();
        let mut prices = Vec::new();
        let mut sizes = Vec::new();

        for event in &self.ticks_buffer {
            if let MarketPayload::Tick { price, size, .. } = &event.payload {
                exchange_times.push(event.exchange_time.timestamp_nanos_opt().unwrap_or(0));
                local_times.push(event.local_time.timestamp_nanos_opt().unwrap_or(0));
                symbols.push(event.symbol.as_str());
                prices.push(*price);
                sizes.push(*size);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(exchange_times)),
                Arc::new(TimestampNanosecondArray::from(local_times)),
                Arc::new(StringArray::from(symbols)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Float64Array::from(sizes)),
            ],
        )?;

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        self.ticks_buffer.clear();
        Ok(())
    }

    /// Serializes buffered bars to a Parquet file.
    ///
    /// # Parameters
    /// * `path` - Destination file path
    ///
    /// # Returns
    /// `Ok(())` on success.
    pub fn write_bars<P: AsRef<Path>>(&mut self, path: P) -> anyhow::Result<()> {
        if self.bars_buffer.is_empty() {
            return Ok(());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
            Field::new("interval_ms", DataType::UInt64, false),
        ]));

        let mut timestamps = Vec::new();
        let mut symbols = Vec::new();
        let mut opens = Vec::new();
        let mut highs = Vec::new();
        let mut lows = Vec::new();
        let mut closes = Vec::new();
        let mut volumes = Vec::new();
        let mut intervals = Vec::new();

        for event in &self.bars_buffer {
            if let MarketPayload::Bar { open, high, low, close, volume, interval_ms } = &event.payload {
                timestamps.push(event.exchange_time.timestamp_nanos_opt().unwrap_or(0));
                symbols.push(event.symbol.as_str());
                opens.push(*open);
                highs.push(*high);
                lows.push(*low);
                closes.push(*close);
                volumes.push(*volume);
                intervals.push(*interval_ms);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(StringArray::from(symbols)),
                Arc::new(Float64Array::from(opens)),
                Arc::new(Float64Array::from(highs)),
                Arc::new(Float64Array::from(lows)),
                Arc::new(Float64Array::from(closes)),
                Arc::new(Float64Array::from(volumes)),
                Arc::new(arrow::array::UInt64Array::from(intervals)),
            ],
        )?;

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        self.bars_buffer.clear();
        Ok(())
    }

    /// Evaluation of buffer state.
    ///
    /// # Returns
    /// `true` if either tick or bar buffers meet `batch_size`.
    pub fn is_full(&self) -> bool {
        self.ticks_buffer.len() >= self.batch_size || self.bars_buffer.len() >= self.batch_size
    }
}
