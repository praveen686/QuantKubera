//! # Simple Binary Encoding (SBE) Decoder Module
//!
//! Decodes Binance SBE-encoded market data messages for ultra-low latency.
//!
//! ## Description
//! Implements decoders for Binance's Simple Binary Encoding format, providing
//! a zero-copy or near-zero-copy interface for high-frequency trading. 
//! SBE bypasses the overhead of JSON parsing by mapping binary buffers 
//! directly to structured types.
//!
//! ## Supported Message Types
//! | Template ID | Type | Description |
//! |-------------|------|-------------|
//! | 10000 | TradesStreamEvent | Aggregated trades |
//! | 10003 | DepthDiffStreamEvent | Orderbook deltas |
//!
//! ## References
//! - Binance SBE Documentation: <https://github.com/binance/binance-spot-api-docs>
//! - SBE Specification: <https://github.com/FIXTradingCommunity/fix-simple-binary-encoding>
//! - IEEE Std 1016-2009: Software Design Descriptions

use byteorder::{ByteOrder, LittleEndian};
use chrono::{DateTime, TimeZone, Utc};

/// Fixed size of SBE message header in bytes.
pub const SBE_HEADER_SIZE: usize = 8;

/// Metadata header preceding all SBE messages.
///
/// # Fields
/// * `block_length` - Size of the fixed-length portion of the message.
/// * `template_id` - Unique identifier for the message type.
/// * `schema_id` - Version of the XML schema used for encoding.
/// * `version` - Specific version of the template.
#[derive(Debug, Clone)]
pub struct SbeHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

impl SbeHeader {
    /// Deserializes a binary header into an `SbeHeader` struct.
    ///
    /// # Parameters
    /// * `data` - Raw byte buffer (must be at least 8 bytes).
    pub fn decode(data: &[u8]) -> anyhow::Result<Self> {
        if data.len() < SBE_HEADER_SIZE {
            anyhow::bail!("Data too short for SBE header");
        }
        Ok(Self {
            block_length: LittleEndian::read_u16(&data[0..2]),
            template_id: LittleEndian::read_u16(&data[2..4]),
            schema_id: LittleEndian::read_u16(&data[4..6]),
            version: LittleEndian::read_u16(&data[6..8]),
        })
    }
}

/// Specialized decoder for Binance-specific SBE implementations.
pub struct BinanceSbeDecoder;

impl BinanceSbeDecoder {
    /// Decodes an aggregated trade message (Template 10000).
    ///
    /// # Parameters
    /// * `header` - Pre-decoded message header.
    /// * `body` - Remaining binary payload.
    pub fn decode_trade(header: &SbeHeader, body: &[u8]) -> anyhow::Result<AggTrade> {
        if header.template_id != 10000 {
            anyhow::bail!("Invalid template ID for TradesStreamEvent: {}", header.template_id);
        }

        if body.len() < 24 {
            anyhow::bail!("Trade body too short: {} bytes", body.len());
        }

        let transact_time_us = LittleEndian::read_i64(&body[0..8]) as u64;
        let event_time_us = LittleEndian::read_i64(&body[8..16]) as u64;
        let price_exponent = body[16] as i8;
        let qty_exponent = body[17] as i8;

        let group_offset = 18;
        if body.len() < group_offset + 6 {
            anyhow::bail!("Trade body too short for group header");
        }

        let trade_block_len = LittleEndian::read_u16(&body[group_offset..group_offset+2]) as usize;
        let group_count = LittleEndian::read_u32(&body[group_offset+2..group_offset+6]) as usize;

        if trade_block_len == 0 {
            anyhow::bail!("Invalid trade block length: 0");
        }

        let mut trades = Vec::with_capacity(group_count);
        let mut offset = group_offset + 6;

        for _ in 0..group_count {
            if body.len() < offset + 25 {
                anyhow::bail!("Trade body too short for trade entry at offset {}", offset);
            }

            let trade_id = LittleEndian::read_i64(&body[offset..offset+8]);
            let price_mantissa = LittleEndian::read_i64(&body[offset+8..offset+16]);
            let qty_mantissa = LittleEndian::read_i64(&body[offset+16..offset+24]);
            let is_buyer_maker = body[offset+24] != 0;

            let price = price_mantissa as f64 * 10f64.powi(price_exponent as i32);
            let quantity = qty_mantissa as f64 * 10f64.powi(qty_exponent as i32);

            trades.push(TradeEntry {
                trade_id,
                price,
                quantity,
                is_buyer_maker,
            });

            offset += trade_block_len;
        }

        if trades.is_empty() {
            anyhow::bail!("No trades in SBE message");
        }

        let last = trades.last().unwrap();
        Ok(AggTrade {
            price: last.price,
            quantity: last.quantity,
            transact_time_us,
            event_time_us,
            is_buyer_maker: last.is_buyer_maker,
            trade_count: trades.len(),
        })
    }

    /// Decodes an orderbook depth increment message (Template 10003).
    ///
    /// # Parameters
    /// * `header` - Pre-decoded message header.
    /// * `body` - Remaining binary payload.
    pub fn decode_depth_update(header: &SbeHeader, body: &[u8]) -> anyhow::Result<DepthUpdate> {
        if header.template_id != 10003 {
            anyhow::bail!("Invalid template ID for DepthDiffStreamEvent: {}", header.template_id);
        }

        if body.len() < 26 {
            anyhow::bail!("Depth body too short: {} bytes", body.len());
        }

        let transact_time_us = LittleEndian::read_i64(&body[0..8]) as u64;
        let first_update_id = LittleEndian::read_i64(&body[8..16]) as u64;
        let last_update_id = LittleEndian::read_i64(&body[16..24]) as u64;
        let price_exponent = body[24] as i8;
        let qty_exponent = body[25] as i8;

        let mut offset = 26;

        if body.len() < offset + 4 {
            anyhow::bail!("Depth body too short for bids group header");
        }
        let bid_block_len = LittleEndian::read_u16(&body[offset..offset+2]) as usize;
        let bid_count = LittleEndian::read_u16(&body[offset+2..offset+4]) as usize;
        offset += 4;

        let mut bids = Vec::with_capacity(bid_count);
        for i in 0..bid_count {
            if body.len() < offset + 16 {
                anyhow::bail!("Depth body too short for bid entry {} at offset {}", i, offset);
            }
            let price_mantissa = LittleEndian::read_i64(&body[offset..offset+8]);
            let qty_mantissa = LittleEndian::read_i64(&body[offset+8..offset+16]);

            bids.push(kubera_models::L2Level {
                price: price_mantissa as f64 * 10f64.powi(price_exponent as i32),
                size: qty_mantissa as f64 * 10f64.powi(qty_exponent as i32),
            });
            offset += bid_block_len;
        }

        if body.len() < offset + 4 {
            anyhow::bail!("Depth body too short for asks group header at offset {}", offset);
        }
        let ask_block_len = LittleEndian::read_u16(&body[offset..offset+2]) as usize;
        let ask_count = LittleEndian::read_u16(&body[offset+2..offset+4]) as usize;
        offset += 4;

        let mut asks = Vec::with_capacity(ask_count);
        for i in 0..ask_count {
            if body.len() < offset + 16 {
                anyhow::bail!("Depth body too short for ask entry {} at offset {}", i, offset);
            }
            let price_mantissa = LittleEndian::read_i64(&body[offset..offset+8]);
            let qty_mantissa = LittleEndian::read_i64(&body[offset+8..offset+16]);

            asks.push(kubera_models::L2Level {
                price: price_mantissa as f64 * 10f64.powi(price_exponent as i32),
                size: qty_mantissa as f64 * 10f64.powi(qty_exponent as i32),
            });
            offset += ask_block_len;
        }

        Ok(DepthUpdate {
            transact_time_us,
            first_update_id,
            last_update_id,
            bids,
            asks,
        })
    }

    /// Normalizes a microsecond epoch timestamp into a `DateTime<Utc>`.
    pub fn timestamp_to_datetime(timestamp_us: u64) -> DateTime<Utc> {
        let secs = (timestamp_us / 1_000_000) as i64;
        let nanos = ((timestamp_us % 1_000_000) * 1000) as u32;
        Utc.timestamp_opt(secs, nanos).unwrap()
    }
}

/// Raw representation of a trade event within an SBE group.
#[derive(Debug, Clone)]
pub struct TradeEntry {
    pub trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub is_buyer_maker: bool,
}

/// High-level representation of an aggregated trade event.
#[derive(Debug, Clone)]
pub struct AggTrade {
    pub price: f64,
    pub quantity: f64,
    pub transact_time_us: u64,
    pub event_time_us: u64,
    pub is_buyer_maker: bool,
    pub trade_count: usize,
}

impl AggTrade {
    /// Returns the transaction time as a standard `DateTime<Utc>`.
    pub fn exchange_time(&self) -> DateTime<Utc> {
        BinanceSbeDecoder::timestamp_to_datetime(self.transact_time_us)
    }
}

/// Structured representation of orderbook changes.
#[derive(Debug, Clone)]
pub struct DepthUpdate {
    pub transact_time_us: u64,
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub bids: Vec<kubera_models::L2Level>,
    pub asks: Vec<kubera_models::L2Level>,
}

impl DepthUpdate {
    /// Returns the transaction time as a standard `DateTime<Utc>`.
    pub fn exchange_time(&self) -> DateTime<Utc> {
        BinanceSbeDecoder::timestamp_to_datetime(self.transact_time_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_conversion() {
        let ts_us = 1766053116014248u64;
        let dt = BinanceSbeDecoder::timestamp_to_datetime(ts_us);
        assert!(dt.timestamp() > 0);
    }

    #[test]
    fn test_header_decode() {
        let data = [0x1a, 0x00, 0x13, 0x27, 0x01, 0x00, 0x00, 0x00];
        let header = SbeHeader::decode(&data).unwrap();
        assert_eq!(header.block_length, 26);
        assert_eq!(header.template_id, 10003); // 0x2713
        assert_eq!(header.schema_id, 1);
        assert_eq!(header.version, 0);
    }
}
