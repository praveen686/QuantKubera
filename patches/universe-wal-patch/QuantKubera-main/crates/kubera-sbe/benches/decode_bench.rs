use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kubera_sbe::{SbeHeader, BinanceSbeDecoder};
use byteorder::{ByteOrder, LittleEndian};
use chrono::Utc;

fn bench_sbe_decode(c: &mut Criterion) {
    // Mock SBE TradesStreamEvent binary (Header + Body)
    // Header (8 bytes): block_length(18), template_id(10000), schema_id(1), version(0)
    let header_bytes = [0x12, 0x00, 0x10, 0x27, 0x01, 0x00, 0x00, 0x00];
    
    // Body (49 bytes for 1 trade)
    let mut body_bytes = [0u8; 49];
    // eventTime (0..8)
    LittleEndian::write_u64(&mut body_bytes[0..8], Utc::now().timestamp_micros() as u64);
    // transactTime (8..16)
    LittleEndian::write_u64(&mut body_bytes[8..16], Utc::now().timestamp_micros() as u64);
    // priceExponent (-2)
    body_bytes[16] = -2i8 as u8;
    // qtyExponent (-4)
    body_bytes[17] = -4i8 as u8;
    
    // Group Header: blockLength (25), numInGroup (1)
    LittleEndian::write_u16(&mut body_bytes[18..20], 25);
    LittleEndian::write_u32(&mut body_bytes[20..24], 1);
    
    // Trade: id (8), price (8), qty (8), isBuyerMaker (1)
    LittleEndian::write_i64(&mut body_bytes[24..32], 12345);
    // Price 5000000 (with -2 expo = 50000.0)
    LittleEndian::write_i64(&mut body_bytes[32..40], 5000000);
    // Qty 15000 (with -4 expo = 1.5)
    LittleEndian::write_i64(&mut body_bytes[40..48], 15000);
    // isBuyerMaker (true)
    body_bytes[48] = 1;
    
    let mut full_msg = Vec::from(header_bytes);
    full_msg.extend_from_slice(&body_bytes);

    c.bench_function("sbe_decode_trade", |b| {
        b.iter(|| {
            let header = SbeHeader::decode(black_box(&full_msg[0..8])).unwrap();
            let _trade = BinanceSbeDecoder::decode_trade(&header, black_box(&full_msg[8..])).unwrap();
        })
    });
}

fn bench_json_decode(c: &mut Criterion) {
    let json_msg = r#"{"e":"aggTrade","E":1672531200000,"s":"BTCUSDT","a":12345,"p":"50000.00","q":"1.50","f":100,"l":105,"T":1672531199999,"m":true,"M":true}"#;

    c.bench_function("json_decode_agg_trade", |b| {
        b.iter(|| {
            let _: serde_json::Value = serde_json::from_str(black_box(json_msg)).unwrap();
        })
    });
}

criterion_group!(benches, bench_sbe_decode, bench_json_decode);
criterion_main!(benches);
