use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite::client::IntoClientRequest};
use futures::{StreamExt, SinkExt};
use url::Url;
use kubera_sbe::{SbeHeader, BinanceSbeDecoder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let api_key = std::env::var("BINANCE_API_KEY_ED25519").expect("BINANCE_API_KEY_ED25519 must be set");

    let url_str = "wss://stream-sbe.binance.com:9443/stream";
    println!("Connecting to Binance SBE stream: {}", url_str);

    let url = Url::parse(url_str)?;
    let mut request = url.into_client_request()?;
    request.headers_mut().insert("X-MBX-APIKEY", api_key.parse()?);
    request.headers_mut().insert("Sec-WebSocket-Protocol", "binance-sbe".parse()?);

    let (ws_stream, _) = connect_async(request).await?;
    println!("✅ Connected successfully!");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to both trade and depth streams
    // NOTE: Use btcusdt@depth (not @depth@100ms) for SBE format
    let sub = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@trade",
            "btcusdt@depth"
        ],
        "id": 1
    });

    println!("Subscribing to btcusdt@trade and btcusdt@depth...");
    write.send(Message::Text(sub.to_string())).await?;

    let mut trade_count = 0;
    let mut depth_count = 0;
    let max_trades = 5;
    let max_depth = 3;

    println!("\n--- Waiting for SBE messages ---\n");

    while trade_count < max_trades || depth_count < max_depth {
        match read.next().await {
            Some(Ok(Message::Binary(bin))) => {
                if bin.len() < 8 { continue; }

                if let Ok(header) = SbeHeader::decode(&bin[..8]) {
                    match header.template_id {
                        10000 => {
                            // Trade message
                            match BinanceSbeDecoder::decode_trade(&header, &bin[8..]) {
                                Ok(trade) => {
                                    trade_count += 1;
                                    println!("📈 TRADE #{}", trade_count);
                                    println!("   Price:     ${:.2}", trade.price);
                                    println!("   Quantity:  {:.6} BTC", trade.quantity);
                                    println!("   Side:      {}", if trade.is_buyer_maker { "SELL" } else { "BUY" });
                                    println!("   Timestamp: {} ({})", trade.transact_time_us, trade.exchange_time());
                                    println!();
                                }
                                Err(e) => println!("❌ Trade decode error: {}", e),
                            }
                        }
                        10003 => {
                            // Depth update
                            match BinanceSbeDecoder::decode_depth_update(&header, &bin[8..]) {
                                Ok(depth) => {
                                    depth_count += 1;
                                    println!("📊 DEPTH UPDATE #{} (U={}, u={}, time={})",
                                        depth_count, depth.first_update_id, depth.last_update_id, depth.exchange_time());

                                    println!("   BIDS ({} levels):", depth.bids.len());
                                    for (i, bid) in depth.bids.iter().take(5).enumerate() {
                                        println!("     [{:2}] ${:.2} @ {:.6}", i+1, bid.price, bid.size);
                                    }
                                    if depth.bids.len() > 5 {
                                        println!("     ... and {} more", depth.bids.len() - 5);
                                    }

                                    println!("   ASKS ({} levels):", depth.asks.len());
                                    for (i, ask) in depth.asks.iter().take(5).enumerate() {
                                        println!("     [{:2}] ${:.2} @ {:.6}", i+1, ask.price, ask.size);
                                    }
                                    if depth.asks.len() > 5 {
                                        println!("     ... and {} more", depth.asks.len() - 5);
                                    }
                                    println!();
                                }
                                Err(e) => println!("❌ Depth decode error: {}", e),
                            }
                        }
                        _ => {
                            println!("⚠️ Unknown template ID: {}", header.template_id);
                        }
                    }
                }
            }
            Some(Ok(Message::Text(text))) => {
                println!("📝 JSON Response: {}", text);
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = write.send(Message::Pong(p)).await;
            }
            Some(Err(e)) => {
                println!("❌ Error: {}", e);
                break;
            }
            None => {
                println!("Connection closed");
                break;
            }
            _ => {}
        }
    }

    println!("\n--- Test Summary ---");
    println!("Trades decoded:  {}", trade_count);
    println!("Depth updates:   {}", depth_count);
    println!("✅ SBE LOB test completed successfully!");

    Ok(())
}
