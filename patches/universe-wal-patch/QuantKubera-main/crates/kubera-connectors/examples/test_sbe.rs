use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite::client::IntoClientRequest};
use futures::{StreamExt, SinkExt};
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let api_key = std::env::var("BINANCE_API_KEY_ED25519").expect("BINANCE_API_KEY_ED25519 must be set");

    let urls = vec![
        "wss://stream-sbe.binance.com:9443/stream",
        "wss://stream-sbe.binance.com:9443/ws",
        "wss://stream-sbe.binance.com:9443/",
        "wss://stream-sbe.binance.com/stream",
        "wss://stream-sbe.binance.com/ws",
        "wss://stream-sbe.binance.com:9443/ws/btcusdt@trade?responseFormat=sbe&sbeSchemaId=1&sbeSchemaVersion=0",
    ];

    for url_str in urls {
        println!("\n--- Testing URL: {} ---", url_str);
        let url = Url::parse(url_str)?;
        let mut request = url.into_client_request()?;
        request.headers_mut().insert("X-MBX-APIKEY", api_key.parse()?);
        request.headers_mut().insert("Sec-WebSocket-Protocol", "binance-sbe".parse()?);

        match connect_async(request).await {
            Ok((ws_stream, _)) => {
                println!("✅ SUCCESS: Connected to {}", url_str);
                let (mut write, mut read) = ws_stream.split();
                
                let sub = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": ["btcusdt@trade"],
                    "id": 1
                });
                println!("Sending subscription...");
                write.send(Message::Text(sub.to_string())).await?;

                let mut count = 0;
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Binary(bin)) => {
                            println!("Received BINARY: {} bytes", bin.len());
                            if bin.len() >= 8 {
                                println!("Header: {:02x?}", &bin[..8]);
                            }
                            count += 1;
                        }
                        Ok(Message::Text(text)) => println!("Received TEXT: {}", text),
                        Ok(Message::Ping(p)) => {
                            println!("Received Ping, sending Pong...");
                            write.send(Message::Pong(p)).await?;
                        }
                        Err(e) => println!("Error during message read: {}", e),
                        _ => {}
                    }
                    if count >= 10 { break; }
                }
                return Ok(());
            }
            Err(e) => {
                println!("❌ FAILED: {} - Error: {}", url_str, e);
            }
        }
    }

    println!("\nAll connection attempts failed.");
    Ok(())
}
