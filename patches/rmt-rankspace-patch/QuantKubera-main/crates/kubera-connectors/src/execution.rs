use hmac::{Hmac, Mac};
use sha2::Sha256;
use reqwest::Client;
use std::time::{SystemTime, UNIX_EPOCH};
use kubera_models::{OrderEvent, OrderPayload, OrderType};
use tracing::info;

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceRestAdapter {
    _client: Client,
    _api_key: String,
    api_secret: String,
    base_url: String,
}

impl BinanceRestAdapter {
    pub fn new(api_key: String, api_secret: String, is_testnet: bool) -> Self {
        let base_url = if is_testnet {
            "https://testnet.binance.vision".to_string()
        } else {
            "https://api.binance.com".to_string()
        };

        Self {
            _client: Client::new(),
            _api_key: api_key,
            api_secret,
            base_url,
        }
    }

    pub async fn submit_order(&self, order: &OrderEvent) -> anyhow::Result<()> {
        if let OrderPayload::New { symbol, side, quantity, order_type, price, .. } = &order.payload {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            
            let mut params = format!(
                "symbol={}&side={}&type={}&quantity={}&timestamp={}",
                symbol.replace("-", ""),
                side.to_string().to_uppercase(),
                match order_type {
                    OrderType::Market => "MARKET",
                    OrderType::Limit => "LIMIT",
                },
                quantity,
                timestamp
            );

            if let Some(p) = price {
                params.push_str(&format!("&price={}&timeInForce=GTC", p));
            }

            let signature = self.sign(&params);
            let url = format!("{}/api/v3/order?{}&signature={}", self.base_url, params, signature);

            info!("Submitting order to Binance: {}", url);

            // In a real system, we'd actually send the request:
            /*
            let res = self.client.post(&url)
                .header("X-MBX-APIKEY", &self.api_key)
                .send()
                .await?;
            debug!("Binance response: {:?}", res.text().await?);
            */
            
            info!("MOCK: Binance Order Submitted Successfully");
        }
        Ok(())
    }

    fn sign(&self, data: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(data.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}
