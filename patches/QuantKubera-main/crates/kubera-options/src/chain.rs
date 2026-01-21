use crate::contract::OptionType;
use crate::pricing::implied_volatility;
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

const KITE_INSTRUMENTS_URL: &str = "https://api.kite.trade/instruments/NFO";
const KITE_QUOTE_URL: &str = "https://api.kite.trade/quote";

/// Zerodha instrument data from CSV
#[derive(Debug, Clone, Deserialize)]
pub struct ZerodhaInstrument {
    pub instrument_token: u32,
    pub exchange_token: u32,
    pub tradingsymbol: String,
    pub name: String,
    pub last_price: f64,
    pub expiry: String,
    pub strike: f64,
    pub tick_size: f64,
    pub lot_size: u32,
    pub instrument_type: String,
    pub segment: String,
    pub exchange: String,
}

/// Quote data from Kite API
#[derive(Debug, Clone, Deserialize)]
pub struct KiteQuote {
    pub last_price: f64,
    #[serde(default)]
    pub last_quantity: u64,
    #[serde(default)]
    pub volume: u64,
    #[serde(default)]
    pub buy_quantity: u64,
    #[serde(default)]
    pub sell_quantity: u64,
    pub ohlc: Option<OHLC>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OHLC {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// IV Surface data point
#[derive(Debug, Clone, Serialize)]
pub struct IVPoint {
    pub strike: f64,
    pub expiry: NaiveDate,
    pub option_type: OptionType,
    pub iv: f64,
    pub delta: f64,
    pub last_price: f64,
}

/// IV Surface for an underlying
#[derive(Debug, Clone)]
pub struct IVSurface {
    pub underlying: String,
    pub spot_price: f64,
    pub points: Vec<IVPoint>,
    pub timestamp: chrono::DateTime<Utc>,
}

impl IVSurface {
    pub fn new(underlying: String, spot_price: f64) -> Self {
        Self {
            underlying,
            spot_price,
            points: Vec::new(),
            timestamp: Utc::now(),
        }
    }

    /// Get IV skew (difference between OTM put and OTM call IV)
    pub fn iv_skew(&self, expiry: NaiveDate, otm_distance: f64) -> Option<f64> {
        let put_strike = self.spot_price - otm_distance;
        let call_strike = self.spot_price + otm_distance;
        
        let put_iv = self.points.iter()
            .find(|p| p.expiry == expiry && p.option_type == OptionType::Put 
                  && (p.strike - put_strike).abs() < 25.0)?
            .iv;
        
        let call_iv = self.points.iter()
            .find(|p| p.expiry == expiry && p.option_type == OptionType::Call 
                  && (p.strike - call_strike).abs() < 25.0)?
            .iv;
        
        Some(put_iv - call_iv)
    }

    /// Get ATM IV for an expiry
    pub fn atm_iv(&self, expiry: NaiveDate) -> Option<f64> {
        let atm_strike = (self.spot_price / 50.0).round() * 50.0;
        
        let call_iv = self.points.iter()
            .find(|p| p.expiry == expiry && p.option_type == OptionType::Call 
                  && (p.strike - atm_strike).abs() < 25.0)?
            .iv;
        
        Some(call_iv)
    }

    /// Get IV percentile (how current IV compares to historical range)
    pub fn iv_percentile(&self, expiry: NaiveDate, historical_min: f64, historical_max: f64) -> Option<f64> {
        let atm_iv = self.atm_iv(expiry)?;
        Some((atm_iv - historical_min) / (historical_max - historical_min) * 100.0)
    }

    /// Get term structure - ATM IV across multiple expiries
    pub fn term_structure(&self) -> Vec<(NaiveDate, f64)> {
        let mut expiries: Vec<NaiveDate> = self.points.iter()
            .map(|p| p.expiry)
            .collect();
        expiries.sort();
        expiries.dedup();
        
        expiries.iter()
            .filter_map(|expiry| {
                self.atm_iv(*expiry).map(|iv| (*expiry, iv))
            })
            .collect()
    }

    /// Check if term structure is in contango (far expiry IV > near expiry IV)
    pub fn is_contango(&self) -> bool {
        let ts = self.term_structure();
        if ts.len() < 2 {
            return false;
        }
        ts.last().map(|(_, iv)| *iv).unwrap_or(0.0) > ts.first().map(|(_, iv)| *iv).unwrap_or(0.0)
    }

    /// Check if term structure is in backwardation (near expiry IV > far expiry IV)
    pub fn is_backwardation(&self) -> bool {
        let ts = self.term_structure();
        if ts.len() < 2 {
            return false;
        }
        ts.first().map(|(_, iv)| *iv).unwrap_or(0.0) > ts.last().map(|(_, iv)| *iv).unwrap_or(0.0)
    }

    /// Get term structure spread (front month - back month IV)
    pub fn term_spread(&self) -> Option<f64> {
        let ts = self.term_structure();
        if ts.len() < 2 {
            return None;
        }
        Some(ts.first()?.1 - ts.last()?.1)
    }
}

/// Live option chain fetcher
pub struct OptionChainFetcher {
    api_key: String,
    access_token: String,
    instruments_cache: HashMap<String, Vec<ZerodhaInstrument>>,
    client: reqwest::Client,
}

impl OptionChainFetcher {
    pub fn new(api_key: String, access_token: String) -> Self {
        Self {
            api_key,
            access_token,
            instruments_cache: HashMap::new(),
            client: reqwest::Client::new(),
        }
    }

    /// Fetch and cache NFO instruments
    pub async fn refresh_instruments(&mut self) -> anyhow::Result<()> {
        info!("Fetching NFO instruments from Zerodha...");
        
        let response = self.client.get(KITE_INSTRUMENTS_URL)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?
            .text()
            .await?;
        
        let mut instruments: HashMap<String, Vec<ZerodhaInstrument>> = HashMap::new();
        
        for line in response.lines().skip(1) {
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 12 {
                let inst = ZerodhaInstrument {
                    instrument_token: parts[0].parse().unwrap_or(0),
                    exchange_token: parts[1].parse().unwrap_or(0),
                    tradingsymbol: parts[2].to_string(),
                    name: parts[3].to_string(),
                    last_price: parts[4].parse().unwrap_or(0.0),
                    expiry: parts[5].to_string(),
                    strike: parts[6].parse().unwrap_or(0.0),
                    tick_size: parts[7].parse().unwrap_or(0.05),
                    lot_size: parts[8].parse().unwrap_or(1),
                    instrument_type: parts[9].to_string(),
                    segment: parts[10].to_string(),
                    exchange: parts[11].to_string(),
                };
                
                if inst.instrument_type == "CE" || inst.instrument_type == "PE" {
                    instruments.entry(inst.name.clone())
                        .or_default()
                        .push(inst);
                }
            }
        }
        
        info!("Cached {} underlyings with options", instruments.len());
        self.instruments_cache = instruments;
        Ok(())
    }

    /// Get option chain for an underlying and expiry
    pub fn get_chain(&self, underlying: &str, expiry: NaiveDate) -> Option<Vec<&ZerodhaInstrument>> {
        let expiry_str = expiry.format("%Y-%m-%d").to_string();
        
        self.instruments_cache.get(underlying).map(|instruments| {
            instruments.iter()
                .filter(|i| i.expiry == expiry_str)
                .collect()
        })
    }

    /// Fetch live quotes for option chain
    pub async fn fetch_chain_quotes(&self, instruments: &[&ZerodhaInstrument]) -> anyhow::Result<HashMap<u32, KiteQuote>> {
        if instruments.is_empty() {
            return Ok(HashMap::new());
        }
        
        // Build query params
        let params: Vec<(&str, String)> = instruments.iter()
            .map(|i| ("i", format!("NFO:{}", i.tradingsymbol)))
            .collect();
        
        let response = self.client.get(KITE_QUOTE_URL)
            .query(&params)
            .header("X-Kite-Version", "3")
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        
        let mut quotes = HashMap::new();
        
        if let Some(data) = response.get("data").and_then(|d| d.as_object()) {
            for (_key, value) in data {
                if let Ok(quote) = serde_json::from_value::<KiteQuote>(value.clone()) {
                    // Extract instrument token from response
                    if let Some(token) = value.get("instrument_token").and_then(|t| t.as_u64()) {
                        quotes.insert(token as u32, quote);
                    }
                }
            }
        }
        
        Ok(quotes)
    }

    /// Build IV surface from live data
    pub async fn build_iv_surface(
        &self,
        underlying: &str,
        spot_price: f64,
        expiry: NaiveDate,
        rate: f64,
    ) -> anyhow::Result<IVSurface> {
        let instruments = self.get_chain(underlying, expiry)
            .ok_or_else(|| anyhow::anyhow!("No instruments found for {}", underlying))?;
        
        let quotes = self.fetch_chain_quotes(&instruments).await?;
        
        let time_to_expiry = (expiry - Utc::now().date_naive()).num_days() as f64 / 365.0;
        
        let mut surface = IVSurface::new(underlying.to_string(), spot_price);
        
        for inst in instruments {
            if let Some(quote) = quotes.get(&inst.instrument_token) {
                let is_call = inst.instrument_type == "CE";
                
                if let Some(iv) = implied_volatility(
                    quote.last_price,
                    spot_price,
                    inst.strike,
                    time_to_expiry,
                    rate,
                    is_call,
                ) {
                    // Calculate delta
                    let delta = if is_call {
                        crate::greeks::OptionGreeks::for_call(spot_price, inst.strike, time_to_expiry, rate, iv).delta
                    } else {
                        crate::greeks::OptionGreeks::for_put(spot_price, inst.strike, time_to_expiry, rate, iv).delta
                    };
                    
                    surface.points.push(IVPoint {
                        strike: inst.strike,
                        expiry,
                        option_type: if is_call { OptionType::Call } else { OptionType::Put },
                        iv,
                        delta,
                        last_price: quote.last_price,
                    });
                }
            }
        }
        
        info!("Built IV surface with {} points for {} expiry {}", 
              surface.points.len(), underlying, expiry);
        
        Ok(surface)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iv_surface_skew() {
        let mut surface = IVSurface::new("NIFTY".to_string(), 25800.0);
        let expiry = NaiveDate::from_ymd_opt(2024, 12, 26).unwrap();
        
        // Add some test points
        surface.points.push(IVPoint {
            strike: 25600.0,
            expiry,
            option_type: OptionType::Put,
            iv: 0.14,
            delta: -0.3,
            last_price: 50.0,
        });
        
        surface.points.push(IVPoint {
            strike: 26000.0,
            expiry,
            option_type: OptionType::Call,
            iv: 0.12,
            delta: 0.3,
            last_price: 45.0,
        });
        
        let skew = surface.iv_skew(expiry, 200.0).unwrap();
        assert!((skew - 0.02).abs() < 0.001, "Skew should be 0.02: {}", skew);
    }
}
