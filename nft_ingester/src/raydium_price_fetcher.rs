use crate::error::IngesterError;
use async_trait::async_trait;
use interface::error::UsecaseError;
use interface::price_fetcher::TokenPriceFetcher;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

pub struct RaydiumTokenPriceFetcher {
    host: String,
}

impl Default for RaydiumTokenPriceFetcher {
    fn default() -> Self {
        Self::new("https://api-v3.raydium.io".to_string())
    }
}

impl RaydiumTokenPriceFetcher {
    pub fn new(host: String) -> Self {
        Self { host }
    }

    async fn get(&self, endpoint: &str) -> Result<serde_json::Value, IngesterError> {
        reqwest::get(format!("{host}/{ep}", host = self.host, ep = endpoint))
            .await?
            .json()
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl TokenPriceFetcher for RaydiumTokenPriceFetcher {
    async fn fetch_token_symbols(
        &self,
        token_ids: &[Pubkey],
    ) -> Result<HashMap<String, String>, UsecaseError> {
        let token_ids = token_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let req = format!("mint/ids?mints={}", token_ids.join("%2C"));
        let response = self
            .get(&req)
            .await
            .map_err(|e| UsecaseError::Reqwest(e.to_string()))?;
        let Some(tokens_data) = response.get("data").and_then(|td| td.as_array()) else {
            return Err(UsecaseError::Reqwest(format!(
                "No 'data' field in RaydiumTokenPriceFetcher ids response. Full response: {:#?}",
                response
            )));
        };
        let mut res = HashMap::with_capacity(tokens_data.len());
        for data in tokens_data {
            let address = data.get("address").and_then(|a| a.as_str());
            let symbol = data.get("symbol").and_then(|s| s.as_str());
            address
                .zip(symbol)
                .map(|(address, symbol)| res.insert(address.to_string(), symbol.to_string()));
        }
        Ok(res)
    }

    async fn fetch_token_prices(
        &self,
        token_ids: &[Pubkey],
    ) -> Result<HashMap<String, f64>, UsecaseError> {
        let token_ids = token_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let req = format!("mint/price?mints={}", token_ids.join("%2C"));
        let response = self
            .get(&req)
            .await
            .map_err(|e| UsecaseError::Reqwest(e.to_string()))?;
        let Some(tokens_data) = response.get("data").and_then(|a| a.as_object()) else {
            return Err(UsecaseError::Reqwest(format!(
                "No 'data' field in RaydiumTokenPriceFetcher price response. Full response: {:#?}",
                response
            )));
        };
        let mut res = HashMap::with_capacity(tokens_data.len());
        for (key, value) in tokens_data {
            let price = value.as_str().and_then(|price| f64::from_str(price).ok());
            if let Some(price) = price {
                res.insert(key.clone(), price);
            }
        }
        Ok(res)
    }
}
