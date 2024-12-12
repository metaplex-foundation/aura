use crate::error::IngesterError;
use async_trait::async_trait;
use interface::error::UsecaseError;
use interface::price_fetcher::TokenPriceFetcher;
use metrics_utils::red::RequestErrorDurationMetrics;
use moka::future::Cache;
use std::collections::HashMap;
use std::sync::Arc;

pub const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

pub struct RaydiumTokenPriceFetcher {
    host: String,
    price_cache: Cache<String, f64>,
    symbol_cache: Cache<String, String>,
    red_metrics: Option<Arc<RequestErrorDurationMetrics>>,
}

impl Default for RaydiumTokenPriceFetcher {
    fn default() -> Self {
        Self::new("https://api-v3.raydium.io".to_string(), CACHE_TTL, None)
    }
}

impl RaydiumTokenPriceFetcher {
    pub fn new(
        host: String,
        ttl: std::time::Duration,
        red_metrics: Option<Arc<RequestErrorDurationMetrics>>,
    ) -> Self {
        Self {
            host,
            price_cache: Cache::builder().time_to_live(ttl).build(),
            symbol_cache: Cache::builder().time_to_live(ttl).build(),
            red_metrics,
        }
    }

    async fn get(&self, endpoint: &str) -> Result<serde_json::Value, IngesterError> {
        let start_time = chrono::Utc::now();
        let response = reqwest::get(format!("{host}/{ep}", host = self.host, ep = endpoint))
            .await?
            .json()
            .await
            .map_err(Into::into);
        if let Some(red_metrics) = &self.red_metrics {
            // cut the part after ? in the endpoint for metrics
            let endpoint = endpoint.split('?').next().unwrap_or(endpoint);
            match &response {
                Ok(_) => red_metrics.observe_request("raydium", "get", endpoint, start_time),
                Err(_) => red_metrics.observe_error("raydium", "get", endpoint),
            }
        }
        response
    }
}

#[async_trait]
impl TokenPriceFetcher for RaydiumTokenPriceFetcher {
    async fn fetch_token_symbols(
        &self,
        token_ids: &[String],
    ) -> Result<HashMap<String, String>, UsecaseError> {
        let token_ids_str: Vec<String> = token_ids.iter().map(ToString::to_string).collect();
        let mut result = HashMap::with_capacity(token_ids.len());
        let mut missing_token_ids = Vec::new();

        for token_id in &token_ids_str {
            if let Some(symbol) = self.symbol_cache.get(token_id).await {
                result.insert(token_id.clone(), symbol);
            } else {
                missing_token_ids.push(token_id.clone());
            }
        }

        if !missing_token_ids.is_empty() {
            let req = format!("mint/ids?mints={}", missing_token_ids.join("%2C"));
            let response = self
                .get(&req)
                .await
                .map_err(|e| UsecaseError::Reqwest(e.to_string()))?;

            let tokens_data = response
                .get("data")
                .and_then(|td| td.as_array())
                .ok_or_else(|| {
                    UsecaseError::Reqwest(format!(
                        "No 'data' field in RaydiumTokenPriceFetcher ids response. Full response: {:#?}",
                        response
                    ))
                })?;

            for data in tokens_data {
                if let (Some(address), Some(symbol)) = (
                    data.get("address").and_then(|a| a.as_str()),
                    data.get("symbol").and_then(|s| s.as_str()),
                ) {
                    let address = address.to_string();
                    let symbol = symbol.to_string();
                    self.symbol_cache
                        .insert(address.clone(), symbol.clone())
                        .await;
                    result.insert(address, symbol);
                }
            }
        }

        Ok(result)
    }

    async fn fetch_token_prices(
        &self,
        token_ids: &[String],
    ) -> Result<HashMap<String, f64>, UsecaseError> {
        let token_ids_str: Vec<String> = token_ids.iter().map(ToString::to_string).collect();
        let mut result = HashMap::with_capacity(token_ids.len());
        let mut missing_token_ids = Vec::new();

        for token_id in &token_ids_str {
            if let Some(price) = self.price_cache.get(token_id).await {
                result.insert(token_id.clone(), price);
            } else {
                missing_token_ids.push(token_id.clone());
            }
        }

        if !missing_token_ids.is_empty() {
            let req = format!("mint/price?mints={}", missing_token_ids.join("%2C"));
            let response = self
                .get(&req)
                .await
                .map_err(|e| UsecaseError::Reqwest(e.to_string()))?;

            let tokens_data = response
                .get("data")
                .and_then(|a| a.as_object())
                .ok_or_else(|| {
                    UsecaseError::Reqwest(format!(
                        "No 'data' field in RaydiumTokenPriceFetcher price response. Full response: {:#?}",
                        response
                    ))
                })?;

            for (key, value) in tokens_data {
                if let Some(price_str) = value.as_str() {
                    if let Ok(price) = price_str.parse::<f64>() {
                        self.price_cache.insert(key.clone(), price).await;
                        result.insert(key.clone(), price);
                    }
                }
            }
        }

        Ok(result)
    }
}
