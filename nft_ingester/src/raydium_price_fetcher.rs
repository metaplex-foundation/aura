use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use interface::{error::UsecaseError, price_fetcher::TokenPriceFetcher};
use metrics_utils::red::RequestErrorDurationMetrics;
use moka::future::Cache;

use crate::error::IngesterError;

pub const CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

pub struct RaydiumTokenPriceFetcher {
    host: String,
    price_cache: Cache<String, f64>,
    symbol_cache: Cache<String, String>,
    red_metrics: Option<Arc<RequestErrorDurationMetrics>>,
}

impl Default for RaydiumTokenPriceFetcher {
    fn default() -> Self {
        Self::new(crate::consts::RAYDIUM_API_HOST.to_string(), CACHE_TTL, None)
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

    pub async fn warmup(&self) -> Result<(), IngesterError> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MintListItem {
            address: String,
            symbol: String,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MintListResponse {
            mint_list: Vec<MintListItem>,
        }
        // returns well-known token infos
        let req = "mint/list";
        let response = self.get(req).await.map_err(|e| UsecaseError::Reqwest(e.to_string()))?;

        let tokens_data = response
            .get("data")
            .and_then(|mint_list| {
                serde_json::from_value::<MintListResponse>(mint_list.clone()).ok()
            })
            .ok_or_else(|| {
                UsecaseError::Reqwest(format!(
                "No 'data' field in RaydiumTokenPriceFetcher ids response. Full response: {:#?}",
                response
            ))
            })?;

        for MintListItem { address, symbol } in tokens_data.mint_list {
            self.symbol_cache.insert(address.clone(), symbol.clone()).await;
        }

        self.symbol_cache.run_pending_tasks().await;

        Ok(())
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

    /// Returns the approximate sizes of the symbol and the price caches.
    ///
    /// The return format is (symbol_cache_size, price_cache_size).
    pub fn get_cache_sizes(&self) -> (u64, u64) {
        (self.symbol_cache.weighted_size(), self.price_cache.weighted_size())
    }
}

#[async_trait]
impl TokenPriceFetcher for RaydiumTokenPriceFetcher {
    async fn fetch_token_symbols(
        &self,
        token_ids: &[String],
    ) -> Result<HashMap<String, String>, UsecaseError> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MintIdsItem {
            address: String,
            symbol: String,
        }
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
            let response =
                self.get(&req).await.map_err(|e| UsecaseError::Reqwest(e.to_string()))?;

            let tokens_data = response
                .get("data")
                .and_then(|item| serde_json::from_value::<Vec<Option<MintIdsItem>>>(item.clone()).ok())
                .ok_or_else(|| {
                    UsecaseError::Reqwest(format!(
                        "No 'data' field in RaydiumTokenPriceFetcher ids response. Full response: {:#?}",
                        response
                    ))
                })?;

            for MintIdsItem { address, symbol } in tokens_data.into_iter().flatten() {
                self.symbol_cache.insert(address.clone(), symbol.clone()).await;
                result.insert(address, symbol);
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
            let response =
                self.get(&req).await.map_err(|e| UsecaseError::Reqwest(e.to_string()))?;

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
