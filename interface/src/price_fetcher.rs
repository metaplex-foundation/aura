use crate::error::UsecaseError;
use async_trait::async_trait;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;

#[async_trait]
pub trait PriceFetcher {
    async fn fetch_usd_token_price(&self, token_name: &str) -> Result<f64, UsecaseError>;
}

#[async_trait]
pub trait TokenPriceFetcher {
    async fn fetch_token_symbols(
        &self,
        token_ids: &[String],
    ) -> Result<HashMap<String, String>, UsecaseError>;
    async fn fetch_token_prices(
        &self,
        token_ids: &[String],
    ) -> Result<HashMap<String, f64>, UsecaseError>;
}
