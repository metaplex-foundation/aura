use crate::error::UsecaseError;
use async_trait::async_trait;

#[async_trait]
pub trait PriceFetcher {
    async fn fetch_usd_token_price(&self, token_name: &str) -> Result<f64, UsecaseError>;
}
