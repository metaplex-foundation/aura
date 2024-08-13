use crate::error::IngesterError;
use async_trait::async_trait;
use coingecko::CoinGeckoClient;
use interface::error::UsecaseError;
use interface::price_fetcher::PriceFetcher;
use log::error;
use rocks_db::token_prices::TokenPrice;
use rocks_db::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;

pub const SOLANA_CURRENCY: &str = "solana";
const USD_CURRENCY: &str = "usd";
const PRICE_MONITORING_INTERVAL_SEC: u64 = 30;

pub struct CoinGeckoPriceFetcher {
    client: CoinGeckoClient,
}

impl CoinGeckoPriceFetcher {
    pub fn new() -> Self {
        Self {
            client: CoinGeckoClient::default(),
        }
    }
}

impl Default for CoinGeckoPriceFetcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PriceFetcher for CoinGeckoPriceFetcher {
    async fn fetch_usd_token_price(&self, token_name: &str) -> Result<f64, UsecaseError> {
        let price = self
            .client
            .price(&[token_name], &[USD_CURRENCY], false, false, false, false)
            .await?
            .get(token_name)
            .ok_or(UsecaseError::EmptyPriceFetcherResponse(
                token_name.to_string(),
            ))?
            .usd
            .ok_or(UsecaseError::EmptyPriceFetcherResponse(
                USD_CURRENCY.to_string(),
            ))?;

        Ok(price)
    }
}

pub struct SolanaPriceUpdater<P: PriceFetcher> {
    price_fetcher: P,
    rocks_db: Arc<Storage>,
}

impl<P: PriceFetcher> SolanaPriceUpdater<P> {
    pub fn new(rocks_db: Arc<Storage>, price_fetcher: P) -> Self {
        Self {
            price_fetcher,
            rocks_db,
        }
    }

    pub async fn start_price_monitoring(&self, mut rx: Receiver<()>) {
        while rx.is_empty() {
            if let Err(e) = self.update_price().await {
                error!("update_price: {e}");
            }
            tokio::select! {
                _ = rx.recv() => {
                    return;
                }
                _ = tokio::time::sleep(Duration::from_secs(PRICE_MONITORING_INTERVAL_SEC)) => {}
            }
        }
    }

    pub async fn update_price(&self) -> Result<(), IngesterError> {
        let price = self
            .price_fetcher
            .fetch_usd_token_price(SOLANA_CURRENCY)
            .await?;
        self.rocks_db
            .token_prices
            .put_async(SOLANA_CURRENCY.to_string(), TokenPrice { price })
            .await?;

        Ok(())
    }
}
