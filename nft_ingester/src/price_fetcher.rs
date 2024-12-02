use crate::error::IngesterError;
use async_trait::async_trait;
use coingecko::CoinGeckoClient;
use interface::error::UsecaseError;
use interface::price_fetcher::PriceFetcher;
use rocks_db::token_prices::TokenPrice;
use rocks_db::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinError;
use tracing::log::{error, info};

pub const SOLANA_CURRENCY: &str = "solana";
const USD_CURRENCY: &str = "usd";

// Not used now but can be useful in future
pub async fn _start_price_monitoring<T: PriceFetcher>(
    solana_price_updater: SolanaPriceUpdater<T>,
    rx: Receiver<()>,
) -> Result<(), JoinError> {
    info!("Start monitoring Solana price...");
    solana_price_updater.start_price_monitoring(rx).await;
    info!("Stop monitoring Solana price...");
    Ok(())
}

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
    price_monitoring_interval: Duration,
}

impl<P: PriceFetcher> SolanaPriceUpdater<P> {
    pub fn new(
        rocks_db: Arc<Storage>,
        price_fetcher: P,
        price_monitoring_interval_sec: u64,
    ) -> Self {
        Self {
            price_fetcher,
            rocks_db,
            price_monitoring_interval: Duration::from_secs(price_monitoring_interval_sec),
        }
    }

    pub async fn start_price_monitoring(&self, mut rx: Receiver<()>) {
        tokio::select! {
            _ = async move {
                if let Err(e) = self.update_price().await {
                    error!("update_price: {e}");
                };
                tokio::time::sleep(self.price_monitoring_interval).await;
            } => {}
            _ = rx.recv() => {
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
