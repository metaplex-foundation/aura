use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use entities::models::{CoreAssetFee, CoreFee};
use metrics_utils::IngesterMetricsConfig;
use postgre_client::PgClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::{pubkey::Pubkey, rent::Rent, sysvar::rent};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::error::IngesterError;

// in fact rent program constant almost all of the time, so we can update it 1 time per twenty-four hours
const FETCH_RENT_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

#[derive(Clone)]
pub struct MplCoreFeeProcessor {
    pub storage: Arc<PgClient>,
    pub metrics: Arc<IngesterMetricsConfig>,
    rpc_client: Arc<RpcClient>,
    rent: Arc<RwLock<Rent>>,
}

impl MplCoreFeeProcessor {
    pub async fn build(
        storage: Arc<PgClient>,
        metrics: Arc<IngesterMetricsConfig>,
        rpc_client: Arc<RpcClient>,
    ) -> Result<Self, IngesterError> {
        let rent_account = rpc_client.get_account(&rent::ID).await?;
        let rent: Rent = bincode::deserialize(&rent_account.data)?;
        Ok(Self { storage, metrics, rpc_client, rent: Arc::new(RwLock::new(rent)) })
    }

    // on-chain programs can fetch rent without RPC call
    // but off-chain indexer need to make such calls in order
    // to get actual rent data
    pub fn update_rent(&self, cancellation_token: CancellationToken) {
        let rpc_client = self.rpc_client.clone();
        let rent = self.rent.clone();
        usecase::executor::spawn(async move {
            loop {
                if let Err(e) = Self::fetch_actual_rent(rpc_client.clone(), rent.clone()).await {
                    error!("fetch_actual_rent: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                tokio::select! {
                    _ = cancellation_token.cancelled() => { break; }
                    _ = tokio::time::sleep(FETCH_RENT_INTERVAL) => {}
                }
            }
        });
    }

    async fn fetch_actual_rent(
        rpc_client: Arc<RpcClient>,
        rent: Arc<RwLock<Rent>>,
    ) -> Result<(), IngesterError> {
        let rent_account = rpc_client.get_account(&rent::ID).await?;
        let actual_rent: Rent = bincode::deserialize(&rent_account.data)?;
        let mut rent = rent.write().await;
        *rent = actual_rent;

        Ok(())
    }

    pub async fn store_mpl_assets_fee(&self, asset_fees: &HashMap<Pubkey, CoreAssetFee>) {
        let mut fees = Vec::new();
        for (pk, asset) in asset_fees.iter() {
            let rent = match self.calculate_rent_amount(asset).await {
                Ok(rent) => rent,
                Err(err) => {
                    error!("calculate_rent_amount: {:?}", err);
                    continue;
                },
            };

            fees.push(CoreFee {
                pubkey: *pk,
                is_paid: asset.lamports <= rent,
                current_balance: asset.lamports,
                minimum_rent: rent,
                slot_updated: asset.slot_updated,
            });
        }

        fees.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));

        let begin_processing = Instant::now();
        if let Err(err) = self.storage.save_core_fees(fees).await {
            error!("save_core_fees: {}", err);
        };
        self.metrics
            .set_latency("mpl_core_asset_fee", begin_processing.elapsed().as_millis() as f64);
    }

    async fn calculate_rent_amount(
        &self,
        account_info: &CoreAssetFee,
    ) -> Result<u64, IngesterError> {
        Ok(self.rent.read().await.minimum_balance(account_info.data.len()))
    }
}
