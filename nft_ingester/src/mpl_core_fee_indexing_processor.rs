use crate::buffer::Buffer;
use crate::error::IngesterError;
use crate::mplx_updates_processor::CoreAssetFee;
use crate::process_accounts;
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use entities::models::CoreFee;
use metrics_utils::IngesterMetricsConfig;
use mpl_core_program::state::{AssetV1, DataBlob, HashedAssetV1};
use mpl_core_program::utils::fetch_core_data;
use postgre_client::PgClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::account_info::AccountInfo;
use solana_program::pubkey::Pubkey;
use solana_program::rent::Rent;
use solana_program::sysvar::rent;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast::Receiver;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tracing::{error, info};

// in fact rent program constant almost all of the time, so we can update it 1 time per twenty-four hours
const FETCH_RENT_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

#[derive(Clone)]
pub struct MplCoreFeeProcessor {
    pub storage: Arc<PgClient>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
    rpc_client: Arc<RpcClient>,
    rent: Arc<RwLock<Rent>>,
    join_set: Arc<Mutex<JoinSet<Result<(), tokio::task::JoinError>>>>,

    last_received_mpl_asset_at: Option<SystemTime>,
}

impl MplCoreFeeProcessor {
    pub async fn build(
        storage: Arc<PgClient>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        rpc_client: Arc<RpcClient>,
        join_set: Arc<Mutex<JoinSet<Result<(), tokio::task::JoinError>>>>,
        batch_size: usize,
    ) -> Result<Self, IngesterError> {
        let rent_account = rpc_client.get_account(&rent::ID).await?;
        let rent: Rent = bincode::deserialize(&rent_account.data)?;
        Ok(Self {
            storage,
            buffer,
            batch_size,
            metrics,
            rpc_client,
            last_received_mpl_asset_at: None,
            rent: Arc::new(RwLock::new(rent)),
            join_set,
        })
    }

    pub async fn start_processing(&mut self, rx: Receiver<()>) {
        self.update_rent(rx.resubscribe()).await;
        process_accounts!(
            self,
            rx,
            self.buffer.mpl_core_fee_assets,
            self.batch_size,
            |s: CoreAssetFee| s,
            self.last_received_mpl_asset_at,
            Self::store_mpl_assets_fee,
            "mpl_core_asset_fee"
        );
    }

    // on-chain programs can fetch rent without RPC call
    // but off-chain indexer need to make such calls in order
    // to get actual rent data
    async fn update_rent(&self, mut rx: Receiver<()>) {
        let rpc_client = self.rpc_client.clone();
        let rent = self.rent.clone();
        self.join_set.lock().await.spawn(tokio::spawn(async move {
            while rx.is_empty() {
                if let Err(e) = Self::fetch_actual_rent(rpc_client.clone(), rent.clone()).await {
                    error!("fetch_actual_rent: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                tokio::select! {
                    _ = rx.recv() => {
                        info!("Received stop signal, stopping update_rent...");
                        return;
                    }
                    _ = tokio::time::sleep(FETCH_RENT_INTERVAL) => {},
                }
            }
        }));
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

    pub async fn store_mpl_assets_fee(&self, metadata_info: &HashMap<Pubkey, CoreAssetFee>) {
        let mut fees = Vec::new();
        for (pk, asset) in metadata_info.iter() {
            let rent = match self.calculate_rent_amount(*pk, asset).await {
                Ok(rent) => rent,
                Err(err) => {
                    error!("calculate_rent_amount: {:?}", err);
                    continue;
                }
            };

            fees.push(CoreFee {
                pubkey: *pk,
                is_paid: asset.lamports < rent,
                current_balance: asset.lamports,
                minimum_rent: rent,
                slot_updated: asset.slot_updated,
            });
        }

        let begin_processing = Instant::now();
        if let Err(err) = self.storage.save_core_fees(fees).await {
            error!("save_core_fees: {}", err);
        };
        self.metrics.set_latency(
            "mpl_core_asset_fee",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    async fn calculate_rent_amount(
        &self,
        key: Pubkey,
        account_info: &CoreAssetFee,
    ) -> Result<u64, IngesterError> {
        let rent_amount = match &account_info.indexable_asset {
            MplCoreAccountData::EmptyAccount => self.rent.read().await.minimum_balance(1),
            MplCoreAccountData::Asset(_) => {
                let (asset, header, registry) = fetch_core_data::<AssetV1>(&AccountInfo::new(
                    &key,
                    false,
                    false,
                    &mut account_info.lamports.clone(),
                    account_info.data.clone().borrow_mut(),
                    &mpl_core_program::id(),
                    false,
                    account_info.rent_epoch,
                ))?;
                let header_size = match header {
                    Some(header) => header.get_size(),
                    None => 0,
                };
                let registry_size = match registry {
                    Some(registry) => registry.get_size(),
                    None => 0,
                };
                self.rent.read().await.minimum_balance(
                    asset
                        .get_size()
                        .checked_add(header_size)
                        .ok_or(IngesterError::NumericalOverflowError)?
                        .checked_add(registry_size)
                        .ok_or(IngesterError::NumericalOverflowError)?,
                )
            }
            MplCoreAccountData::HashedAsset => self
                .rent
                .read()
                .await
                .minimum_balance(HashedAssetV1::LENGTH),
            _ => return Err(IngesterError::IncorrectAccount),
        };

        Ok(rent_amount)
    }
}
