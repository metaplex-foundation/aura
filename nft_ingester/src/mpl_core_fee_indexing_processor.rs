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
use solana_program::account_info::AccountInfo;
use solana_program::pubkey::Pubkey;
use solana_program::rent::Rent;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::broadcast::Receiver;
use tracing::error;

#[derive(Clone)]
pub struct MplCoreFeeProcessor {
    pub storage: Arc<PgClient>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,

    last_received_mpl_asset_at: Option<SystemTime>,
}

impl MplCoreFeeProcessor {
    pub fn new(
        storage: Arc<PgClient>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            storage,
            buffer,
            batch_size,
            metrics,
            last_received_mpl_asset_at: None,
        }
    }

    pub async fn process(&mut self, rx: Receiver<()>) {
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

    pub async fn store_mpl_assets_fee(&self, metadata_info: &HashMap<Pubkey, CoreAssetFee>) {
        let mut fees = Vec::new();
        for (pk, asset) in metadata_info.iter() {
            let rent = match Self::calculate_rent_amount(*pk, asset) {
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

    fn calculate_rent_amount(
        key: Pubkey,
        account_info: &CoreAssetFee,
    ) -> Result<u64, IngesterError> {
        let rent = Rent::default();
        let rent_amount = match &account_info.indexable_asset {
            MplCoreAccountData::EmptyAccount => rent.minimum_balance(1),
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
                rent.minimum_balance(
                    asset
                        .get_size()
                        .checked_add(header_size)
                        .ok_or(IngesterError::NumericalOverflowError)?
                        .checked_add(registry_size)
                        .ok_or(IngesterError::NumericalOverflowError)?,
                )
            }
            MplCoreAccountData::HashedAsset => rent.minimum_balance(HashedAssetV1::LENGTH),
            _ => return Err(IngesterError::IncorrectAccount),
        };

        Ok(rent_amount)
    }
}
