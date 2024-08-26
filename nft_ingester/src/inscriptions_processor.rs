use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast::Receiver;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::Storage;
use crate::buffer::Buffer;
use crate::error::IngesterError;
use crate::process_accounts;

#[derive(Clone)]
pub struct InscriptionsProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,

    last_received_inscription_at: Option<SystemTime>,
    last_received_inscription_data_at: Option<SystemTime>,
}

impl InscriptionsProcessor {
    pub async fn new(
        rocks_db: Arc<Storage>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Result<Self, IngesterError> {
        Ok(Self {
            rocks_db,
            buffer,
            batch_size,
            metrics,
            last_received_inscription_at: None,
            last_received_inscription_data_at: None,
        })
    }

    pub async fn start_processing(&mut self, rx: Receiver<()>) {
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
            let rent = match self.calculate_rent_amount(asset).await {
                Ok(rent) => rent,
                Err(err) => {
                    error!("calculate_rent_amount: {:?}", err);
                    continue;
                }
            };

            fees.push(CoreFee {
                pubkey: *pk,
                is_paid: asset.lamports <= rent,
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
        account_info: &CoreAssetFee,
    ) -> Result<u64, IngesterError> {
        Ok(self
            .rent
            .read()
            .await
            .minimum_balance(account_info.data.len()))
    }
}
