use crate::buffer::Buffer;
use crate::mplx_updates_processor::IndexableAssetWithAccountInfo;
use crate::process_accounts;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::broadcast::Receiver;
use tracing::error;

#[derive(Clone)]
pub struct MplCoreFeeProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,

    last_received_mpl_asset_at: Option<SystemTime>,
}

impl MplCoreFeeProcessor {
    pub fn new(
        rocks_db: Arc<Storage>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
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
            self.buffer.mpl_core_indexable_assets,
            self.batch_size,
            |s: IndexableAssetWithAccountInfo| s,
            self.last_received_mpl_asset_at,
            Self::store_mpl_assets_fee,
            "burn_metadata"
        );
    }

    pub async fn store_mpl_assets_fee(
        &self,
        metadata_info: &HashMap<Pubkey, IndexableAssetWithAccountInfo>,
    ) {
        let metadata_models = match self.create_mpl_asset_models(metadata_info).await {
            Ok(metadata_models) => metadata_models,
            Err(e) => {
                error!("create_mpl_asset_models: {}", e);
                return;
            }
        };

        let begin_processing = Instant::now();
        self.rocks_db
            .store_metadata_models(&metadata_models, self.metrics.clone())
            .await;
        self.metrics.set_latency(
            "mpl_core_asset_fee",
            begin_processing.elapsed().as_millis() as f64,
        );
    }
}
