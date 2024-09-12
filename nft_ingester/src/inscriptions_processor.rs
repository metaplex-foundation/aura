use entities::models::{InscriptionDataInfo, InscriptionInfo};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::errors::StorageError;
use solana_program::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Instant;
use usecase::save_metrics::result_to_metrics;

pub struct InscriptionsProcessor {
    metrics: Arc<IngesterMetricsConfig>,
}

impl InscriptionsProcessor {
    pub fn new(metrics: Arc<IngesterMetricsConfig>) -> Self {
        Self { metrics }
    }

    pub fn store_inscription(
        &self,
        storage: &mut BatchSaveStorage,
        inscription: &InscriptionInfo,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = storage.store_inscription(inscription);

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_merge_with_batch");
        self.metrics.set_latency(
            "inscriptions_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn store_inscription_data(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        inscription_data: &InscriptionDataInfo,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = storage.store_inscription_data(key, inscription_data);

        result_to_metrics(
            self.metrics.clone(),
            &res,
            "inscriptions_data_merge_with_batch",
        );
        self.metrics.set_latency(
            "inscriptions_data_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }
}
