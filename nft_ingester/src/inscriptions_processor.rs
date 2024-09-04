use entities::models::{InscriptionDataInfo, InscriptionInfo};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::errors::StorageError;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Instant;
use usecase::save_metrics::result_to_metrics;

#[derive(Clone)]
pub struct InscriptionsProcessor {
    pub rocks_db: Arc<Storage>,
    pub metrics: Arc<IngesterMetricsConfig>,
}

impl InscriptionsProcessor {
    pub fn new(rocks_db: Arc<Storage>, metrics: Arc<IngesterMetricsConfig>) -> Self {
        Self { rocks_db, metrics }
    }

    pub fn store_inscription(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        inscription: &InscriptionInfo,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = self.rocks_db.inscriptions.merge_with_batch(
            db_batch,
            inscription.inscription.root,
            &inscription.into(),
        );

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_saving");
        self.metrics.set_latency(
            "inscriptions_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn store_inscription_data(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: Pubkey,
        inscription_data: &InscriptionDataInfo,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = self.rocks_db.inscription_data.merge_with_batch(
            db_batch,
            key,
            &rocks_db::inscriptions::InscriptionData {
                pubkey: key,
                data: inscription_data.inscription_data.clone(),
                write_version: inscription_data.write_version,
            },
        );

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_data_saving");
        self.metrics.set_latency(
            "inscriptions_data_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }
}
