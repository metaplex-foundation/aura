use entities::models::{InscriptionDataInfo, InscriptionInfo};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::errors::StorageError;
use solana_program::pubkey::Pubkey;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use usecase::save_metrics::result_to_metrics;

pub struct InscriptionsProcessor {
    storage: Rc<RefCell<BatchSaveStorage>>,
    metrics: Arc<IngesterMetricsConfig>,
}

impl InscriptionsProcessor {
    pub fn new(
        storage: Rc<RefCell<BatchSaveStorage>>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Self {
        Self { storage, metrics }
    }

    pub fn store_inscription(&mut self, inscription: &InscriptionInfo) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = self.storage.borrow_mut().store_inscription(inscription);

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_saving");
        self.metrics.set_latency(
            "inscriptions_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn store_inscription_data(
        &mut self,
        key: Pubkey,
        inscription_data: &InscriptionDataInfo,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = self
            .storage
            .borrow_mut()
            .store_inscription_data(key, inscription_data);

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_data_saving");
        self.metrics.set_latency(
            "inscriptions_data_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }
}
