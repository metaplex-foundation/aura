use crate::processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor;
use entities::models::BufferedTransaction;
use interface::{error::StorageError, signature_persistence::TransactionIngester};
use rocks_db::transaction::{TransactionProcessor, TransactionResult};
use std::sync::Arc;
use tonic::async_trait;

pub struct BackfillTransactionIngester {
    pub tx_processor: Arc<BubblegumTxProcessor>,
}

impl BackfillTransactionIngester {
    pub fn new(tx_processor: Arc<BubblegumTxProcessor>) -> Self {
        Self { tx_processor }
    }
}
#[async_trait]
impl TransactionIngester for BackfillTransactionIngester {
    async fn ingest_transaction(&self, tx: BufferedTransaction) -> Result<(), StorageError> {
        self.tx_processor
            .process_transaction(tx)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))
    }
}

#[async_trait]
impl TransactionProcessor for BackfillTransactionIngester {
    fn get_ingest_transaction_results(
        &self,
        tx: BufferedTransaction,
    ) -> Result<TransactionResult, StorageError> {
        BubblegumTxProcessor::get_process_transaction_results(
            tx,
            self.tx_processor.instruction_parser.clone(),
            self.tx_processor.transaction_parser.clone(),
            self.tx_processor.metrics.clone(),
        )
        .map_err(|e| StorageError::Common(e.to_string()))
    }
}
