use std::sync::Arc;

use entities::models::{BufferedTransaction, TransactionInfo};
use interface::{error::StorageError, signature_persistence::TransactionIngester};
use rocks_db::transaction::{TransactionProcessor, TransactionResult};
use tonic::async_trait;

use crate::processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor;

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
    // called only from the signatures fetcher at the moment, as it's switched to fetch finalized signatures only it's safe to assume the source is finalized
    async fn ingest_transaction(&self, tx: BufferedTransaction) -> Result<(), StorageError> {
        self.tx_processor
            .process_transaction(tx, true)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))
    }
}

#[async_trait]
impl TransactionProcessor for BackfillTransactionIngester {
    fn get_ingest_transaction_results(
        &self,
        tx: TransactionInfo,
    ) -> Result<TransactionResult, StorageError> {
        BubblegumTxProcessor::get_handle_transaction_results(
            self.tx_processor.instruction_parser.clone(),
            tx,
            self.tx_processor.metrics.clone(),
        )
        .map_err(|e| StorageError::Common(e.to_string()))
    }
}
