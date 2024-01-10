use crate::bubblegum_updates_processor::BubblegumTxProcessor;
use entities::models::BufferedTransaction;
use interface::error::StorageError;
use interface::signature_persistence::TransactionIngester;
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
            .process_transaction(Some(tx))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))
    }
}
