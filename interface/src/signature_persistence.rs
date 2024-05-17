use std::sync::Arc;

use crate::error::{BlockConsumeError, StorageError};
use async_trait::async_trait;
use entities::models::{BufferedTransaction, SignatureWithSlot};
use mockall::automock;
use solana_sdk::pubkey::Pubkey;

#[automock]
#[async_trait]
pub trait SignaturePersistence {
    async fn first_persisted_signature_for(
        &self,
        program_id: Pubkey,
    ) -> Result<Option<SignatureWithSlot>, StorageError>;

    async fn drop_signatures_before(
        &self,
        program_id: Pubkey,
        signature: SignatureWithSlot,
    ) -> Result<(), StorageError>;

    async fn missing_signatures(
        &self,
        program_id: Pubkey,
        signatures: Vec<SignatureWithSlot>,
    ) -> Result<Vec<SignatureWithSlot>, StorageError>;
}

#[automock]
#[async_trait]
pub trait TransactionIngester: Sync + Send + 'static {
    /// Ingests a transaction into the storage layer.
    /// The transaction is expected to be in the format of a flatbuffer.
    /// The ingester should return only after the transaction is fully processed, not just scheduled.
    async fn ingest_transaction(&self, tx: BufferedTransaction) -> Result<(), StorageError>;
}
#[async_trait]
pub trait ProcessingDataGetter {
    async fn get_processing_transaction(&self) -> Option<BufferedTransaction>;
}

// FIXME: Replace String error with more meningful one
#[async_trait]
pub trait BlockConsumer: Send + Sync + 'static {
    async fn consume_block(
        &self,
        slot: u64,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> Result<(), BlockConsumeError>;
    async fn already_processed_slot(&self, slot: u64) -> Result<bool, BlockConsumeError>;
}

// TODO-XXX: is StorageError is sufficient type to cover all possible problems?
#[async_trait]
pub trait BlockProducer: Send + Sync + 'static {
    async fn get_block(
        &self,
        slot: u64,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<solana_transaction_status::UiConfirmedBlock, StorageError>;
}
