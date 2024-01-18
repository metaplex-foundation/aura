use crate::error::StorageError;
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

#[async_trait]
pub trait BlockConsumer: Send + Sync + 'static {
    async fn consume_block(
        &self,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> Result<(), String>;
    async fn already_processed_slot(&self, slot: u64) -> Result<bool, String>;
}

#[async_trait]
pub trait BlockProducer: Send + Sync + 'static {
    async fn get_block(
        &self,
        slot: u64,
    ) -> Result<solana_transaction_status::UiConfirmedBlock, StorageError>;
}
