use crate::{error::StorageError, solana_rpc::SignatureWithSlot};
use async_trait::async_trait;
use entities::models::BufferedTransaction;
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
pub trait TransactionIngester {
    async fn ingest_transaction(&self, tx: BufferedTransaction) -> Result<(), StorageError>;
}
