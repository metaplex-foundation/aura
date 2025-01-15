use async_trait::async_trait;
use entities::models::{BufferedTransaction, SignatureWithSlot};
use mockall::automock;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::error::UsecaseError;

#[automock]
#[async_trait]
pub trait TransactionsGetter: Send + Sync {
    /// Returns all the signatures for the given address, starting from the newest signatures and going backwards up to until.
    async fn get_signatures_by_address(
        &self,
        until: SignatureWithSlot,
        address: Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError>;

    async fn get_txs_by_signatures(
        &self,
        signatures: Vec<Signature>,
        retry_interval_millis: u64,
    ) -> Result<Vec<BufferedTransaction>, UsecaseError>;
}
