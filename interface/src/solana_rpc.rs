use crate::error::UsecaseError;
use async_trait::async_trait;
use entities::models::BufferedTransaction;
use mockall::automock;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

//todo: move it to entities
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignatureWithSlot {
    pub signature: Signature,
    pub slot: u64,
}

#[automock]
#[async_trait]
pub trait GetBackfillTransactions: Send + Sync {
    async fn get_signatures_by_address(
        &self,
        until: Signature,
        before: Option<Signature>,
        address: Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError>;
    async fn get_txs_by_signatures(&self, signatures: Vec<Signature>) -> Vec<BufferedTransaction>;
}
