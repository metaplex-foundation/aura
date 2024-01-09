use crate::error::UsecaseError;
use async_trait::async_trait;
use entities::models::BufferedTransaction;
use mockall::automock;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;

#[allow(dead_code)]
pub struct SignatureWithSlot {
    pub signature: Signature,
    pub slot: u64,
}

#[automock]
#[async_trait]
pub trait GetSignaturesByAddress: Send + Sync {
    async fn get_signatures_by_address(
        &self,
        signature: Signature,
        program_id: Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError>;
}

#[automock]
#[async_trait]
pub trait GetTransactionsBySignatures: Send + Sync {
    async fn get_txs_by_signatures(
        &self,
        signatures: Vec<Signature>,
    ) -> Result<Vec<BufferedTransaction>, UsecaseError>;
}
