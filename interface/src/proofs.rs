use async_trait::async_trait;
use mockall::automock;
use solana_program::pubkey::Pubkey;

#[automock]
#[async_trait]
pub trait ProofChecker {
    async fn check_proof(
        &self,
        tree_id_pk: Pubkey,
        initial_proofs: Vec<Pubkey>,
        leaf_index: u32,
        leaf: [u8; 32],
    ) -> Result<bool, String>;
}
