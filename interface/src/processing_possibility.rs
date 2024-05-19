use async_trait::async_trait;
use solana_program::pubkey::Pubkey;

#[async_trait]
pub trait ProcessingPossibilityChecker: Send + Sync + 'static {
    async fn can_process_assets(&self, keys: &[Pubkey]) -> bool;
}
