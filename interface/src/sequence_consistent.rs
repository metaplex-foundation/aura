use async_trait::async_trait;
use entities::models::TreeState;
use solana_program::pubkey::Pubkey;

#[async_trait]
pub trait SequenceConsistentManager {
    fn tree_sequence_iter(&self) -> impl Iterator<Item = TreeState>;
    fn gaps_count(&self) -> i64;
    async fn process_tree_gap(&self, tree: Pubkey, gap_found: bool);
    async fn get_last_ingested_slot(&self) -> Result<Option<u64>, String>;
}
