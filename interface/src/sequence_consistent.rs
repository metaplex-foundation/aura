use async_trait::async_trait;
use entities::models::TreeState;
use solana_program::pubkey::Pubkey;

#[async_trait]
pub trait SequenceConsistentManager {
    fn tree_sequence_iter(&self) -> impl Iterator<Item = TreeState>;
    fn gaps_count(&self) -> i64;
    async fn process_tree_gap(&self, tree: Pubkey, gap_found: bool);
    async fn all_processed_reingestable_slots(&self) -> Vec<u64>;
    async fn manage_ingestable_slots(&self, processed_reingestable_slots: Vec<u64>);
}
