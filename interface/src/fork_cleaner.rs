use std::collections::HashSet;

use async_trait::async_trait;
use entities::models::{ClItem, ForkedItem, LeafSignatureAllData};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait CompressedTreeChangesManager {
    fn tree_seq_idx_iter(&self) -> impl Iterator<Item = LeafSignatureAllData>;
    fn cl_items_iter(&self) -> impl Iterator<Item = ClItem>;
    async fn delete_tree_seq_idx(&self, keys: Vec<ForkedItem>);
    async fn delete_cl_items(&self, keys: Vec<ForkedItem>);
    async fn delete_signatures(&self, keys: Vec<(Signature, Pubkey, u64)>);
}

#[async_trait]
pub trait ForkChecker {
    fn get_all_non_forked_slots(&self, cancellation_token: CancellationToken) -> HashSet<u64>;
    fn last_slot_for_check(&self) -> u64;
}
