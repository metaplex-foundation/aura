use crate::Storage;
use async_trait::async_trait;
use interface::processing_possibility::ProcessingPossibilityChecker;
use solana_sdk::pubkey::Pubkey;

#[async_trait]
impl ProcessingPossibilityChecker for Storage {
    async fn can_process_assets(&self, pubkeys: Vec<Pubkey>) -> bool {
        if pubkeys.is_empty() {
            return true;
        }
        let trees = match self.asset_leaf_data.batch_get(pubkeys).await {
            Ok(asset_leafs) => asset_leafs
                .into_iter()
                .flat_map(|leaf| leaf.map(|l| l.tree_id))
                .collect::<Vec<_>>(),
            // Some troubles with DB, so we cannot handle requests
            Err(_) => return false,
        };
        let gaps = match self.trees_gaps.batch_get(trees).await {
            Ok(gaps) => gaps,
            // Some troubles with DB, so we cannot handle requests
            Err(_) => return false,
        };
        // If there at least 1 gap, we cannot handle this request
        for gap in gaps.iter() {
            if gap.is_some() {
                return false;
            }
        }
        true
    }
}
