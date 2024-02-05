use crate::bubblegum_slots::IngestedSlots;
use crate::column::TypedColumn;
use crate::tree_seq::{TreeSeqIdx, TreesGaps};
use crate::{key_encoders, Storage};
use async_trait::async_trait;
use entities::models::TreeState;
use interface::sequence_consistent::SequenceConsistentManager;
use solana_sdk::pubkey::Pubkey;
use tracing::error;

#[async_trait]
impl SequenceConsistentManager for Storage {
    fn tree_sequence_iter(&self) -> impl Iterator<Item = TreeState> {
        self.tree_seq_idx
            .iter_start()
            .filter_map(Result::ok)
            .flat_map(|(key, value)| {
                let (tree, seq) = key_encoders::decode_pubkey_u64(key.to_vec()).ok()?;
                let tree_seq_idx = bincode::deserialize::<TreeSeqIdx>(value.as_ref()).ok()?;
                Some(TreeState {
                    tree,
                    seq,
                    slot: tree_seq_idx.slot,
                })
            })
    }

    fn gaps_count(&self) -> i64 {
        self.trees_gaps.iter_start().count() as i64
    }

    async fn process_tree_gap(&self, tree: Pubkey, gap_found: bool, _last_consistent_seq: u64) {
        let result = if gap_found {
            self.trees_gaps.put_async(tree, TreesGaps {}).await
        } else {
            self.trees_gaps.delete(tree).map_err(Into::into)
        };
        if let Err(e) = result {
            error!(
                "{} tree gap: {}",
                if gap_found { "Put" } else { "Delete" },
                e
            );
        }
    }

    fn get_last_ingested_slot(&self) -> core::result::Result<Option<u64>, String> {
        let mut iter = self.ingested_slots.iter_end();
        let last = iter.next();
        if let Some(last) = last {
            let (k, _) = last?;
            let slot = IngestedSlots::decode_key(k.to_vec()).map_err(|e| e.to_string())?;
            return Ok(Some(slot));
        }
        Ok(None)
    }
}
