use crate::bubblegum_slots::{ForceReingestableSlots, PeerForceReingestableSlots};
use crate::column::TypedColumn;
use crate::tree_seq::{TreeSeqIdx, TreesGaps};
use crate::{key_encoders, Storage};
use async_trait::async_trait;
use entities::models::TreeState;
use interface::sequence_consistent::SequenceConsistentManager;
use interface::slot_getter::LastProcessedSlotGetter;
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

    async fn process_tree_gap(&self, tree: Pubkey, gap_found: bool) {
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

    async fn all_processed_reingestable_slots(&self) -> Vec<u64> {
        let processed_reingestable_slots = self
            .peer_force_reingestable_slots
            .iter_start()
            .filter_map(Result::ok)
            .flat_map(|(key, value)| {
                let processed =
                    bincode::deserialize::<PeerForceReingestableSlots>(value.to_vec().as_slice())
                        .ok()?
                        .processed;
                if !processed {
                    return None;
                }
                ForceReingestableSlots::decode_key(key.to_vec()).ok()
            })
            .fold(Vec::new(), |mut acc, slot| {
                acc.push(slot);
                acc
            });
        // Delete all these slots, so we can check will we find them second time
        if let Err(e) = self
            .peer_force_reingestable_slots
            .delete_batch(processed_reingestable_slots.clone())
            .await
        {
            error!("Delete peer force reingestable slots: {}", e);
        }
        processed_reingestable_slots
    }

    async fn manage_ingestable_slots(&self, processed_reingestable_slots: Vec<u64>) {
        let secondly_find_non_consistent_slots = match self
            .peer_force_reingestable_slots
            .batch_get(processed_reingestable_slots.clone())
            .await
            .map(|slots| {
                slots
                    .into_iter()
                    .flatten()
                    .map(|r| r.slot)
                    .collect::<Vec<_>>()
            }) {
            Ok(slots) => slots,
            Err(e) => {
                error!("Batch get peer force reingestable slots: {}", e);
                return;
            }
        };
        if let Err(e) = self
            .peer_force_reingestable_slots
            .delete_batch(secondly_find_non_consistent_slots.clone())
            .await
        {
            error!("Delete peer force reingestable slots: {}", e);
        };
        if let Err(e) = self
            .force_reingestable_slots
            .put_batch(
                secondly_find_non_consistent_slots
                    .into_iter()
                    .map(|slot| (slot, ForceReingestableSlots {}))
                    .collect(),
            )
            .await
        {
            error!("Delete peer force reingestable slots: {}", e);
        };
    }
}

#[async_trait]
impl LastProcessedSlotGetter for Storage {
    async fn get_last_ingested_slot(&self) -> Result<Option<u64>, String> {
        self.get_parameter::<u64>(crate::parameters::Parameter::TopSeenSlot)
            .await
            .map_err(|e| e.to_string())
    }
}
