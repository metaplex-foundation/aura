use entities::models::TreeState;
use interface::sequence_consistent::SequenceConsistentManager;
use interface::slot_getter::FinalizedSlotGetter;
use interface::slots_dumper::SlotsDumper;
use metrics_utils::SequenceConsistentGapfillMetricsConfig;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::{info, warn};
use usecase::slots_collector::{SlotsCollector, SlotsGetter};

pub struct SequenceConsistentGapfiller<T, R, S, F>
where
    T: SlotsDumper + Sync + Send + 'static,
    R: SlotsGetter + Sync + Send + 'static,
    S: SequenceConsistentManager,
    F: FinalizedSlotGetter,
{
    sequence_consistent_manager: Arc<S>,
    slots_collector: Arc<SlotsCollector<T, R>>,
    metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
    finalized_slot_getter: Arc<F>,
}

impl<T, R, S, F> SequenceConsistentGapfiller<T, R, S, F>
where
    T: SlotsDumper + Sync + Send + 'static,
    R: SlotsGetter + Sync + Send + 'static,
    S: SequenceConsistentManager,
    F: FinalizedSlotGetter,
{
    pub fn new(
        sequence_consistent_manager: Arc<S>,
        slots_collector: SlotsCollector<T, R>,
        metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
        finalized_slot_getter: Arc<F>,
    ) -> Self {
        Self {
            sequence_consistent_manager,
            slots_collector: Arc::new(slots_collector),
            metrics,
            finalized_slot_getter,
        }
    }

    pub async fn collect_sequences_gaps(&self, rx: Receiver<()>) {
        let last_slot_to_look_for_gaps = self
            .finalized_slot_getter
            .get_finalized_slot_no_error()
            .await;
        let mut last_consistent_seq = 0;
        let mut prev_state = TreeState::default();
        let mut gap_found = false;
        for current_state in self.sequence_consistent_manager.tree_sequence_iter() {
            if !rx.is_empty() {
                info!("Stop iteration over tree iterator...");
                return;
            }
            // Skip the most recent slots to avoid gaps in recent slots.
            if current_state.slot > last_slot_to_look_for_gaps {
                continue;
            }
            if current_state.tree == prev_state.tree && current_state.seq != prev_state.seq + 1 {
                warn!(
                    "Gap found for {} tree. Sequences: [{}, {}], slots: [{}, {}]",
                    prev_state.tree,
                    prev_state.seq,
                    current_state.seq,
                    prev_state.slot,
                    current_state.slot
                );
                gap_found = true;

                let slots_collector = self.slots_collector.clone();
                slots_collector
                    .collect_slots(
                        &current_state.tree,
                        current_state.slot,
                        prev_state.slot,
                        &rx,
                    )
                    .await;
            };
            if prev_state.tree != current_state.tree {
                self.save_tree_gap_analyze(prev_state.tree, last_consistent_seq, gap_found)
                    .await;
                gap_found = false
            }
            // If keys already deleted for some tree, we must not to delete other keys in this tree
            // in order to save gap and in future check, if we fix it
            if !gap_found {
                last_consistent_seq = current_state.seq;
            }
            prev_state = current_state;
        }
        // Handle last tree keys
        self.save_tree_gap_analyze(prev_state.tree, last_consistent_seq, gap_found)
            .await
    }

    async fn save_tree_gap_analyze(
        &self,
        tree: solana_program::pubkey::Pubkey,
        _last_consistent_seq: u64, // TODO: use this parameter if we need to optimize the runtime of the iterator to skip the already consistent sequences. For example, store it with a tree in a separate column family.
        gap_found: bool,
    ) {
        self.sequence_consistent_manager
            .process_tree_gap(tree, gap_found)
            .await;
        self.metrics
            .set_total_tree_with_gaps(self.sequence_consistent_manager.gaps_count());
    }
}
