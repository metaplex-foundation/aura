use entities::models::TreeState;
use interface::sequence_consistent::SequenceConsistentManager;
use interface::slots_dumper::SlotsDumper;
use metrics_utils::SequenceConsistentGapfillMetricsConfig;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::{info, warn};
use usecase::slots_collector::{RowKeysGetter, SlotsCollector};

pub struct SequenceConsistentGapfiller<T, R, S>
where
    T: SlotsDumper + Sync + Send + 'static,
    R: RowKeysGetter + Sync + Send + 'static,
    S: SequenceConsistentManager,
{
    sequence_consistent_manager: Arc<S>,
    slots_collector: Arc<SlotsCollector<T, R>>,
    metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
    tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
}

impl<T, R, S> SequenceConsistentGapfiller<T, R, S>
where
    T: SlotsDumper + Sync + Send + 'static,
    R: RowKeysGetter + Sync + Send + 'static,
    S: SequenceConsistentManager,
{
    pub fn new(
        sequence_consistent_manager: Arc<S>,
        slots_collector: SlotsCollector<T, R>,
        metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    ) -> Self {
        Self {
            sequence_consistent_manager,
            slots_collector: Arc::new(slots_collector),
            metrics,
            tasks,
        }
    }

    pub async fn collect_sequences_gaps(&self, rx: &mut Receiver<()>) {
        let mut last_consistent_key = (solana_program::pubkey::Pubkey::default(), 0);
        let mut prev_state = TreeState::default();
        let mut is_find_gap = false;
        for current_state in self.sequence_consistent_manager.tree_sequence_iter() {
            if !rx.is_empty() {
                info!("Stop iteration over tree iterator...");
                return;
            }
            if current_state.tree == prev_state.tree && current_state.seq != prev_state.seq + 1 {
                warn!(
                    "Find GAP for {} tree: sequences: [{}, {}], slots: [{}, {}]",
                    prev_state.tree,
                    prev_state.seq,
                    current_state.seq,
                    prev_state.slot,
                    current_state.slot
                );
                is_find_gap = true;

                let slots_collector = self.slots_collector.clone();
                let mut rx_clone = rx.resubscribe();
                self.tasks.lock().await.spawn(tokio::spawn(async move {
                    slots_collector
                        .collect_slots(
                            &format!("{}/", current_state.tree),
                            current_state.slot,
                            prev_state.slot,
                            &mut rx_clone,
                        )
                        .await;
                }));
            };
            if prev_state.tree != current_state.tree {
                self.save_tree_gap_analyze(prev_state.tree, last_consistent_key, is_find_gap)
                    .await;
                is_find_gap = false
            }
            // If keys already deleted for some tree, we must not to delete other keys in this tree
            // in order to save gap and in future check, if we fix it
            if !is_find_gap {
                last_consistent_key = (current_state.tree, current_state.seq);
            }
            prev_state = current_state;
        }
        // Handle last tree keys
        self.save_tree_gap_analyze(prev_state.tree, last_consistent_key, is_find_gap)
            .await
    }

    async fn save_tree_gap_analyze(
        &self,
        tree: solana_program::pubkey::Pubkey,
        last_consistent_key: (solana_program::pubkey::Pubkey, u64),
        is_find_gap: bool,
    ) {
        self.sequence_consistent_manager
            .process_tree_gap(tree, is_find_gap, last_consistent_key)
            .await;
        self.metrics
            .set_total_tree_with_gaps(self.sequence_consistent_manager.gaps_count());
    }
}
