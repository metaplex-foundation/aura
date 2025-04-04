use std::sync::Arc;

use entities::models::TreeState;
use interface::{
    sequence_consistent::SequenceConsistentManager,
    signature_persistence::{BlockConsumer, BlockProducer},
    slot_getter::FinalizedSlotGetter,
};
use metrics_utils::{BackfillerMetricsConfig, SequenceConsistentGapfillMetricsConfig};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use usecase::slots_collector::{SlotsCollector, SlotsGetter};

use crate::inmemory_slots_dumper::InMemorySlotsDumper;

#[allow(clippy::too_many_arguments)]
pub async fn collect_sequences_gaps<R, S, F, BP, BC>(
    finalized_slot_getter: Arc<F>,
    sequence_consistent_manager: Arc<S>,
    backfiller_source: Arc<R>,
    backfiller_metrics: Arc<BackfillerMetricsConfig>,
    sequence_consistent_gapfill_metrics: Arc<SequenceConsistentGapfillMetricsConfig>,
    bp: Arc<BP>,
    bc: Arc<BC>,
    cancellation_token: CancellationToken,
) where
    R: SlotsGetter + Sync + Send + 'static,
    S: SequenceConsistentManager,
    F: FinalizedSlotGetter,
    BP: BlockProducer,
    BC: BlockConsumer,
{
    let last_slot_to_look_for_gaps = finalized_slot_getter.get_finalized_slot_no_error().await;
    let mut prev_state = TreeState::default();
    for current_state in sequence_consistent_manager.tree_sequence_iter() {
        if cancellation_token.is_cancelled() {
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
            sequence_consistent_gapfill_metrics.inc_gaps_count();
            let in_memory_dumper = Arc::new(InMemorySlotsDumper::new());
            let collector = SlotsCollector::new(
                in_memory_dumper.clone(),
                backfiller_source.clone(),
                backfiller_metrics.clone(),
            );
            // fill the gap now, the dumper is the inmemory one, so we could fetch the slots and ingest all of those

            collector
                .collect_slots(
                    &current_state.tree,
                    current_state.slot,
                    prev_state.slot,
                    cancellation_token.child_token(),
                )
                .await;
            let slots = in_memory_dumper.get_sorted_keys().await;
            for slot_num in slots {
                let slot_res = bp.get_block(slot_num, None::<Arc<BP>>).await;
                if let Err(e) = slot_res {
                    warn!("failed getting slot {}: {:?}", slot_num, e);
                    continue;
                }
                let slot = slot_res.unwrap();
                if let Err(e) = bc.consume_block(slot_num, slot).await {
                    warn!("failed processign slot {}: {:?}", slot_num, e);
                    continue;
                }
            }
        };
        prev_state = current_state;
    }
}
