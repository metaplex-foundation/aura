use crate::api::synchronization_state_consistency::CATCH_UP_SEQUENCES_TIMEOUT_SEC;
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;
use rocks_db::parameters::Parameter;
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub struct BackfillingStateConsistencyChecker {
    overwhelm_backfill_gap: Arc<AtomicBool>,
}

impl BackfillingStateConsistencyChecker {
    pub(crate) async fn build(
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        keep_running: Arc<AtomicBool>,
        rocks_db: Arc<Storage>,
        consistence_backfilling_slots_threshold: u64,
    ) -> Self {
        let overwhelm_backfill_gap = Arc::new(AtomicBool::new(false));
        let overwhelm_backfill_gap_clone = overwhelm_backfill_gap.clone();
        let cloned_keep_running = keep_running.clone();
        tasks.lock().await.spawn(async move {
            while cloned_keep_running.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC)).await;
                let Ok(Some(top_seen_slot)) =
                    rocks_db.get_parameter::<u64>(Parameter::TopSeenSlot).await
                else {
                    continue;
                };
                let Ok(Some(last_backfilled_slot)) = rocks_db
                    .get_parameter::<u64>(Parameter::LastBackfilledSlot)
                    .await
                else {
                    continue;
                };

                overwhelm_backfill_gap_clone.store(
                    top_seen_slot.saturating_sub(last_backfilled_slot)
                        >= consistence_backfilling_slots_threshold,
                    Ordering::SeqCst,
                );
            }
            Ok(())
        });

        Self {
            overwhelm_backfill_gap,
        }
    }
}

impl ConsistencyChecker for BackfillingStateConsistencyChecker {
    fn should_cancel_request(&self, _call: &Call) -> bool {
        self.overwhelm_backfill_gap.load(Ordering::SeqCst)
    }
}
