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
    pub(crate) fn new() -> Self {
        Self {
            overwhelm_backfill_gap: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn run(
        &self,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        mut rx: tokio::sync::broadcast::Receiver<()>,
        rocks_db: Arc<Storage>,
        consistence_backfilling_slots_threshold: u64,
    ) {
        let overwhelm_backfill_gap_clone = self.overwhelm_backfill_gap.clone();
        tasks.lock().await.spawn(async move {
            tokio::select! {
                _ = async {
                    loop {
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
                } => {
                    Ok(())
                },
                _ = rx.recv() => {
                    Ok(())
                }
            }
        });
    }
}

impl ConsistencyChecker for BackfillingStateConsistencyChecker {
    fn should_cancel_request(&self, _call: &Call) -> bool {
        self.overwhelm_backfill_gap.load(Ordering::SeqCst)
    }
}
