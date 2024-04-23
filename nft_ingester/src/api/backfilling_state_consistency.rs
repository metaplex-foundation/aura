use crate::api::synchronization_state_consistency::CATCH_UP_SEQUENCES_TIMEOUT_SEC;
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tracing::info;

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
            while rx.is_empty() {
                overwhelm_backfill_gap_clone.store(
                    rocks_db.bubblegum_slots.iter_start().count() + rocks_db.ingestable_slots.iter_start().count()
                        >= consistence_backfilling_slots_threshold as usize,
                    Ordering::SeqCst,
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC)) => {},
                    _ = rx.recv() => {
                        info!("Received stop signal, stopping BackfillingStateConsistencyChecker...");
                        return Ok(());
                    }
                }
            }
            Ok(())
        });
    }
}

impl ConsistencyChecker for BackfillingStateConsistencyChecker {
    fn should_cancel_request(&self, _call: &Call) -> bool {
        self.overwhelm_backfill_gap.load(Ordering::SeqCst)
    }
}
