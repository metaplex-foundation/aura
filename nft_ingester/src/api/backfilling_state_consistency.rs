use crate::api::synchronization_state_consistency::CATCH_UP_SEQUENCES_TIMEOUT_SEC;
use entities::enums::{AssetType, ASSET_TYPES};
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
    overwhelm_fungible_backfill_gap: Arc<AtomicBool>,
    overwhelm_nft_backfill_gap: Arc<AtomicBool>,
}

impl BackfillingStateConsistencyChecker {
    pub(crate) fn new() -> Self {
        Self {
            overwhelm_fungible_backfill_gap: Arc::new(AtomicBool::new(false)),
            overwhelm_nft_backfill_gap: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn run(
        &self,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        rx: tokio::sync::broadcast::Receiver<()>,
        rocks_db: Arc<Storage>,
        _consistence_backfilling_slots_threshold: u64,
    ) {
        for asset_type in ASSET_TYPES {
            let _rocks_db = rocks_db.clone();
            let mut rx = rx.resubscribe();
            let _overwhelm_backfill_gap = match asset_type {
                AssetType::NonFungible => self.overwhelm_nft_backfill_gap.clone(),
                AssetType::Fungible => self.overwhelm_fungible_backfill_gap.clone(),
            };
            tasks.lock().await.spawn(async move {
            while rx.is_empty() {
                // TODO: refactor this to use parameter from storage and last slot from slot storage
                // overwhelm_backfill_gap.store(
                //     rocks_db.bubblegum_slots.iter_start().count().saturating_add(rocks_db.ingestable_slots.iter_start().count())
                //         >= consistence_backfilling_slots_threshold as usize,
                //     Ordering::Relaxed,
                // );
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
}

impl ConsistencyChecker for BackfillingStateConsistencyChecker {
    fn should_cancel_request(&self, _call: &Call) -> bool {
        self.overwhelm_nft_backfill_gap.load(Ordering::Relaxed)
            && self.overwhelm_fungible_backfill_gap.load(Ordering::Relaxed)
    }
}
