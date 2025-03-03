use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use entities::enums::{AssetType, ASSET_TYPES};
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;
use rocks_db::Storage;
use tokio_util::sync::CancellationToken;

use crate::api::synchronization_state_consistency::CATCH_UP_SEQUENCES_TIMEOUT_SEC;

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
        cancellation_token: CancellationToken,
        rocks_db: Arc<Storage>,
        _consistence_backfilling_slots_threshold: u64,
    ) {
        for asset_type in ASSET_TYPES {
            let _rocks_db = rocks_db.clone();
            let _overwhelm_backfill_gap = match asset_type {
                AssetType::NonFungible => self.overwhelm_nft_backfill_gap.clone(),
                AssetType::Fungible => self.overwhelm_fungible_backfill_gap.clone(),
            };
            let cancellation_token = cancellation_token.child_token();
            usecase::executor::spawn(async move {
                while !cancellation_token.is_cancelled() {
                    // TODO: refactor this to use parameter from storage and last slot from slot storage
                    // overwhelm_backfill_gap.store(
                    //     rocks_db.bubblegum_slots.iter_start().count().saturating_add(rocks_db.ingestable_slots.iter_start().count())
                    //         >= consistence_backfilling_slots_threshold as usize,
                    //     Ordering::Relaxed,
                    // );
                    //
                    cancellation_token
                        .run_until_cancelled(tokio::time::sleep(Duration::from_secs(
                            CATCH_UP_SEQUENCES_TIMEOUT_SEC,
                        )))
                        .await;
                }
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
