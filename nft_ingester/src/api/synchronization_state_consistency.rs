use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;
use postgre_client::storage_traits::AssetIndexStorage;
use postgre_client::PgClient;
use rocks_db::key_encoders::decode_u64x2_pubkey;
use rocks_db::storage_traits::AssetUpdateIndexStorage;
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

pub(crate) const CATCH_UP_SEQUENCES_TIMEOUT_SEC: u64 = 30;
const INDEX_STORAGE_DEPENDS_METHODS: &[&str] = &[
    "getAssetsByOwner",
    "get_assets_by_owner",
    "getAssetsByCreator",
    "get_assets_by_creator",
    "getAssetsByAuthority",
    "get_assets_by_authority",
    "getAssetsByGroup",
    "get_assets_by_group",
    "getGrouping",
    "get_grouping",
    "searchAssets",
    "search_assets",
];

pub struct SynchronizationStateConsistencyChecker {
    overwhelm_seq_gap: Arc<AtomicBool>,
}

impl SynchronizationStateConsistencyChecker {
    pub(crate) fn new() -> Self {
        Self {
            overwhelm_seq_gap: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn run(
        &self,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        mut rx: tokio::sync::broadcast::Receiver<()>,
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        synchronization_api_threshold: u64,
    ) {
        let overwhelm_seq_gap_clone = self.overwhelm_seq_gap.clone();
        tasks.lock().await.spawn(async move {
            tokio::select! {
                _ = async {
                    loop {
                        tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC)).await;
                        let Ok(Some(index_seq)) = pg_client.fetch_last_synced_id().await else {
                            continue;
                        };
                        let Ok(decoded_index_update_key) = decode_u64x2_pubkey(index_seq) else {
                            continue;
                        };
                        let Ok(Some(primary_update_key)) = rocks_db.last_known_asset_updated_key() else {
                            continue;
                        };

                        overwhelm_seq_gap_clone.store(
                            primary_update_key
                                .seq
                                .saturating_sub(decoded_index_update_key.seq)
                                >= synchronization_api_threshold,
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

impl ConsistencyChecker for SynchronizationStateConsistencyChecker {
    fn should_cancel_request(&self, call: &Call) -> bool {
        if !self.overwhelm_seq_gap.load(Ordering::SeqCst) {
            return false;
        }

        return match call {
            Call::MethodCall(method_call) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&method_call.method.as_str())
            }
            Call::Notification(notification) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&notification.method.as_str())
            }
            _ => false,
        };
    }
}
