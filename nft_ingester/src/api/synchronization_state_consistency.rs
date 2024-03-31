use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;
use postgre_client::storage_traits::AssetIndexStorage;
use postgre_client::PgClient;
use rocks_db::key_encoders::decode_u64x2_pubkey;
use rocks_db::storage_traits::AssetUpdateIndexStorage;
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};

const CATCH_UP_SEQUENCES_TIMEOUT_SEC: u64 = 30;
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
    last_primary_storage_seq: Arc<AtomicU64>,
    last_index_storage_seq: Arc<AtomicU64>,
    synchronization_api_threshold: u64,
}

impl SynchronizationStateConsistencyChecker {
    pub(crate) async fn build(
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        keep_running: Arc<AtomicBool>,
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        synchronization_api_threshold: u64,
    ) -> Self {
        let last_primary_storage_seq = Arc::new(AtomicU64::new(0));
        let last_index_storage_seq = Arc::new(AtomicU64::new(0));

        let last_primary_storage_seq_clone = last_primary_storage_seq.clone();
        let last_index_storage_seq_clone = last_index_storage_seq.clone();
        let cloned_keep_running = keep_running.clone();
        tasks.lock().await.spawn(async move {
            while cloned_keep_running.load(Ordering::SeqCst) {
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
                last_primary_storage_seq_clone.store(primary_update_key.seq, Ordering::SeqCst);
                last_index_storage_seq_clone.store(decoded_index_update_key.seq, Ordering::SeqCst);
            }
            Ok(())
        });

        Self {
            last_primary_storage_seq,
            last_index_storage_seq,
            synchronization_api_threshold,
        }
    }
}

impl ConsistencyChecker for SynchronizationStateConsistencyChecker {
    fn should_cancel_request(&self, call: &Call) -> bool {
        if self
            .last_primary_storage_seq
            .load(Ordering::SeqCst)
            .saturating_sub(self.last_index_storage_seq.load(Ordering::SeqCst))
            < self.synchronization_api_threshold
        {
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
