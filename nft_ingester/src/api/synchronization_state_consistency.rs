use entities::enums::{AssetType, ASSET_TYPES};
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
use tracing::info;

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
    overwhelm_nft_seq_gap: Arc<AtomicBool>,
    overwhelm_fungible_seq_gap: Arc<AtomicBool>,
}

impl SynchronizationStateConsistencyChecker {
    pub(crate) fn new() -> Self {
        Self {
            overwhelm_nft_seq_gap: Arc::new(AtomicBool::new(false)),
            overwhelm_fungible_seq_gap: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn run(
        &self,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
        rx: tokio::sync::broadcast::Receiver<()>,
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        synchronization_api_threshold: u64,
    ) {
        for asset_type in ASSET_TYPES {
            let overwhelm_seq_gap = match asset_type {
                AssetType::NonFungible => self.overwhelm_nft_seq_gap.clone(),
                AssetType::Fungible => self.overwhelm_fungible_seq_gap.clone(),
            };
            let pg_client = pg_client.clone();
            let rocks_db = rocks_db.clone();
            let mut rx = rx.resubscribe();
            tasks.lock().await.spawn(async move {
                while rx.is_empty() {
                    let Ok(Some(index_seq)) = pg_client.fetch_last_synced_id(asset_type).await else {
                        continue;
                    };
                    let Ok(decoded_index_update_key) = decode_u64x2_pubkey(index_seq) else {
                        continue;
                    };

                    let last_known_updated_asset = match asset_type {
                        AssetType::NonFungible => rocks_db.last_known_nft_asset_updated_key(),
                        AssetType::Fungible => rocks_db.last_known_fungible_asset_updated_key(),
                    };
                    let Ok(Some(primary_update_key)) = last_known_updated_asset else {
                        continue;
                    };

                    overwhelm_seq_gap.store(
                        primary_update_key
                            .seq
                            .saturating_sub(decoded_index_update_key.seq)
                            >= synchronization_api_threshold,
                        Ordering::Relaxed,
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC))=> {},
                        _ = rx.recv() => {
                            info!("Received stop signal, stopping SynchronizationStateConsistencyChecker...");
                            return Ok(());
                        }
                    }
                }
                Ok(())
            });
        }
    }
}

impl ConsistencyChecker for SynchronizationStateConsistencyChecker {
    fn should_cancel_request(&self, call: &Call) -> bool {
        if !&self.overwhelm_nft_seq_gap.load(Ordering::Relaxed)
            || !&self.overwhelm_fungible_seq_gap.load(Ordering::Relaxed)
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
