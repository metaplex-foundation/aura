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
use postgre_client::{storage_traits::AssetIndexStorage, PgClient};
use rocks_db::{
    key_encoders::decode_u64x2_pubkey, storage_traits::AssetUpdateIndexStorage, Storage,
};
use tokio_util::sync::CancellationToken;

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
        cancellation_token: CancellationToken,
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
            let cancellation_token = cancellation_token.child_token();
            usecase::executor::spawn(async move {
                while !cancellation_token.is_cancelled() {
                    let Ok(Some(index_seq)) = pg_client.fetch_last_synced_id(asset_type).await
                    else {
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
                        primary_update_key.seq.saturating_sub(decoded_index_update_key.seq)
                            >= synchronization_api_threshold,
                        Ordering::Relaxed,
                    );

                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC))=> {},
                        _ = cancellation_token.cancelled() => {
                            tracing::info!("Received stop signal, stopping SynchronizationStateConsistencyChecker...");
                        }
                    }
                }
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

        match call {
            Call::MethodCall(method_call) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&method_call.method.as_str())
            },
            Call::Notification(notification) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&notification.method.as_str())
            },
            _ => false,
        }
    }
}
