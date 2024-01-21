use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use blockbuster::token_metadata::state::{Metadata, TokenStandard, UseMethod};
use log::error;
use serde_json::json;
use tokio::time::Instant;

use entities::enums::{RoyaltyTargetType, SpecificationAssetClass};
use entities::models::Updated;
use entities::models::{ChainDataV1, Creator, Uses};
use metrics_utils::{IngesterMetricsConfig, MetricStatus};
use rocks_db::asset::{AssetAuthority, AssetCollection, AssetDynamicDetails, AssetStaticDetails};
use rocks_db::errors::StorageError;
use rocks_db::Storage;

use crate::buffer::Buffer;
use crate::db_v2::{DBClient as DBClientV2, Task};

// interval after which buffer is flushed
pub const FLUSH_INTERVAL_SEC: u64 = 5;
// worker idle timeout
pub const WORKER_IDLE_TIMEOUT_MS: u64 = 100;
// arbitrary number, should be enough to not overflow batch insert command at Postgre
pub const MAX_BUFFERED_TASKS_TO_TAKE: usize = 5000;

#[derive(Default, Debug)]
pub struct RocksMetadataModels {
    pub asset_static: Vec<AssetStaticDetails>,
    pub asset_dynamic: Vec<AssetDynamicDetails>,
    pub asset_authority: Vec<AssetAuthority>,
    pub asset_collection: Vec<AssetCollection>,
    pub tasks: Vec<Task>,
}

pub struct MetadataInfo {
    pub metadata: Metadata,
    pub slot: u64,
}

#[derive(Clone)]
pub struct MplxAccsProcessor {
    pub batch_size: usize,
    pub db_client_v2: Arc<DBClientV2>,
    pub rocks_db: Arc<Storage>,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
    last_received_at: Option<SystemTime>,
}

macro_rules! store_assets {
    ($self:expr, $assets:expr, $db_field:ident, $metric_name:expr) => {{
        let save_values =
            $assets
                .into_iter()
                .fold(HashMap::new(), |mut acc: HashMap<_, _>, asset| {
                    acc.insert(asset.pubkey, asset);
                    acc
                });

        let res = $self.rocks_db.$db_field.merge_batch(save_values).await;
        result_to_metrics($self.metrics.clone(), &res, $metric_name);
        res
    }};
}

impl MplxAccsProcessor {
    pub fn new(
        batch_size: usize,
        buffer: Arc<Buffer>,
        db_client_v2: Arc<DBClientV2>,
        rocks_db: Arc<Storage>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Self {
        Self {
            batch_size,
            buffer,
            db_client_v2,
            rocks_db,
            metrics,
            last_received_at: None,
        }
    }

    pub async fn process_metadata_accs(&mut self, keep_running: Arc<AtomicBool>) {
        while keep_running.load(Ordering::SeqCst) {
            let buffer_len = self.buffer.mplx_metadata_len().await;
            if buffer_len < self.batch_size {
                // sleep only in case when buffer is empty or n seconds passed since last insert
                if buffer_len == 0
                    || self.last_received_at.is_some_and(|t| {
                        t.elapsed().is_ok_and(|e| e.as_secs() < FLUSH_INTERVAL_SEC)
                    })
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(WORKER_IDLE_TIMEOUT_MS))
                        .await;
                    continue;
                }
            }

            let mut max_slot = 0;
            let metadata_info = {
                let mut metadata_info_buffer = self.buffer.mplx_metadata_info.lock().await;
                let mut elems = HashMap::new();

                for key in metadata_info_buffer
                    .keys()
                    .take(self.batch_size)
                    .cloned()
                    .collect::<Vec<Vec<u8>>>()
                {
                    if let Some(value) = metadata_info_buffer.remove(&key) {
                        if value.slot > max_slot {
                            max_slot = value.slot;
                        }

                        elems.insert(key, value);
                    }
                }

                elems
            };
            let metadata_models = self.create_rocks_metadata_models(&metadata_info).await;

            // store data
            let begin_processing = Instant::now();
            let (store_static_res, store_dynamic_res, store_authority_res, store_collection_res, _) = tokio::join!(
                self.store_static(metadata_models.asset_static),
                self.store_dynamic(metadata_models.asset_dynamic.clone()),
                self.store_authority(metadata_models.asset_authority),
                self.store_collection(metadata_models.asset_collection),
                self.store_tasks(metadata_models.tasks)
            );

            // We need to call asset_updated only if all asset-related columns was successfully stored new data
            if store_static_res.is_ok()
                && store_dynamic_res.is_ok()
                && store_authority_res.is_ok()
                && store_collection_res.is_ok()
            {
                metadata_models.asset_dynamic.iter().for_each(|asset| {
                    let upd_res = self
                        .rocks_db
                        .asset_updated(asset.get_slot_updated(), asset.pubkey);
                    if let Err(e) = upd_res {
                        error!("Error while updating assets update idx: {}", e);
                    }
                });
            }

            self.metrics.set_latency(
                "accounts_saving",
                begin_processing.elapsed().as_millis() as f64,
            );

            self.metrics
                .set_last_processed_slot("mplx_metadata", max_slot as i64);

            self.last_received_at = Some(SystemTime::now());
        }
    }

    async fn create_rocks_metadata_models(
        &self,
        metadatas: &HashMap<Vec<u8>, MetadataInfo>,
    ) -> RocksMetadataModels {
        let mut models = RocksMetadataModels::default();

        for (_, metadata_info) in metadatas.iter() {
            let metadata = metadata_info.metadata.clone();
            let data = metadata.data;
            let mint = metadata.mint;
            let authority = metadata.update_authority;
            let uri = data.uri.trim().replace('\0', "");
            let class: SpecificationAssetClass = match metadata.token_standard {
                Some(TokenStandard::NonFungible) => SpecificationAssetClass::Nft,
                Some(TokenStandard::FungibleAsset) => SpecificationAssetClass::FungibleAsset,
                Some(TokenStandard::Fungible) => SpecificationAssetClass::FungibleToken,
                _ => SpecificationAssetClass::Unknown,
            };

            models.asset_static.push(AssetStaticDetails {
                pubkey: mint,
                specification_asset_class: class,
                royalty_target_type: RoyaltyTargetType::Creators,
                created_at: metadata_info.slot as i64,
            });

            let mut chain_data = ChainDataV1 {
                name: data.name.clone(),
                symbol: data.symbol.clone(),
                edition_nonce: metadata.edition_nonce,
                primary_sale_happened: metadata.primary_sale_happened,
                token_standard: metadata
                    .token_standard
                    .map(|s| token_standard_from_mpl_state(&s)),
                uses: metadata.uses.map(|u| Uses {
                    use_method: use_method_from_mpl_state(&u.use_method),
                    remaining: u.remaining,
                    total: u.total,
                }),
                chain_mutability: None,
            };
            chain_data.sanitize();

            let chain_data = json!(chain_data);

            // supply field saving inside process_mint_accs fn
            models.asset_dynamic.push(AssetDynamicDetails {
                pubkey: mint,
                is_compressible: Updated::new(metadata_info.slot, None, false),
                is_compressed: Updated::new(metadata_info.slot, None, false),
                is_frozen: Updated::new(metadata_info.slot, None, false),
                seq: None,
                is_burnt: Updated::new(metadata_info.slot, None, false),
                was_decompressed: Updated::new(metadata_info.slot, None, false),
                onchain_data: Some(Updated::new(
                    metadata_info.slot,
                    None,
                    chain_data.to_string(),
                )),
                creators: Updated::new(
                    metadata_info.slot,
                    None,
                    data.clone()
                        .creators
                        .unwrap_or_default()
                        .iter()
                        .map(|creator| Creator {
                            creator: creator.address,
                            creator_verified: creator.verified,
                            creator_share: creator.share,
                        })
                        .collect(),
                ),
                royalty_amount: Updated::new(
                    metadata_info.slot,
                    None,
                    data.seller_fee_basis_points,
                ),
                url: Updated::new(metadata_info.slot, None, uri.clone()),
                ..Default::default()
            });

            models.tasks.push(Task {
                ofd_metadata_url: uri.clone(),
                ofd_locked_until: Some(chrono::Utc::now()),
                ofd_attempts: 0,
                ofd_max_attempts: 10,
                ofd_error: None,
            });

            if let Some(c) = &metadata.collection {
                models.asset_collection.push(AssetCollection {
                    pubkey: mint,
                    collection: c.key,
                    is_collection_verified: c.verified,
                    collection_seq: None,
                    slot_updated: metadata_info.slot,
                });
            } else if let Some(creators) = data.creators {
                if !creators.is_empty() {
                    models.asset_collection.push(AssetCollection {
                        pubkey: mint,
                        collection: creators[0].address,
                        is_collection_verified: true,
                        collection_seq: None,
                        slot_updated: metadata_info.slot,
                    });
                }
            }

            models.asset_authority.push(AssetAuthority {
                pubkey: mint,
                authority,
                slot_updated: metadata_info.slot,
            });
        }

        models
    }

    async fn store_static(
        &self,
        asset_static: Vec<AssetStaticDetails>,
    ) -> Result<(), StorageError> {
        store_assets!(
            self,
            asset_static,
            asset_static_data,
            "accounts_saving_static"
        )
    }

    async fn store_dynamic(
        &self,
        asset_dynamic: Vec<AssetDynamicDetails>,
    ) -> Result<(), StorageError> {
        store_assets!(
            self,
            asset_dynamic,
            asset_dynamic_data,
            "accounts_saving_dynamic"
        )
    }

    async fn store_authority(
        &self,
        asset_authority: Vec<AssetAuthority>,
    ) -> Result<(), StorageError> {
        store_assets!(
            self,
            asset_authority,
            asset_authority_data,
            "accounts_saving_authority"
        )
    }

    async fn store_collection(
        &self,
        asset_collection: Vec<AssetCollection>,
    ) -> Result<(), StorageError> {
        store_assets!(
            self,
            asset_collection,
            asset_collection_data,
            "accounts_saving_collection"
        )
    }

    async fn store_tasks(&self, tasks: Vec<Task>) {
        let mut tasks_to_insert = tasks.clone();

        // scope crated to unlock mutex before insert_tasks func, which can be time consuming
        let tasks = {
            let mut tasks_buffer = self.buffer.json_tasks.lock().await;

            let number_of_tasks = {
                if tasks_buffer.len() + tasks.len() > MAX_BUFFERED_TASKS_TO_TAKE {
                    MAX_BUFFERED_TASKS_TO_TAKE.saturating_sub(tasks.len())
                } else {
                    tasks_buffer.len()
                }
            };

            tasks_buffer
                .drain(0..number_of_tasks)
                .collect::<Vec<Task>>()
        };

        tasks_to_insert.extend(tasks);

        let res = self.db_client_v2.insert_tasks(&tasks_to_insert).await;
        result_to_metrics(self.metrics.clone(), &res, "accounts_saving_tasks");
    }
}

pub fn use_method_from_mpl_state(value: &UseMethod) -> entities::enums::UseMethod {
    match value {
        UseMethod::Burn => entities::enums::UseMethod::Burn,
        UseMethod::Multiple => entities::enums::UseMethod::Multiple,
        UseMethod::Single => entities::enums::UseMethod::Single,
    }
}

pub fn token_standard_from_mpl_state(value: &TokenStandard) -> entities::enums::TokenStandard {
    match value {
        TokenStandard::NonFungible => entities::enums::TokenStandard::NonFungible,
        TokenStandard::FungibleAsset => entities::enums::TokenStandard::FungibleAsset,
        TokenStandard::Fungible => entities::enums::TokenStandard::Fungible,
        TokenStandard::NonFungibleEdition => entities::enums::TokenStandard::NonFungibleEdition,
        TokenStandard::ProgrammableNonFungible => {
            entities::enums::TokenStandard::ProgrammableNonFungible
        }
        TokenStandard::ProgrammableNonFungibleEdition => {
            entities::enums::TokenStandard::ProgrammableNonFungibleEdition
        }
    }
}

pub fn result_to_metrics<T, E: Display>(
    metrics: Arc<IngesterMetricsConfig>,
    result: &Result<T, E>,
    metric_name: &str,
) {
    match result {
        Err(e) => {
            metrics.inc_process(metric_name, MetricStatus::FAILURE);
            error!("Error {}: {}", metric_name, e);
        }
        Ok(_) => {
            metrics.inc_process(metric_name, MetricStatus::SUCCESS);
        }
    }
}
