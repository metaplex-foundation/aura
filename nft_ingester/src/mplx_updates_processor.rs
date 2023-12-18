use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use blockbuster::token_metadata::state::{Metadata, TokenStandard, UseMethod, Uses};
use log::error;
use num_traits::FromPrimitive;
use serde_json::json;
use tokio::time::Instant;

use metrics_utils::{IngesterMetricsConfig, MetricStatus};
use rocks_db::asset::{
    AssetAuthority, AssetCollection, AssetDynamicDetails, AssetStaticDetails, ChainDataV1, Creator,
    RoyaltyTargetType, SpecificationAssetClass,
};
use rocks_db::columns::Mint;
use rocks_db::Storage;

use crate::buffer::Buffer;
use crate::db_v2::{Asset, DBClient as DBClientV2, Task};

pub const BUFFER_PROCESSING_COUNTER: i32 = 10;

#[derive(Default)]
pub struct MetadataModels {
    pub asset_pubkeys: Vec<Vec<u8>>,
    pub all_pubkeys: Vec<Vec<u8>>,
    pub asset: Vec<Asset>,
    pub asset_creators: Vec<Creator>,
    pub asset_data: Vec<Task>,
}

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
    pub max_task_attempts: i16,
    pub db_client_v2: Arc<DBClientV2>,
    pub rocks_db: Arc<Storage>,

    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
}

impl MplxAccsProcessor {
    pub fn new(
        batch_size: usize,
        max_task_attempts: i16,
        buffer: Arc<Buffer>,
        db_client_v2: Arc<DBClientV2>,
        rocks_db: Arc<Storage>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Self {
        Self {
            batch_size,
            max_task_attempts,
            buffer,
            db_client_v2,
            rocks_db,
            metrics,
        }
    }

    pub async fn process_metadata_accs(&self, keep_running: Arc<AtomicBool>) {
        let mut counter = BUFFER_PROCESSING_COUNTER;
        let mut prev_buffer_size = 0;

        while keep_running.load(Ordering::SeqCst) {
            let mut metadata_info_buffer = self.buffer.mplx_metadata_info.lock().await;

            let buffer_size = metadata_info_buffer.len();

            if prev_buffer_size == 0 {
                prev_buffer_size = buffer_size;
            } else {
                if prev_buffer_size == buffer_size {
                    counter -= 1;
                } else {
                    prev_buffer_size = buffer_size;
                }
            }

            if buffer_size < self.batch_size {
                if counter != 0 {
                    drop(metadata_info_buffer);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            }

            counter = BUFFER_PROCESSING_COUNTER;

            let mut metadata_info = HashMap::new();

            for key in metadata_info_buffer
                .keys()
                .take(self.batch_size)
                .cloned()
                .collect::<Vec<Vec<u8>>>()
            {
                if let Some(value) = metadata_info_buffer.remove(&key) {
                    metadata_info.insert(key, value);
                }
            }
            drop(metadata_info_buffer);

            let mut mints_info_buffer = self.buffer.mints.lock().await;

            let mut mints_info = HashMap::new();

            for key in mints_info_buffer
                .keys()
                .take(self.batch_size)
                .cloned()
                .collect::<Vec<Vec<u8>>>()
            {
                if let Some(value) = mints_info_buffer.remove(&key) {
                    mints_info.insert(key, value);
                }
            }
            drop(mints_info_buffer);

            let metadata_models = self
                .create_rocks_metadata_models(&metadata_info, &mut mints_info)
                .await;

            let begin_processing = Instant::now();

            let res = metadata_models
                .asset_static
                .iter()
                .map(|asset| self.rocks_db.asset_static_data.merge(asset.pubkey, asset))
                .collect::<Result<(), _>>();
            match res {
                Err(e) => {
                    self.metrics
                        .inc_process("accounts_saving_static", MetricStatus::FAILURE);
                    error!("Error while saving static assets: {}", e);
                }
                Ok(_) => {
                    self.metrics
                        .inc_process("accounts_saving_static", MetricStatus::SUCCESS);
                }
            }

            for asset in metadata_models.asset_dynamic.iter() {
                let existing_value = self.rocks_db.asset_dynamic_data.get(asset.pubkey.clone());

                match existing_value {
                    Ok(existing_value) => {
                        let insert_value = if let Some(existing_value) = existing_value {
                            AssetDynamicDetails {
                                pubkey: asset.pubkey,
                                is_compressible: existing_value.is_compressible,
                                is_compressed: existing_value.is_compressed,
                                is_frozen: existing_value.is_frozen,
                                supply: asset.supply,
                                seq: asset.seq,
                                is_burnt: existing_value.is_burnt,
                                was_decompressed: existing_value.was_decompressed,
                                onchain_data: asset.onchain_data.clone(),
                                creators: asset.creators.clone(),
                                royalty_amount: asset.royalty_amount,
                                slot_updated: asset.slot_updated,
                            }
                        } else {
                            asset.clone()
                        };
                        let res = self
                            .rocks_db
                            .asset_dynamic_data
                            .merge(asset.pubkey, &insert_value);

                        match res {
                            Err(e) => {
                                self.metrics
                                    .inc_process("accounts_saving_dynamic", MetricStatus::FAILURE);
                                error!("Error while inserting dynamic data: {}", e);
                            }
                            Ok(_) => {
                                self.metrics
                                    .inc_process("accounts_saving_dynamic", MetricStatus::SUCCESS);

                                let upd_res = self
                                    .rocks_db
                                    .asset_updated(asset.slot_updated as u64, asset.pubkey.clone());

                                if let Err(e) = upd_res {
                                    error!("Error while updating assets update idx: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        self.metrics
                            .inc_process("accounts_saving_dynamic", MetricStatus::FAILURE);
                        error!("Error while inserting dynamic data: {}", e);
                    }
                }
            }

            let res = metadata_models
                .asset_authority
                .iter()
                .map(|asset| {
                    self.rocks_db
                        .asset_authority_data
                        .merge(asset.pubkey, asset)
                })
                .collect::<Result<(), _>>();
            match res {
                Err(e) => {
                    self.metrics
                        .inc_process("accounts_saving_authority", MetricStatus::FAILURE);
                    error!("Error while saving authority: {}", e);
                }
                Ok(_) => {
                    self.metrics
                        .inc_process("accounts_saving_authority", MetricStatus::SUCCESS);
                }
            }

            let res = metadata_models
                .asset_collection
                .iter()
                .map(|asset| {
                    self.rocks_db
                        .asset_collection_data
                        .merge(asset.pubkey, asset)
                })
                .collect::<Result<(), _>>();
            match res {
                Err(e) => {
                    self.metrics
                        .inc_process("accounts_saving_collection", MetricStatus::FAILURE);
                    error!("Error while saving collection: {}", e);
                }
                Ok(_) => {
                    self.metrics
                        .inc_process("accounts_saving_collection", MetricStatus::SUCCESS);
                }
            }

            let res = self.db_client_v2.insert_tasks(&metadata_models.tasks).await;
            match res {
                Err(e) => {
                    self.metrics
                        .inc_process("accounts_saving_tasks", MetricStatus::FAILURE);
                    error!("Error while saving tasks: {}", e);
                }
                Ok(_) => {
                    self.metrics
                        .inc_process("accounts_saving_tasks", MetricStatus::SUCCESS);
                }
            }

            self.metrics
                .set_latency("accounts_saving", begin_processing.elapsed().as_secs_f64());
        }
    }

    async fn create_rocks_metadata_models(
        &self,
        metadatas: &HashMap<Vec<u8>, MetadataInfo>,
        mints: &mut HashMap<Vec<u8>, Mint>,
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
                pubkey: mint.clone(),
                specification_asset_class: class,
                royalty_target_type: RoyaltyTargetType::Creators,
                created_at: metadata_info.slot as i64,
            });

            let mut supply = None;

            let mint_bytes = mint.to_bytes().to_vec();
            if let Some(mint_data) = mints.get_mut(&mint_bytes) {
                if mint_data.slot_updated > metadata_info.slot as i64 {
                    supply = Some(mint_data.supply as u64);
                } else {
                    supply = Some(1);
                }

                mints.remove(&mint_bytes);
            } else {
                supply = Some(1);
            }

            let mut chain_data = ChainDataV1 {
                name: data.name.clone(),
                symbol: data.symbol.clone(),
                edition_nonce: metadata.edition_nonce,
                primary_sale_happened: metadata.primary_sale_happened,
                token_standard: metadata.token_standard,
                uses: metadata.uses.map(|u| Uses {
                    use_method: UseMethod::from_u8(u.use_method as u8).unwrap(),
                    remaining: u.remaining,
                    total: u.total,
                }),
            };
            chain_data.sanitize();

            let chain_data = json!(chain_data);

            models.asset_dynamic.push(AssetDynamicDetails {
                pubkey: mint.clone(),
                is_compressible: false,
                is_compressed: false,
                is_frozen: false,
                supply,
                seq: None,
                is_burnt: false,
                was_decompressed: false,
                onchain_data: Some(chain_data.to_string()),
                creators: data
                    .clone()
                    .creators
                    .unwrap_or_default()
                    .iter()
                    .map(|creator| Creator {
                        creator: creator.address,
                        creator_verified: creator.verified,
                        creator_share: creator.share,
                    })
                    .collect(),
                royalty_amount: data.seller_fee_basis_points,
                slot_updated: metadata_info.slot,
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
                    pubkey: mint.clone(),
                    collection: c.key.clone(),
                    is_collection_verified: c.verified,
                    collection_seq: None,
                    slot_updated: metadata_info.slot,
                });
            } else {
                if let Some(creators) = data.creators {
                    if creators.len() > 0 {
                        models.asset_collection.push(AssetCollection {
                            pubkey: mint.clone(),
                            collection: creators[0].address,
                            is_collection_verified: true,
                            collection_seq: None,
                            slot_updated: metadata_info.slot,
                        });
                    }
                }
            }

            models.asset_authority.push(AssetAuthority {
                pubkey: mint.clone(),
                authority,
                slot_updated: metadata_info.slot,
            });
        }

        models
    }
}
