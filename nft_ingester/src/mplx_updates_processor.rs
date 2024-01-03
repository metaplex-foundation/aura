use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use blockbuster::token_metadata::state::{Metadata, TokenStandard, UseMethod};
use log::error;
use serde_json::json;
use tokio::time::Instant;

use entities::enums::{RoyaltyTargetType, SpecificationAssetClass};
use entities::models::Updated;
use entities::models::{ChainDataV1, Creator, Uses};
use metrics_utils::{IngesterMetricsConfig, MetricStatus};
use rocks_db::asset::{AssetAuthority, AssetCollection, AssetDynamicDetails, AssetStaticDetails};
use rocks_db::columns::Mint;
use rocks_db::Storage;

use crate::buffer::Buffer;
use crate::db_v2::{DBClient as DBClientV2, Task};

pub const BUFFER_PROCESSING_COUNTER: i32 = 10;

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
            } else if prev_buffer_size == buffer_size {
                counter -= 1;
            } else {
                prev_buffer_size = buffer_size;
            }

            if buffer_size < self.batch_size && counter != 0 {
                drop(metadata_info_buffer);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
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
                .try_for_each(|asset| self.rocks_db.asset_static_data.merge(asset.pubkey, asset));

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
                let existing_value = self.rocks_db.asset_dynamic_data.get(asset.pubkey);

                match existing_value {
                    Ok(existing_value) => {
                        let insert_value = if let Some(existing_value) = existing_value {
                            AssetDynamicDetails {
                                pubkey: asset.pubkey,
                                is_compressible: existing_value.is_compressible,
                                is_compressed: existing_value.is_compressed,
                                is_frozen: existing_value.is_frozen,
                                supply: asset.supply.clone(),
                                seq: asset.seq.clone(),
                                is_burnt: existing_value.is_burnt,
                                was_decompressed: existing_value.was_decompressed,
                                onchain_data: asset.onchain_data.clone(),
                                creators: asset.creators.clone(),
                                royalty_amount: asset.royalty_amount.clone(),
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
                                    .asset_updated(asset.get_slot_updated(), asset.pubkey);

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
                .try_for_each(|asset| {
                    self.rocks_db
                        .asset_authority_data
                        .merge(asset.pubkey, asset)
                });
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
                .try_for_each(|asset| {
                    self.rocks_db
                        .asset_collection_data
                        .merge(asset.pubkey, asset)
                });
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
                pubkey: mint,
                specification_asset_class: class,
                royalty_target_type: RoyaltyTargetType::Creators,
                created_at: metadata_info.slot as i64,
            });

            let supply;
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
                token_standard: metadata
                    .token_standard
                    .map(|s| token_standard_from_mpl_state(&s)),
                uses: metadata.uses.map(|u| Uses {
                    use_method: use_method_from_mpl_state(&u.use_method),
                    remaining: u.remaining,
                    total: u.total,
                }),
            };
            chain_data.sanitize();

            let chain_data = json!(chain_data);

            models.asset_dynamic.push(AssetDynamicDetails {
                pubkey: mint,
                is_compressible: Updated::new(metadata_info.slot, None, false),
                is_compressed: Updated::new(metadata_info.slot, None, false),
                is_frozen: Updated::new(metadata_info.slot, None, false),
                supply: supply.map(|supply| Updated::new(metadata_info.slot, None, supply)),
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
