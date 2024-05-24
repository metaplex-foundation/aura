use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::SystemTime;

use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use blockbuster::token_metadata::accounts::Metadata;
use blockbuster::token_metadata::types::TokenStandard;
use log::error;
use mpl_token_metadata::accounts::MasterEdition;
use serde_json::json;
use solana_program::pubkey::Pubkey;
use tokio::time::Instant;

use crate::buffer::Buffer;
use entities::enums::{ChainMutability, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::Updated;
use entities::models::{ChainDataV1, Creator, UpdateVersion, Uses};
use metrics_utils::IngesterMetricsConfig;
use postgre_client::PgClient;
use rocks_db::asset::{
    AssetAuthority, AssetCollection, AssetDynamicDetails, AssetStaticDetails, MetadataMintMap,
};
use rocks_db::batch_savers::MetadataModels;
use rocks_db::editions::TokenMetadataEdition;
use rocks_db::errors::StorageError;
use rocks_db::{store_assets, Storage};
use usecase::save_metrics::result_to_metrics;

pub struct MetadataInfo {
    pub metadata: Metadata,
    pub slot_updated: u64,
    pub write_version: u64,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub executable: bool,
    pub metadata_owner: Option<String>,
}

pub struct TokenMetadata {
    pub edition: TokenMetadataEdition,
    pub write_version: u64,
    pub slot_updated: u64,
}

#[derive(Clone, Debug)]
pub struct BurntMetadata {
    pub key: Pubkey,
    pub slot: u64,
}

#[derive(Clone, Debug)]
pub struct BurntMetadataSlot {
    pub slot_updated: u64,
}

#[derive(Clone)]
pub struct IndexableAssetWithAccountInfo {
    pub indexable_asset: MplCoreAccountData,
    pub lamports: u64,
    pub executable: bool,
    pub slot_updated: u64,
    pub write_version: u64,
    pub rent_epoch: u64,
}

#[derive(Clone)]
pub struct MplxAccsProcessor {
    pub batch_size: usize,
    pub pg_client: Arc<PgClient>,
    pub rocks_db: Arc<Storage>,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
    last_received_metadata_at: Option<SystemTime>,
    last_received_edition_at: Option<SystemTime>,
    last_received_burnt_asset_at: Option<SystemTime>,
}

#[macro_export]
macro_rules! process_accounts {
    ($self:expr, $keep_running:expr, $buffer:expr, $batch_size:expr, $extract_value:expr, $last_received_at:expr, $transform_and_save:expr, $metric_name:expr) => {
        // interval after which buffer is flushed
        const WORKER_IDLE_TIMEOUT_MS: u64 = 100;
        // worker idle timeout
        const FLUSH_INTERVAL_SEC: u64 = 5;

        #[allow(clippy::redundant_closure_call)]
        while $keep_running.load(std::sync::atomic::Ordering::SeqCst) {
            let buffer_len = $buffer.lock().await.len();
            if buffer_len < $batch_size {
                if buffer_len == 0
                    || $last_received_at.is_some_and(|t| {
                        t.elapsed().is_ok_and(|e| e.as_secs() < FLUSH_INTERVAL_SEC)
                    })
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(WORKER_IDLE_TIMEOUT_MS))
                        .await;
                    continue;
                }
            }

            let mut max_slot = 0;
            let items_to_save = {
                let mut items = $buffer.lock().await;
                let mut elems = HashMap::new();

                for key in items.keys().take($batch_size).cloned().collect::<Vec<_>>() {
                    if let Some(value) = items.remove(&key) {
                        if value.slot_updated > max_slot {
                            max_slot = value.slot_updated;
                        }
                        elems.insert(key, $extract_value(value));
                    }
                }
                elems
            };

            $transform_and_save(&$self, &items_to_save).await;

            $self
                .metrics
                .set_last_processed_slot($metric_name, max_slot as i64);
            $last_received_at = Some(SystemTime::now());
        }
    };
}

impl MplxAccsProcessor {
    pub fn new(
        batch_size: usize,
        buffer: Arc<Buffer>,
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Self {
        Self {
            batch_size,
            buffer,
            pg_client,
            rocks_db,
            metrics,
            last_received_metadata_at: None,
            last_received_edition_at: None,
            last_received_burnt_asset_at: None,
        }
    }

    pub async fn process_edition_accs(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.token_metadata_editions,
            self.batch_size,
            |s: TokenMetadata| s.edition,
            self.last_received_edition_at,
            Self::transform_and_store_edition_accs,
            "edition"
        );
    }

    pub async fn process_metadata_accs(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.mplx_metadata_info,
            self.batch_size,
            |s: MetadataInfo| s,
            self.last_received_metadata_at,
            Self::transform_and_store_metadata_accs,
            "mplx_metadata"
        );
    }

    pub async fn process_burnt_accs(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.burnt_metadata_at_slot,
            self.batch_size,
            |s: BurntMetadataSlot| s,
            self.last_received_burnt_asset_at,
            Self::transform_and_store_burnt_metadata,
            "burn_metadata"
        );
    }

    pub async fn transform_and_store_burnt_metadata(
        &self,
        metadata_info: &HashMap<Pubkey, BurntMetadataSlot>,
    ) {
        let begin_processing = Instant::now();
        if let Err(e) = self.mark_metadata_as_burnt(metadata_info).await {
            error!("Mark metadata as burnt: {}", e);
        }
        self.metrics.set_latency(
            "burn_metadata",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn transform_and_store_metadata_accs(
        &self,
        metadata_info: &HashMap<Vec<u8>, MetadataInfo>,
    ) {
        let metadata_models = self.create_rocks_metadata_models(metadata_info).await;

        let begin_processing = Instant::now();
        self.rocks_db
            .store_metadata_models(&metadata_models, self.metrics.clone())
            .await;
        self.metrics.set_latency(
            "accounts_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn transform_and_store_edition_accs(
        &self,
        editions: &HashMap<Pubkey, TokenMetadataEdition>,
    ) {
        let begin_processing = Instant::now();
        let res = self
            .rocks_db
            .token_metadata_edition_cbor
            .merge_batch_cbor(editions.to_owned())
            .await;

        result_to_metrics(self.metrics.clone(), &res, "editions_saving");
        self.metrics.set_latency(
            "editions_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn create_rocks_metadata_models(
        &self,
        metadatas: &HashMap<Vec<u8>, MetadataInfo>,
    ) -> MetadataModels {
        let mut models = MetadataModels::default();

        for (metadata_raw_key, metadata_info) in metadatas.iter() {
            let metadata_raw_key: &[u8] = metadata_raw_key.as_ref();
            let metadata_pub_key = Pubkey::try_from(metadata_raw_key).ok();

            let metadata = metadata_info.metadata.clone();

            let mint = metadata.mint;

            if let Some(key) = metadata_pub_key {
                models.metadata_mint.push(MetadataMintMap {
                    pubkey: key,
                    mint_key: mint,
                });
            }

            let data = metadata.clone();
            let authority = metadata.update_authority;
            let uri = data.uri.trim().replace('\0', "");
            let class = match metadata.token_standard {
                Some(TokenStandard::NonFungible) => SpecificationAssetClass::Nft,
                Some(TokenStandard::FungibleAsset) => SpecificationAssetClass::FungibleAsset,
                Some(TokenStandard::Fungible) => SpecificationAssetClass::FungibleToken,
                Some(TokenStandard::NonFungibleEdition) => SpecificationAssetClass::Nft,
                Some(TokenStandard::ProgrammableNonFungible) => {
                    SpecificationAssetClass::ProgrammableNft
                }
                Some(TokenStandard::ProgrammableNonFungibleEdition) => {
                    SpecificationAssetClass::ProgrammableNft
                }
                _ => SpecificationAssetClass::Unknown,
            };

            models.asset_static.push(AssetStaticDetails {
                pubkey: mint,
                specification_asset_class: class,
                royalty_target_type: RoyaltyTargetType::Creators,
                created_at: metadata_info.slot_updated as i64,
                edition_address: Some(MasterEdition::find_pda(&mint).0),
            });

            let mut chain_data = ChainDataV1 {
                name: data.name.clone(),
                symbol: data.symbol.clone(),
                edition_nonce: metadata.edition_nonce,
                primary_sale_happened: metadata.primary_sale_happened,
                token_standard: metadata.token_standard.map(|s| s.into()),
                uses: metadata.uses.map(|u| Uses {
                    use_method: u.use_method.into(),
                    remaining: u.remaining,
                    total: u.total,
                }),
            };
            chain_data.sanitize();

            let chain_data = json!(chain_data);

            let chain_mutability = if metadata_info.metadata.is_mutable {
                ChainMutability::Mutable
            } else {
                ChainMutability::Immutable
            };

            // supply field saving inside process_mint_accs fn
            models.asset_dynamic.push(AssetDynamicDetails {
                pubkey: mint,
                is_compressible: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    false,
                ),
                is_compressed: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    false,
                ),
                seq: None,
                was_decompressed: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    false,
                ),
                onchain_data: Some(Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    chain_data.to_string(),
                )),
                creators: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
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
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    data.seller_fee_basis_points,
                ),
                url: Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    uri.clone(),
                ),
                lamports: Some(Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    metadata_info.lamports,
                )),
                executable: Some(Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    metadata_info.executable,
                )),
                metadata_owner: metadata_info.metadata_owner.clone().map(|m| {
                    Updated::new(
                        metadata_info.slot_updated,
                        Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                        m,
                    )
                }),
                chain_mutability: Some(Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    chain_mutability,
                )),
                rent_epoch: Some(Updated::new(
                    metadata_info.slot_updated,
                    Some(UpdateVersion::WriteVersion(metadata_info.write_version)),
                    metadata_info.rent_epoch,
                )),
                ..Default::default()
            });

            if let Some(c) = &metadata.collection {
                models.asset_collection.push(AssetCollection {
                    pubkey: mint,
                    collection: c.key,
                    is_collection_verified: c.verified,
                    collection_seq: None,
                    slot_updated: metadata_info.slot_updated,
                    write_version: Some(metadata_info.write_version),
                });
            }

            models.asset_authority.push(AssetAuthority {
                pubkey: mint,
                authority,
                slot_updated: metadata_info.slot_updated,
                write_version: Some(metadata_info.write_version),
            });
        }

        models
    }

    async fn mark_metadata_as_burnt(
        &self,
        metadata_slot_burnt: &HashMap<Pubkey, BurntMetadataSlot>,
    ) -> Result<(), StorageError> {
        let mtd_mint_map: Vec<MetadataMintMap> = self
            .rocks_db
            .metadata_mint_map
            .batch_get(metadata_slot_burnt.keys().cloned().collect())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let asset_dynamic_details: Vec<AssetDynamicDetails> = mtd_mint_map
            .iter()
            .map(|map| AssetDynamicDetails {
                pubkey: map.mint_key,
                is_burnt: Updated::new(
                    metadata_slot_burnt.get(&map.pubkey).unwrap().slot_updated,
                    None, // once we got burn we may not even check write version
                    true,
                ),
                ..Default::default()
            })
            .collect();

        store_assets!(
            self.rocks_db,
            asset_dynamic_details,
            self.metrics.clone(),
            asset_dynamic_data,
            "accounts_saving_dynamic"
        )
    }
}
