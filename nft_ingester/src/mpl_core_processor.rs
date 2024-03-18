use crate::buffer::Buffer;
use crate::db_v2::{DBClient, Task};
use crate::error::IngesterError;
use crate::mplx_updates_processor::{
    result_to_metrics, BurntMetadataSlot, CompressedProofWithWriteVersion, RocksMetadataModels,
    MAX_BUFFERED_TASKS_TO_TAKE,
};
use crate::{process_accounts, save_rocks_models, store_assets};
use blockbuster::mpl_core::types::{Authority, Plugin, UpdateAuthority};
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use entities::enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::PubkeyWithSlot;
use entities::models::{ChainDataV1, UpdateVersion, Updated};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::AssetCollection;
use rocks_db::asset::MetadataMintMap;
use rocks_db::errors::StorageError;
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use serde_json::json;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tracing::error;

#[derive(Clone)]
pub struct MplCoreProcessor {
    pub rocks_db: Arc<Storage>,
    pub db_client: Arc<DBClient>,
    pub batch_size: usize,

    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
    last_received_burn_at: Option<SystemTime>,
    last_received_mpl_asset_at: Option<SystemTime>,
}

impl MplCoreProcessor {
    pub fn new(
        rocks_db: Arc<Storage>,
        db_client: Arc<DBClient>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
            db_client,
            buffer,
            metrics,
            batch_size,
            last_received_burn_at: None,
            last_received_mpl_asset_at: None,
        }
    }

    pub async fn process_mpl_asset_burn(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.burnt_mpl_core_at_slot,
            self.batch_size,
            |s: BurntMetadataSlot| s,
            self.last_received_burn_at,
            Self::transform_and_store_burnt_mpl_assets,
            "burn_metadata"
        );
    }

    pub async fn process_mpl_assets(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.mpl_core_compressed_proofs,
            self.batch_size,
            |s: CompressedProofWithWriteVersion| s,
            self.last_received_mpl_asset_at,
            Self::transform_and_store_mpl_assets,
            "mpl_core_asset"
        );
    }

    pub async fn transform_and_store_mpl_assets(
        &self,
        metadata_info: &HashMap<Pubkey, CompressedProofWithWriteVersion>,
    ) {
        let metadata_models = match self.create_mpl_asset_models(metadata_info).await {
            Ok(metadata_models) => metadata_models,
            Err(e) => {
                error!("create_mpl_asset_models: {}", e);
                return;
            }
        };

        let begin_processing = Instant::now();
        self.store_metadata_models(&metadata_models).await;
        self.metrics.set_latency(
            "mpl_core_asset",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn create_mpl_asset_models(
        &self,
        mpl_assets: &HashMap<Pubkey, CompressedProofWithWriteVersion>,
    ) -> Result<RocksMetadataModels, IngesterError> {
        let mut models = RocksMetadataModels::default();
        for (asset_key, account_data) in mpl_assets.iter() {
            // Notes:
            // The address of the Core asset is used for Core Asset ID.  There are no token or mint accounts.
            // There are no `MasterEdition` or `Edition` accounts associated with Core assets.

            // Note: This now indexes both Core assets and Core collections.  Long term I don't think
            // we will necessarily use this `CompressionProof` type as input to DAS but we are still
            // working out how to index plugins properly.
            let (full_asset, owner) = match account_data.proof.clone() {
                MplCoreAccountData::FullAsset(compression_proof) => {
                    (compression_proof.clone(), Some(compression_proof.owner))
                }
                MplCoreAccountData::Collection(compression_proof) => (compression_proof, None),
                _ => return Err(IngesterError::NotImplemented),
            };

            let update_authority = match full_asset.update_authority {
                UpdateAuthority::Address(address) => address,
                UpdateAuthority::Collection(address) => address,
                UpdateAuthority::None => Pubkey::default(),
            };

            let name = full_asset.name.clone();
            let uri = full_asset.uri.trim().replace('\0', "");

            // Notes:
            // There is no symbol for a Core asset.
            // Edition nonce hardcoded to `None`.
            // There is no primary sale concept for Core Assets, hardcoded to `false`.
            // Token standard is hardcoded to `None`.
            let mut chain_data = ChainDataV1 {
                name: full_asset.name.clone(),
                symbol: "".to_string(),
                edition_nonce: None,
                primary_sale_happened: false,
                token_standard: None,
                uses: None,
            };

            chain_data.sanitize();
            let chain_data_json = json!(chain_data);

            // Note:
            // Mutability set based on core asset data having an update authority.
            // Individual plugins could have some or no authority giving them individual mutability status.
            let chain_mutability = match full_asset.update_authority {
                UpdateAuthority::None => ChainMutability::Immutable,
                _ => ChainMutability::Mutable,
            };

            let ownership_type = OwnerType::Single;
            let class = SpecificationAssetClass::Nft;

            // Get seller fee basis points from Royalties plugin if available.
            let royalty_amount = full_asset
                .plugins
                .iter()
                .find_map(|hashable_plugin| match &hashable_plugin.plugin {
                    Plugin::Royalties(royalties) => Some(royalties.basis_points as i32),
                    _ => None,
                })
                .unwrap_or(0);

            let plugins_json = json!(full_asset.plugins);
            let supply = 1;

            // Get transfer delegate from transfer plugin if available.
            let transfer_delegate = full_asset
                .plugins
                .iter()
                .find_map(|hashable_plugin| match &hashable_plugin.plugin {
                    Plugin::Transfer(_) => Some(&hashable_plugin.authority),
                    _ => None,
                })
                .and_then(|authority| match authority {
                    Authority::Owner => owner,
                    Authority::UpdateAuthority => Some(update_authority),
                    Authority::Pubkey { address } => Some(*address),
                    Authority::None => None,
                });

            // Get freeze delegate and frozen status from transfer plugin if available.
            let (freeze_delegate, frozen) = full_asset
                .plugins
                .iter()
                .find_map(|hashable_plugin| match &hashable_plugin.plugin {
                    Plugin::Freeze(freeze) => Some((&hashable_plugin.authority, freeze.frozen)),
                    _ => None,
                })
                .map(|(authority, frozen)| match authority {
                    Authority::Owner => (owner, frozen),
                    Authority::UpdateAuthority => (Some(update_authority), frozen),
                    Authority::Pubkey { address } => (Some(*address), frozen),
                    Authority::None => (None, false),
                })
                .unwrap_or_else(|| (None, false));

            // Get update delegate from transfer plugin if available.
            let update_delegate = full_asset
                .plugins
                .iter()
                .find_map(|hashable_plugin| match &hashable_plugin.plugin {
                    Plugin::UpdateDelegate(_) => Some(&hashable_plugin.authority),
                    _ => None,
                })
                .and_then(|authority| match authority {
                    Authority::Owner => owner,
                    Authority::UpdateAuthority => Some(update_authority),
                    Authority::Pubkey { address } => Some(*address),
                    Authority::None => None,
                });

            if let UpdateAuthority::Collection(address) = full_asset.update_authority {
                models.asset_collection.push(AssetCollection {
                    pubkey: *asset_key,
                    collection: address,
                    is_collection_verified: true,
                    slot_updated: account_data.slot_updated,
                    collection_seq: None,
                    write_version: Some(account_data.write_version),
                });
            }

            let creators = full_asset.plugins.iter().find_map(|hashable_plugin| {
                match &hashable_plugin.plugin {
                    Plugin::Royalties(royalties) => Some(&royalties.creators),
                    _ => None,
                }
            });
            let updated_creators = if let Some(creators) = creators {
                let parsed = creators
                    .iter()
                    .map(|creator| entities::models::Creator {
                        creator: creator.address,
                        creator_verified: true,
                        creator_share: creator.percentage,
                    })
                    .collect::<Vec<_>>();
                Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    parsed,
                )
            } else {
                Updated::default()
            };

            models.asset_static.push(AssetStaticDetails {
                pubkey: *asset_key,
                specification_asset_class: class,
                royalty_target_type: RoyaltyTargetType::Creators,
                created_at: account_data.slot_updated as i64,
                edition_address: None,
            });
            models.asset_authority.push(AssetAuthority {
                pubkey: *asset_key,
                authority: update_authority,
                slot_updated: account_data.slot_updated,
                write_version: Some(account_data.write_version),
            });
            models.asset_owner.push(AssetOwner {
                pubkey: *asset_key,
                owner: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    owner,
                ),
                // Note use transfer delegate for the existing delegate field.
                delegate: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    transfer_delegate,
                ),
                owner_type: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    ownership_type,
                ),
                ..Default::default()
            });
            if !uri.is_empty() {
                models.tasks.push(Task {
                    ofd_metadata_url: uri.clone(),
                    ofd_locked_until: Some(chrono::Utc::now()),
                    ofd_attempts: 0,
                    ofd_max_attempts: 10,
                    ofd_error: None,
                    ofd_status: Default::default(),
                });
            }
            models.asset_dynamic.push(AssetDynamicDetails {
                pubkey: *asset_key,
                is_compressible: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    false,
                ),
                is_compressed: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    false,
                ),
                is_frozen: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    frozen,
                ),
                supply: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    supply,
                )),
                // In reference seq set to also set to Some(0)
                seq: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    0,
                )),
                is_burnt: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    false,
                ),
                was_decompressed: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    false,
                ),
                onchain_data: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    chain_data_json.to_string(),
                )),
                creators: updated_creators,
                royalty_amount: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    royalty_amount as u16,
                ),
                url: Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    uri,
                ),
                chain_mutability: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    chain_mutability,
                )),
                lamports: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    account_data.lamports,
                )),
                executable: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    account_data.executable,
                )),
                metadata_owner: None,
                raw_name: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    name,
                )),
                transfer_delegate: transfer_delegate.map(|transfer_delegate| {
                    Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        transfer_delegate,
                    )
                }),
                freeze_delegate: freeze_delegate.map(|freeze_delegate| {
                    Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        freeze_delegate,
                    )
                }),
                update_delegate: update_delegate.map(|update_delegate| {
                    Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        update_delegate,
                    )
                }),
                plugins: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    plugins_json.to_string(),
                )),
            })
        }

        Ok(models)
    }

    pub async fn transform_and_store_burnt_mpl_assets(
        &self,
        metadata_info: &HashMap<Pubkey, BurntMetadataSlot>,
    ) {
        let begin_processing = Instant::now();
        if let Err(e) = self.mark_mpl_asset_as_burnt(metadata_info).await {
            error!("Mark mpl assets as burnt: {}", e);
        }
        self.metrics.set_latency(
            "burn_mpl_assets",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn mark_mpl_asset_as_burnt(
        &self,
        mpl_assets_slot_burnt: &HashMap<Pubkey, BurntMetadataSlot>,
    ) -> Result<(), StorageError> {
        let asset_dynamic_details: Vec<AssetDynamicDetails> = mpl_assets_slot_burnt
            .iter()
            .map(|(pubkey, slot)| AssetDynamicDetails {
                pubkey: *pubkey,
                is_burnt: Updated::new(
                    slot.slot_updated,
                    None, // once we got burn we may not even check write version
                    true,
                ),
                ..Default::default()
            })
            .collect();

        store_assets!(
            self,
            asset_dynamic_details,
            asset_dynamic_data,
            "accounts_saving_dynamic"
        )
    }

    save_rocks_models!();
}
