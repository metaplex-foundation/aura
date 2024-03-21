use crate::buffer::Buffer;
use crate::db_v2::{DBClient, Task};
use crate::error::IngesterError;
use crate::mplx_updates_processor::{
    result_to_metrics, BurntMetadataSlot, CompressedProofWithWriteVersion, RocksMetadataModels,
    MAX_BUFFERED_TASKS_TO_TAKE,
};
use crate::{process_accounts, save_rocks_models, store_assets};
use blockbuster::mpl_core::types::{Plugin, PluginAuthority, PluginType, UpdateAuthority};
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use entities::enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::PubkeyWithSlot;
use entities::models::{ChainDataV1, UpdateVersion, Updated};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::AssetCollection;
use rocks_db::asset::MetadataMintMap;
use rocks_db::errors::StorageError;
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use serde_json::{json, Value};
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

            // Note: This indexes both Core Assets and Core Collections.
            let asset = match account_data.proof.clone() {
                MplCoreAccountData::Asset(indexable_asset) => indexable_asset,
                MplCoreAccountData::Collection(indexable_asset) => indexable_asset,
                _ => continue,
            };

            let update_authority = match asset.update_authority {
                UpdateAuthority::Address(address) => address,
                UpdateAuthority::Collection(address) => self
                    .rocks_db
                    .asset_authority_data
                    .get(address)
                    .unwrap_or(None)
                    .map(|authority| authority.authority)
                    .unwrap_or_default(),
                UpdateAuthority::None => Pubkey::default(),
            };

            let name = asset.name.clone();
            let uri = asset.uri.trim().replace('\0', "");

            // Notes:
            // There is no symbol for a Core asset.
            // Edition nonce hardcoded to `None`.
            // There is no primary sale concept for Core Assets, hardcoded to `false`.
            // Token standard is hardcoded to `None`.
            let mut chain_data = ChainDataV1 {
                name: asset.name.clone(),
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
            let chain_mutability = match asset.update_authority {
                UpdateAuthority::None => ChainMutability::Immutable,
                _ => ChainMutability::Mutable,
            };

            let ownership_type = OwnerType::Single;
            let (owner, class) = match account_data.proof.clone() {
                MplCoreAccountData::Asset(_) => {
                    (asset.owner, SpecificationAssetClass::MplCoreAsset)
                }
                MplCoreAccountData::Collection(_) => (
                    Some(update_authority),
                    SpecificationAssetClass::MplCoreCollection,
                ),
                _ => continue,
            };

            // Get royalty amount and creators from `Royalties` plugin if available.
            let default_creators = Vec::new();
            let (royalty_amount, creators) = asset
                .plugins
                .get(&PluginType::Royalties.to_string())
                .and_then(|plugin_schema| {
                    if let Plugin::Royalties(royalties) = &plugin_schema.data {
                        Some((royalties.basis_points, &royalties.creators))
                    } else {
                        None
                    }
                })
                .unwrap_or((0, &default_creators));

            let plugins_json = remove_plugins_nesting(json!(asset.plugins));
            let supply = 1;
            let unknown_plugins_json = json!(asset.unknown_plugins);

            // Get transfer delegate from `TransferDelegate` plugin if available.
            let transfer_delegate = asset
                .plugins
                .get(&PluginType::TransferDelegate.to_string())
                .and_then(|plugin_schema| match &plugin_schema.authority {
                    PluginAuthority::Owner => owner,
                    PluginAuthority::UpdateAuthority => Some(update_authority),
                    PluginAuthority::Pubkey { address } => Some(*address),
                    PluginAuthority::None => None,
                });

            // Get frozen status from `FreezeDelegate` plugin if available.
            let frozen = asset
                .plugins
                .get(&PluginType::FreezeDelegate.to_string())
                .and_then(|plugin_schema| {
                    if let Plugin::FreezeDelegate(freeze_delegate) = &plugin_schema.data {
                        Some(freeze_delegate.frozen)
                    } else {
                        None
                    }
                })
                .unwrap_or(false);

            if let UpdateAuthority::Collection(address) = asset.update_authority {
                models.asset_collection.push(AssetCollection {
                    pubkey: *asset_key,
                    collection: address,
                    is_collection_verified: true,
                    slot_updated: account_data.slot_updated,
                    collection_seq: None,
                    write_version: Some(account_data.write_version),
                });
            }

            let updated_creators = Updated::new(
                account_data.slot_updated,
                Some(UpdateVersion::WriteVersion(account_data.write_version)),
                creators
                    .iter()
                    .map(|creator| entities::models::Creator {
                        creator: creator.address,
                        creator_verified: true,
                        creator_share: creator.percentage,
                    })
                    .collect::<Vec<_>>(),
            );

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
                    royalty_amount,
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
                plugins: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    plugins_json.to_string(),
                )),
                unknown_plugins: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    unknown_plugins_json.to_string(),
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

// Modify the JSON structure to remove the Plugin name and just display its data.
// For example, this will transform `FreezeDelegate` JSON from:
// {"data": {"freeze_delegate": {"frozen": false}}}
// to:
// {"data": {"frozen": false}}
//
// Also replaces "authority": {"Pubkey": ...} with "authority": {"pubkey": ...}
fn remove_plugins_nesting(mut parsed_json: Value) -> Value {
    if let Some(plugins) = parsed_json.as_object_mut() {
        for (_, plugin) in plugins.iter_mut() {
            // Convert "Pubkey" key to "pubkey" under "authority".
            if let Some(Value::Object(authority)) = plugin.get_mut("authority") {
                if let Some(pubkey_value) = authority.remove("Pubkey") {
                    authority.insert("pubkey".to_string(), pubkey_value);
                }
            }
            if let Some(Value::Object(data)) = plugin.get_mut("data") {
                // Extract the plugin data and remove it.
                if let Some((_, inner_plugin_data)) = data.iter().next() {
                    let inner_plugin_data_clone = inner_plugin_data.clone();
                    // Clear the "data" object.
                    data.clear();
                    // Move the plugin data fields to the top level of "data".
                    if let Value::Object(inner_plugin_data) = inner_plugin_data_clone {
                        for (field_name, field_value) in inner_plugin_data.iter() {
                            data.insert(field_name.clone(), field_value.clone());
                        }
                    }
                }
            }
        }
    }
    parsed_json
}
