use crate::buffer::Buffer;
use crate::error::IngesterError;
use crate::mplx_updates_processor::{BurntMetadataSlot, IndexableAssetWithAccountInfo};
use crate::process_accounts;
use blockbuster::mpl_core::types::{Plugin, PluginAuthority, PluginType, UpdateAuthority};
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use entities::enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::{ChainDataV1, UpdateVersion, Updated};
use heck::ToSnakeCase;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::AssetCollection;
use rocks_db::batch_savers::MetadataModels;
use rocks_db::errors::StorageError;
use rocks_db::{
    store_assets, AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage,
};
use serde_json::Map;
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::broadcast::Receiver;
use tracing::error;
use usecase::save_metrics::result_to_metrics;

#[derive(Clone)]
pub struct MplCoreProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,

    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
    last_received_burn_at: Option<SystemTime>,
    last_received_mpl_asset_at: Option<SystemTime>,
}

impl MplCoreProcessor {
    pub fn new(
        rocks_db: Arc<Storage>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
            buffer,
            metrics,
            batch_size,
            last_received_burn_at: None,
            last_received_mpl_asset_at: None,
        }
    }

    pub async fn process_mpl_asset_burn(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
            self.buffer.burnt_mpl_core_at_slot,
            self.batch_size,
            |s: BurntMetadataSlot| s,
            self.last_received_burn_at,
            Self::transform_and_store_burnt_mpl_assets,
            "burn_metadata"
        );
    }

    pub async fn process_mpl_assets(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
            self.buffer.mpl_core_indexable_assets,
            self.batch_size,
            |s: IndexableAssetWithAccountInfo| s,
            self.last_received_mpl_asset_at,
            Self::transform_and_store_mpl_assets,
            "mpl_core_asset"
        );
    }

    pub async fn transform_and_store_mpl_assets(
        &self,
        metadata_info: &HashMap<Pubkey, IndexableAssetWithAccountInfo>,
    ) {
        let metadata_models = match self.create_mpl_asset_models(metadata_info).await {
            Ok(metadata_models) => metadata_models,
            Err(e) => {
                error!("create_mpl_asset_models: {}", e);
                return;
            }
        };

        let begin_processing = Instant::now();
        self.rocks_db
            .store_metadata_models(&metadata_models, self.metrics.clone())
            .await;
        self.metrics.set_latency(
            "mpl_core_asset",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn create_mpl_asset_models(
        &self,
        mpl_assets: &HashMap<Pubkey, IndexableAssetWithAccountInfo>,
    ) -> Result<MetadataModels, IngesterError> {
        let mut models = MetadataModels::default();
        for (asset_key, account_data) in mpl_assets.iter() {
            // Notes:
            // The address of the Core asset is used for Core Asset ID.  There are no token or mint accounts.
            // There are no `MasterEdition` or `Edition` accounts associated with Core assets.

            // Note: This indexes both Core Assets and Core Collections.
            let asset = match account_data.indexable_asset.clone() {
                MplCoreAccountData::Asset(indexable_asset) => indexable_asset,
                MplCoreAccountData::Collection(indexable_asset) => indexable_asset,
                _ => continue,
            };

            // If it is an `Address` type, use the value directly.  If it is a `Collection`, search for and
            // use the collection's authority.
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
            let (owner, class) = match account_data.indexable_asset.clone() {
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
                .get(&PluginType::Royalties)
                .and_then(|plugin_schema| {
                    if let Plugin::Royalties(royalties) = &plugin_schema.data {
                        Some((royalties.basis_points, &royalties.creators))
                    } else {
                        None
                    }
                })
                .unwrap_or((0, &default_creators));

            let mut plugins_json = serde_json::to_value(&asset.plugins)
                .map_err(|e| IngesterError::DeserializationError(e.to_string()))?;

            // Improve JSON output.
            remove_plugins_nesting(&mut plugins_json, "data");
            transform_plugins_authority(&mut plugins_json);
            convert_keys_to_snake_case(&mut plugins_json);

            // Serialize known external plugins into JSON.
            let mut external_plugins_json = serde_json::to_value(&asset.external_plugins)
                .map_err(|e| IngesterError::DeserializationError(e.to_string()))?;

            // Improve JSON output.
            remove_plugins_nesting(&mut external_plugins_json, "adapter_config");
            transform_plugins_authority(&mut external_plugins_json);
            convert_keys_to_snake_case(&mut external_plugins_json);

            // Serialize any unknown external plugins into JSON.
            let unknown_external_plugins_json = if !asset.unknown_external_plugins.is_empty() {
                let mut unknown_external_plugins_json =
                    serde_json::to_value(&asset.unknown_external_plugins)
                        .map_err(|e| IngesterError::DeserializationError(e.to_string()))?;

                // Improve JSON output.
                transform_plugins_authority(&mut unknown_external_plugins_json);
                convert_keys_to_snake_case(&mut unknown_external_plugins_json);

                Some(unknown_external_plugins_json)
            } else {
                None
            };

            let supply = 1;
            // Serialize any uknown plugins into JSON.
            let unknown_plugins_json = if !asset.unknown_plugins.is_empty() {
                let mut unknown_plugins_json = serde_json::to_value(&asset.unknown_plugins)
                    .map_err(|e| IngesterError::DeserializationError(e.to_string()))?;

                transform_plugins_authority(&mut unknown_plugins_json);
                convert_keys_to_snake_case(&mut unknown_plugins_json);

                Some(unknown_plugins_json)
            } else {
                None
            };

            // Get transfer delegate from `TransferDelegate` plugin if available.
            let transfer_delegate =
                asset
                    .plugins
                    .get(&PluginType::TransferDelegate)
                    .and_then(|plugin_schema| match &plugin_schema.authority {
                        PluginAuthority::Owner => owner,
                        PluginAuthority::UpdateAuthority => Some(update_authority),
                        PluginAuthority::Address { address } => Some(*address),
                        PluginAuthority::None => None,
                    });

            // Get frozen status from `FreezeDelegate` plugin if available.
            let frozen = asset
                .plugins
                .get(&PluginType::FreezeDelegate)
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
                    collection: Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        address,
                    ),
                    is_collection_verified: Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        true,
                    ),
                    authority: Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        Some(update_authority),
                    ),
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
                mpl_core_plugins: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    plugins_json.to_string(),
                )),
                mpl_core_unknown_plugins: unknown_plugins_json.map(|unknown_plugins_json| {
                    Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        unknown_plugins_json.to_string(),
                    )
                }),
                num_minted: asset.num_minted.map(|num_minted| {
                    Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        num_minted,
                    )
                }),
                rent_epoch: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    account_data.rent_epoch,
                )),
                current_size: asset.current_size.map(|current_size| {
                    Updated::new(
                        account_data.slot_updated,
                        Some(UpdateVersion::WriteVersion(account_data.write_version)),
                        current_size,
                    )
                }),
                plugins_json_version: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    1,
                )),
                mpl_core_external_plugins: Some(Updated::new(
                    account_data.slot_updated,
                    Some(UpdateVersion::WriteVersion(account_data.write_version)),
                    external_plugins_json.to_string(),
                )),
                mpl_core_unknown_external_plugins: unknown_external_plugins_json.map(
                    |unknown_external_plugins_json| {
                        Updated::new(
                            account_data.slot_updated,
                            Some(UpdateVersion::WriteVersion(account_data.write_version)),
                            unknown_external_plugins_json.to_string(),
                        )
                    },
                ),
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
            self.rocks_db,
            asset_dynamic_details,
            self.metrics.clone(),
            asset_dynamic_data,
            "accounts_saving_dynamic"
        )
    }
}

// Modify the JSON structure to remove the `Plugin` name and just display its data.
// For example, this will transform `FreezeDelegate` JSON from:
// "data":{"freeze_delegate":{"frozen":false}}}
// to:
// "data":{"frozen":false}
fn remove_plugins_nesting(plugins_json: &mut Value, nested_key: &str) {
    match plugins_json {
        Value::Object(plugins) => {
            // Handle the case where plugins_json is an object.
            for (_, plugin) in plugins.iter_mut() {
                remove_nesting_from_plugin(plugin, nested_key);
            }
        }
        Value::Array(plugins_array) => {
            // Handle the case where plugins_json is an array.
            for plugin in plugins_array.iter_mut() {
                remove_nesting_from_plugin(plugin, nested_key);
            }
        }
        _ => {}
    }
}

fn remove_nesting_from_plugin(plugin: &mut Value, nested_key: &str) {
    if let Some(Value::Object(nested_key)) = plugin.get_mut(nested_key) {
        // Extract the plugin data and remove it.
        if let Some((_, inner_plugin_data)) = nested_key.iter().next() {
            let inner_plugin_data_clone = inner_plugin_data.clone();
            // Clear the `nested_key` object.
            nested_key.clear();
            // Move the plugin data fields to the top level of `nested_key`.
            if let Value::Object(inner_plugin_data) = inner_plugin_data_clone {
                for (field_name, field_value) in inner_plugin_data.iter() {
                    nested_key.insert(field_name.clone(), field_value.clone());
                }
            }
        }
    }
}

// Modify the JSON for `PluginAuthority` to have consistent output no matter the enum type.
// For example, from:
// "authority":{"Address":{"address":"D7whDWAP5gN9x4Ff6T9MyQEkotyzmNWtfYhCEWjbUDBM"}}
// to:
// "authority":{"address":"4dGxsCAwSCopxjEYY7sFShFUkfKC6vzsNEXJDzFYYFXh","type":"Address"}
// and from:
// "authority":"UpdateAuthority"
// to:
// "authority":{"address":null,"type":"UpdateAuthority"}
fn transform_plugins_authority(plugins_json: &mut Value) {
    match plugins_json {
        Value::Object(plugins) => {
            // Transform plugins in an object
            for (_, plugin) in plugins.iter_mut() {
                if let Some(plugin_obj) = plugin.as_object_mut() {
                    transform_authority_in_object(plugin_obj);
                }
            }
        }
        Value::Array(plugins_array) => {
            // Transform plugins in an array
            for plugin in plugins_array.iter_mut() {
                if let Some(plugin_obj) = plugin.as_object_mut() {
                    transform_authority_in_object(plugin_obj);
                }
            }
        }
        _ => {}
    }
}

// Helper for `transform_plugins_authority` logic.
fn transform_authority_in_object(plugin: &mut Map<String, Value>) {
    match plugin.get_mut("authority") {
        Some(Value::Object(authority)) => {
            if let Some(authority_type) = authority.keys().next().cloned() {
                // Replace the nested JSON objects with desired format.
                if let Some(Value::Object(pubkey_obj)) = authority.remove(&authority_type) {
                    if let Some(address_value) = pubkey_obj.get("address") {
                        authority.insert("type".to_string(), Value::from(authority_type));
                        authority.insert("address".to_string(), address_value.clone());
                    }
                }
            }
        }
        Some(Value::String(authority_type)) => {
            // Handle the case where authority is a string.
            let mut authority_obj = Map::new();
            authority_obj.insert("type".to_string(), Value::String(authority_type.clone()));
            authority_obj.insert("address".to_string(), Value::Null);
            plugin.insert("authority".to_string(), Value::Object(authority_obj));
        }
        _ => {}
    }
}

// Convert all keys to snake case.  Ignore values that aren't JSON objects themselves.
fn convert_keys_to_snake_case(plugins_json: &mut Value) {
    match plugins_json {
        Value::Object(obj) => {
            let keys = obj.keys().cloned().collect::<Vec<String>>();
            for key in keys {
                let snake_case_key = key.to_snake_case();
                if let Some(val) = obj.remove(&key) {
                    obj.insert(snake_case_key, val);
                }
            }
            for (_, val) in obj.iter_mut() {
                convert_keys_to_snake_case(val);
            }
        }
        Value::Array(arr) => {
            for val in arr {
                convert_keys_to_snake_case(val);
            }
        }
        _ => {}
    }
}
