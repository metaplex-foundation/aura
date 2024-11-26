use crate::error::IngesterError;
use blockbuster::mpl_core::types::{Plugin, PluginAuthority, PluginType, UpdateAuthority};
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use entities::enums::{ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass};
use entities::models::{
    BurntMetadataSlot, ChainDataV1, IndexableAssetWithAccountInfo, UpdateVersion, Updated,
};
use heck::ToSnakeCase;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::{AssetCollection, AssetCompleteDetails};
use rocks_db::batch_savers::{BatchSaveStorage, MetadataModels};
use rocks_db::errors::StorageError;
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails};
use serde_json::Map;
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use usecase::save_metrics::result_to_metrics;

pub struct MplCoreProcessor {
    metrics: Arc<IngesterMetricsConfig>,
}

impl MplCoreProcessor {
    pub fn new(metrics: Arc<IngesterMetricsConfig>) -> Self {
        Self { metrics }
    }

    pub fn transform_and_store_mpl_asset(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        indexable_asset: &IndexableAssetWithAccountInfo,
    ) -> Result<(), StorageError> {
        let Some(metadata_models) = self
            .create_mpl_asset_models(storage, key, indexable_asset)
            .map_err(|e| StorageError::Common(e.to_string()))?
        else {
            return Ok(());
        };

        let begin_processing = Instant::now();
        let asset = AssetCompleteDetails::from(&metadata_models);
        let res = storage.store_metadata_models(&asset, metadata_models.metadata_mint);
        result_to_metrics(
            self.metrics.clone(),
            &res,
            "mpl_core_asset_merge_with_batch",
        );
        self.metrics.set_latency(
            "mpl_core_asset_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    pub fn create_mpl_asset_models(
        &self,
        storage: &mut BatchSaveStorage,
        asset_key: Pubkey,
        account_data: &IndexableAssetWithAccountInfo,
    ) -> Result<Option<MetadataModels>, IngesterError> {
        let mut models = MetadataModels::default();
        // Notes:
        // The address of the Core asset is used for Core Asset ID.  There are no token or mint accounts.
        // There are no `MasterEdition` or `Edition` accounts associated with Core assets.

        // Note: This indexes both Core Assets and Core Collections.
        let asset = match account_data.indexable_asset.clone() {
            MplCoreAccountData::Asset(indexable_asset) => indexable_asset,
            MplCoreAccountData::Collection(indexable_asset) => indexable_asset,
            _ => return Ok(None),
        };

        // If it is an `Address` type, use the value directly.  If it is a `Collection`, search for and
        // use the collection's authority.
        let update_authority = match asset.update_authority {
            UpdateAuthority::Address(address) => Some(address),
            UpdateAuthority::Collection(address) => storage.get_authority(address),
            UpdateAuthority::None => None,
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
            MplCoreAccountData::Asset(_) => (asset.owner, SpecificationAssetClass::MplCoreAsset),
            MplCoreAccountData::Collection(_) => (
                update_authority,
                SpecificationAssetClass::MplCoreCollection,
            ),
            _ => return Ok(None),
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

        // convert HashMap plugins into BTreeMap to have always same plugins order
        // for example without ordering 2 assets with same plugins can have different order saved in DB
        // it affects only API response and tests
        let ordered_plugins: BTreeMap<_, _> = asset.plugins
            .iter()
            .map(|(key, value)| (format!("{:?}", key), value))
            .collect();
        let mut plugins_json = serde_json::to_value(&ordered_plugins)
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
                    PluginAuthority::UpdateAuthority => update_authority,
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
            // setup update_authority only on collection updates
            let authority = if matches!(
                account_data.indexable_asset,
                MplCoreAccountData::Collection(_)
            ) {
                update_authority
            } else {
                None
            };
            models.asset_collection = Some(AssetCollection {
                pubkey: asset_key,
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
                    authority,
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

        models.asset_static = Some(AssetStaticDetails {
            pubkey: asset_key,
            specification_asset_class: class,
            royalty_target_type: RoyaltyTargetType::Creators,
            created_at: account_data.slot_updated as i64,
            edition_address: None,
        });
        if let Some(upd_auth) = update_authority {
            models.asset_authority = Some(AssetAuthority {
                pubkey: asset_key,
                authority: upd_auth,
                slot_updated: account_data.slot_updated,
                write_version: Some(account_data.write_version),
            });
        } else {
            models.asset_authority = None;
        }
        models.asset_owner = Some(AssetOwner {
            pubkey: asset_key,
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
        models.asset_dynamic = Some(AssetDynamicDetails {
            pubkey: asset_key,
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
            mint_extensions: None,
        });

        Ok(Some(models))
    }

    pub fn transform_and_store_burnt_mpl_asset(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        burnt_slot: &BurntMetadataSlot,
    ) -> Result<(), StorageError> {
        let begin_processing = Instant::now();
        let res = self.mark_mpl_asset_as_burnt(storage, key, burnt_slot);
        result_to_metrics(
            self.metrics.clone(),
            &res,
            "burn_mpl_assets_merge_with_batch",
        );
        self.metrics.set_latency(
            "burn_mpl_assets_merge_with_batch",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }

    fn mark_mpl_asset_as_burnt(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        burnt_slot: &BurntMetadataSlot,
    ) -> Result<(), StorageError> {
        storage.store_dynamic(&AssetDynamicDetails {
            pubkey: key,
            is_burnt: Updated::new(
                burnt_slot.slot_updated,
                None, // once we got burn we may not even check write version
                true,
            ),
            ..Default::default()
        })
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
                    transform_data_authority_in_object(plugin_obj);
                    transform_linked_app_data_parent_key_in_object(plugin_obj);
                }
            }
        }
        Value::Array(plugins_array) => {
            // Transform plugins in an array
            for plugin in plugins_array.iter_mut() {
                if let Some(plugin_obj) = plugin.as_object_mut() {
                    transform_authority_in_object(plugin_obj);
                    transform_data_authority_in_object(plugin_obj);
                    transform_linked_app_data_parent_key_in_object(plugin_obj);
                }
            }
        }
        _ => {}
    }
}

fn transform_authority_in_object(plugin: &mut Map<String, Value>) {
    if let Some(authority) = plugin.get_mut("authority") {
        transform_authority(authority);
    }
}

fn transform_data_authority_in_object(plugin: &mut Map<String, Value>) {
    if let Some(adapter_config) = plugin.get_mut("adapter_config") {
        if let Some(data_authority) = adapter_config
            .as_object_mut()
            .and_then(|o| o.get_mut("data_authority"))
        {
            transform_authority(data_authority);
        }
    }
}

fn transform_linked_app_data_parent_key_in_object(plugin: &mut Map<String, Value>) {
    if let Some(adapter_config) = plugin.get_mut("adapter_config") {
        if let Some(parent_key) = adapter_config
            .as_object_mut()
            .and_then(|o| o.get_mut("parent_key"))
        {
            if let Some(linked_app_data) = parent_key
                .as_object_mut()
                .and_then(|o| o.get_mut("LinkedAppData"))
            {
                transform_authority(linked_app_data);
            }
        }
    }
}

fn transform_authority(authority: &mut Value) {
    match authority {
        Value::Object(authority_obj) => {
            if let Some(authority_type) = authority_obj.keys().next().cloned() {
                // Replace the nested JSON objects with desired format.
                if let Some(Value::Object(pubkey_obj)) = authority_obj.remove(&authority_type) {
                    if let Some(address_value) = pubkey_obj.get("address") {
                        authority_obj.insert("type".to_string(), Value::from(authority_type));
                        authority_obj.insert("address".to_string(), address_value.clone());
                    }
                }
            }
        }
        Value::String(authority_type) => {
            // Handle the case where authority is a string.
            let mut authority_obj = Map::new();
            authority_obj.insert("type".to_string(), Value::String(authority_type.clone()));
            authority_obj.insert("address".to_string(), Value::Null);
            *authority = Value::Object(authority_obj);
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
