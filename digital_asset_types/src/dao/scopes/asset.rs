use std::collections::HashMap;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;

use entities::api_req_params::{AssetSortDirection, Options};
use entities::models::AssetSignatureWithPagination;
use interface::asset_sigratures::AssetSignaturesGetter;
use log::error;
use sea_orm::prelude::Json;
use sea_orm::{entity::*, query::*, ConnectionTrait, DbErr, FromQueryResult};
use solana_sdk::pubkey::Pubkey;

use rocks_db::asset::{
    AssetAuthority, AssetCollection, AssetDynamicDetails, AssetLeaf, AssetOwner, AssetSelectedMaps,
    AssetStaticDetails,
};
use rocks_db::offchain_data::OffChainData;
use rocks_db::Storage;

use crate::dao::sea_orm_active_enums::{
    ChainMutability, Mutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass,
    SpecificationVersions,
};
use crate::dao::{
    asset, asset_authority, asset_creators, asset_data, asset_grouping, AssetDataModel, FullAsset,
    GroupingSize,
};

pub const PROCESSING_METADATA_STATE: &str = "processing";
pub const COLLECTION_GROUP_KEY: &str = "collection";

pub async fn get_grouping(
    conn: &impl ConnectionTrait,
    group_key: String,
    group_value: String,
) -> Result<GroupingSize, DbErr> {
    if group_value != COLLECTION_GROUP_KEY {
        return Ok(GroupingSize { size: 0 });
    }

    let query = "SELECT COUNT(*) FROM assets_v3 WHERE ast_collection = $1 AND ast_is_collection_verified = true";

    let size = conn
        .query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            query,
            [Set(bs58::decode(group_key)
                .into_vec()
                .map_err(|e| DbErr::Custom(e.to_string()))?
                .as_slice())
            .into_value()
            .ok_or(DbErr::Custom("cannot get rows count".to_string()))?],
        ))
        .await?
        .map(|res| res.try_get::<i64>("", "count").unwrap_or_default())
        .unwrap_or_default();

    Ok(GroupingSize { size: size as u64 })
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct AssetID {
    ast_pubkey: Vec<u8>,
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct AssetPubkey {
    ast_pubkey: Vec<u8>,
}

fn convert_rocks_offchain_data(
    asset_pubkey: &Pubkey,
    offchain_data: &OffChainData,
    asset_dynamic_data: &HashMap<Pubkey, AssetDynamicDetails>,
) -> Result<AssetDataModel, DbErr> {
    let mut metadata = offchain_data.metadata.clone();

    if metadata == PROCESSING_METADATA_STATE || metadata.is_empty() {
        metadata = "{}".to_string();
    }
    let dynamic_data = asset_dynamic_data
        .get(asset_pubkey)
        .ok_or(DbErr::Custom("No relevant asset_dynamic_data".to_string()))?;

    let ch_data: serde_json::Value = serde_json::from_str(
        dynamic_data
            .onchain_data
            .clone()
            .map(|onchain_data| onchain_data.value)
            .unwrap_or_default()
            .as_ref(),
    )
    .unwrap_or(serde_json::Value::Null);

    Ok(AssetDataModel {
        asset: asset_data::Model {
            id: dynamic_data.pubkey.to_bytes().to_vec(),
            chain_data_mutability: dynamic_data
                .chain_mutability
                .clone()
                .map(|m| m.value.into())
                .unwrap_or(ChainMutability::Unknown),
            chain_data: ch_data,
            metadata_url: dynamic_data.url.value.clone(),
            metadata_mutability: Mutability::Immutable,
            metadata: Json::from_str(metadata.as_str())
                .map_err(|e| DbErr::Custom(e.to_string()))?,
            slot_updated: dynamic_data.get_slot_updated() as i64,
            reindex: None,
        },
        lamports: dynamic_data.lamports.clone().map(|v| v.value),
        executable: dynamic_data.executable.clone().map(|v| v.value),
        metadata_owner: dynamic_data.metadata_owner.clone().map(|v| v.value),
    })
}

fn convert_rocks_asset_model(
    asset_pubkey: &Pubkey,
    assets_static_data: &HashMap<Pubkey, AssetStaticDetails>,
    assets_owners: &HashMap<Pubkey, AssetOwner>,
    assets_dynamic_data: &HashMap<Pubkey, AssetDynamicDetails>,
    assets_leaf: &HashMap<Pubkey, AssetLeaf>,
) -> Result<asset::Model, DbErr> {
    let static_data = assets_static_data
        .get(asset_pubkey)
        .ok_or(DbErr::Custom("No relevant assets_static_data".to_string()))?;
    let dynamic_data = assets_dynamic_data
        .get(asset_pubkey)
        .ok_or(DbErr::Custom("No relevant asset_dynamic_data".to_string()))?;
    let owner = assets_owners
        .get(asset_pubkey)
        .ok_or(DbErr::Custom("No relevant assets_owners".to_string()))?;

    let leaf = assets_leaf
        .get(asset_pubkey)
        .cloned()
        .unwrap_or(AssetLeaf::default()); // Asset can do not have leaf, but we still can make conversion

    let tree_id = if leaf.tree_id == Pubkey::default() {
        None
    } else {
        Some(leaf.tree_id.to_bytes().to_vec())
    };
    let slot_updated = vec![owner.get_slot_updated(), leaf.slot_updated]
        .into_iter()
        .max()
        .unwrap(); // unwrap here is safe, because vec is not empty

    // there are instructions where we update only assetLeaf seq value
    // and there is burn instruction where we update only assetDynamic seq value
    // that's why we need to take max value from both of them
    let seq = {
        if dynamic_data.is_compressed.value {
            let dynamic_seq = dynamic_data
                .seq
                .clone()
                .and_then(|u| u.value.try_into().ok());
            let leaf_seq = leaf.leaf_seq.map(|seq| seq as i64);
            std::cmp::max(dynamic_seq, leaf_seq)
        } else {
            Some(0)
        }
    };

    Ok(asset::Model {
        id: static_data.pubkey.to_bytes().to_vec(),
        alt_id: None,
        specification_version: Some(SpecificationVersions::V1),
        specification_asset_class: Some(static_data.specification_asset_class.into()),
        owner: Some(owner.owner.value.to_bytes().to_vec()),
        owner_type: owner.owner_type.value.into(),
        delegate: owner.delegate.value.map(|k| k.to_bytes().to_vec()),
        frozen: dynamic_data.is_frozen.value,
        supply: dynamic_data
            .supply
            .clone()
            .map(|supply| supply.value as i64)
            .unwrap_or_default(),
        supply_mint: Some(static_data.pubkey.to_bytes().to_vec()),
        compressed: dynamic_data.is_compressed.value,
        compressible: dynamic_data.is_compressible.value,
        seq,
        tree_id,
        leaf: leaf.leaf.clone(),
        nonce: leaf.nonce.map(|nonce| nonce as i64),
        royalty_target_type: static_data.royalty_target_type.into(),
        royalty_target: None, // TODO
        royalty_amount: dynamic_data.royalty_amount.value as i32,
        asset_data: Some(static_data.pubkey.to_bytes().to_vec()),
        burnt: dynamic_data.is_burnt.value,
        created_at: Some(static_data.created_at),
        slot_updated: Some(slot_updated as i64),
        data_hash: leaf.data_hash.map(|h| h.to_string()),
        creator_hash: leaf.creator_hash.map(|h| h.to_string()),
        owner_delegate_seq: owner.owner_delegate_seq.value.map(|s| s as i64),
        was_decompressed: dynamic_data.was_decompressed.value,
        leaf_seq: leaf.leaf_seq.map(|seq| seq as i64),
    })
}

// todo: remove this and following functions as part of dropping the old db and sea orm, use the entities directly in the business logic and the specific implementations from the pg_client or rocks_client for the db interactions
impl From<entities::enums::SpecificationAssetClass> for SpecificationAssetClass {
    fn from(value: entities::enums::SpecificationAssetClass) -> Self {
        match value {
            entities::enums::SpecificationAssetClass::FungibleAsset => {
                SpecificationAssetClass::FungibleAsset
            }
            entities::enums::SpecificationAssetClass::FungibleToken => {
                SpecificationAssetClass::FungibleToken
            }
            entities::enums::SpecificationAssetClass::IdentityNft => {
                SpecificationAssetClass::IdentityNft
            }
            entities::enums::SpecificationAssetClass::Nft => SpecificationAssetClass::Nft,
            entities::enums::SpecificationAssetClass::NonTransferableNft => {
                SpecificationAssetClass::NonTransferableNft
            }
            entities::enums::SpecificationAssetClass::Print => SpecificationAssetClass::Print,
            entities::enums::SpecificationAssetClass::PrintableNft => {
                SpecificationAssetClass::PrintableNft
            }
            entities::enums::SpecificationAssetClass::ProgrammableNft => {
                SpecificationAssetClass::ProgrammableNft
            }
            entities::enums::SpecificationAssetClass::TransferRestrictedNft => {
                SpecificationAssetClass::TransferRestrictedNft
            }
            entities::enums::SpecificationAssetClass::Unknown => SpecificationAssetClass::Unknown,
        }
    }
}

impl From<entities::enums::OwnerType> for OwnerType {
    fn from(value: entities::enums::OwnerType) -> Self {
        match value {
            entities::enums::OwnerType::Single => OwnerType::Single,
            entities::enums::OwnerType::Token => OwnerType::Token,
            entities::enums::OwnerType::Unknown => OwnerType::Unknown,
        }
    }
}

impl From<entities::enums::RoyaltyTargetType> for RoyaltyTargetType {
    fn from(value: entities::enums::RoyaltyTargetType) -> Self {
        match value {
            entities::enums::RoyaltyTargetType::Creators => RoyaltyTargetType::Creators,
            entities::enums::RoyaltyTargetType::Fanout => RoyaltyTargetType::Fanout,
            entities::enums::RoyaltyTargetType::Single => RoyaltyTargetType::Single,
            entities::enums::RoyaltyTargetType::Unknown => RoyaltyTargetType::Unknown,
        }
    }
}

fn convert_rocks_authority_model(
    asset_pubkey: &Pubkey,
    assets_authority: &HashMap<Pubkey, AssetAuthority>,
) -> asset_authority::Model {
    let authority = assets_authority
        .get(asset_pubkey)
        .cloned()
        .unwrap_or(AssetAuthority::default());

    asset_authority::Model {
        id: 0,
        asset_id: asset_pubkey.to_bytes().to_vec(),
        scopes: None,
        authority: authority.authority.to_bytes().to_vec(),
        seq: authority.slot_updated as i64,
        slot_updated: authority.slot_updated as i64,
    }
}

fn convert_rocks_grouping_model(
    asset_pubkey: &Pubkey,
    assets_collection: &HashMap<Pubkey, AssetCollection>,
) -> Option<asset_grouping::Model> {
    assets_collection
        .get(asset_pubkey)
        .map(|ast| asset_grouping::Model {
            id: 0,
            asset_id: asset_pubkey.to_bytes().to_vec(),
            group_key: COLLECTION_GROUP_KEY.to_string(),
            group_value: Some(ast.collection.to_string()),
            seq: ast.collection_seq.map(|s| s as i64),
            slot_updated: Some(ast.slot_updated as i64),
            verified: Some(ast.is_collection_verified),
            group_info_seq: ast.collection_seq.map(|s| s as i64),
        })
}

fn convert_rocks_creators_model(
    asset_pubkey: &Pubkey,
    assets_dynamic_data: &HashMap<Pubkey, AssetDynamicDetails>,
) -> Vec<asset_creators::Model> {
    let dynamic_data = assets_dynamic_data
        .get(asset_pubkey)
        .cloned()
        .unwrap_or(AssetDynamicDetails::default());

    dynamic_data
        .creators
        .value
        .iter()
        .enumerate()
        .map(|(position, creator)| asset_creators::Model {
            id: 0,
            asset_id: asset_pubkey.to_bytes().to_vec(),
            creator: creator.creator.to_bytes().to_vec(),
            share: creator.creator_share as i32,
            verified: creator.creator_verified,
            seq: dynamic_data.seq.clone().map(|seq| seq.value as i64),
            slot_updated: Some(dynamic_data.get_slot_updated() as i64),
            position: position as i16,
        })
        .collect::<Vec<_>>()
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct AssetWithURL {
    ast_pubkey: Vec<u8>,
    ast_metadata_url: Option<String>,
}

// Use macros to reduce code duplications
#[macro_export]
macro_rules! fetch_asset_data {
    ($db:expr, $field:ident, $asset_ids:expr) => {{
        $db.$field
            .batch_get($asset_ids.clone())
            .await
            .map_err(|e| DbErr::Custom(e.to_string()))?
            .into_iter()
            .filter_map(|asset| asset.map(|a| (a.pubkey, a)))
            .collect::<HashMap<_, _>>()
    }};
}

fn asset_selected_maps_into_full_asset(
    id: &Pubkey,
    asset_selected_maps: &AssetSelectedMaps,
    options: &Options,
) -> Option<FullAsset> {
    if !options.show_unverified_collections {
        if let Some(collection_data) = asset_selected_maps.assets_collection.get(id) {
            if !collection_data.is_collection_verified {
                return None;
            }
        } else {
            // don't have collection data == collection unverified
            return None;
        }
    }

    let offchain_data = asset_selected_maps
        .urls
        .get(&id.to_string())
        .and_then(|url| asset_selected_maps.offchain_data.get(url).cloned())
        .unwrap_or_default();

    match convert_rocks_offchain_data(id, &offchain_data, &asset_selected_maps.assets_dynamic) {
        Ok(data) => convert_rocks_asset_model(
            id,
            &asset_selected_maps.assets_static,
            &asset_selected_maps.assets_owner,
            &asset_selected_maps.assets_dynamic,
            &asset_selected_maps.assets_leaf,
        )
        .ok()
        .map(|asset| FullAsset {
            asset,
            data,
            authorities: vec![convert_rocks_authority_model(
                id,
                &asset_selected_maps.assets_authority,
            )],
            creators: convert_rocks_creators_model(id, &asset_selected_maps.assets_dynamic),
            groups: convert_rocks_grouping_model(id, &asset_selected_maps.assets_collection)
                .map_or(vec![], |v| vec![v]),
            edition_data: asset_selected_maps
                .assets_static
                .get(id)
                .and_then(|static_details| {
                    static_details
                        .edition_address
                        .and_then(|e| asset_selected_maps.editions.get(&e).cloned())
                }),
        }),
        Err(e) => {
            error!(
                "Could not cast asset into asset data model. Key: {:?}. Error: {:?}",
                &id, e
            );
            None
        }
    }
}

pub async fn get_by_ids(
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
    options: Options,
) -> Result<Vec<Option<FullAsset>>, DbErr> {
    if asset_ids.is_empty() {
        return Ok(vec![]);
    }
    // need to pass only unique asset_ids to select query
    // index need to save order of IDs in response
    let mut unique_asset_ids_map = HashMap::new();
    for (index, id) in asset_ids.iter().enumerate() {
        unique_asset_ids_map
            .entry(*id)
            .or_insert_with(Vec::new)
            .push(index);
    }

    let unique_asset_ids: Vec<_> = unique_asset_ids_map.keys().cloned().collect();
    let asset_selected_maps = rocks_db
        .get_asset_selected_maps_async(unique_asset_ids.clone())
        .await
        .map_err(|e| DbErr::Custom(e.to_string()))?;

    let mut results = vec![None; asset_ids.len()];
    for id in unique_asset_ids {
        let res = asset_selected_maps_into_full_asset(&id, &asset_selected_maps, &options);

        if let Some(indexes) = unique_asset_ids_map.get(&id) {
            for &index in indexes {
                results[index] = res.clone();
            }
        }
    }

    Ok(results)
}

#[allow(clippy::too_many_arguments)]
pub async fn get_asset_signatures(
    storage: Arc<Storage>,
    asset_id: Option<Pubkey>,
    tree_id: Option<Pubkey>,
    leaf_idx: Option<u64>,
    page: Option<u64>,
    before: &Option<String>,
    after: &Option<String>,
    limit: u64,
    sort_direction: Option<AssetSortDirection>,
) -> Result<AssetSignatureWithPagination, DbErr> {
    let before_sequence = before.as_ref().and_then(|b| b.parse::<u64>().ok());
    let after_sequence = after.as_ref().and_then(|a| a.parse::<u64>().ok());
    if let (Some(before_sequence), Some(after_sequence)) = (before_sequence, after_sequence) {
        let invalid_range = match sort_direction {
            Some(AssetSortDirection::Asc) => before_sequence <= after_sequence,
            _ => before_sequence >= after_sequence,
        };
        if invalid_range {
            return Ok(AssetSignatureWithPagination::default());
        }
    }
    let sort_direction = sort_direction.unwrap_or(AssetSortDirection::Desc);
    let (tree_id, leaf_idx) = match (tree_id, leaf_idx, asset_id) {
        (Some(tree_id), Some(leaf_idx), None) => {
            // Directly use tree_id and leaf_idx if both are provided
            (tree_id, leaf_idx)
        }
        (None, None, Some(asset_id)) => {
            // if only asset_id is provided, fetch the latest tree and leaf_idx (asset.nonce) for the asset
            // and use them to fetch transactions
            let asset_leaf = storage
                .asset_leaf_data
                .get(asset_id)
                .map_err(|e| DbErr::Custom(e.to_string()))?
                .ok_or_else(|| DbErr::RecordNotFound("Leaf ID does not exist".to_string()))?;
            (
                asset_leaf.tree_id,
                asset_leaf.nonce.ok_or_else(|| {
                    DbErr::RecordNotFound("Leaf nonce does not exist".to_string())
                })?,
            )
        }
        _ => {
            // If neither set of parameters is provided, return an error
            return Err(DbErr::Custom(
                "Either 'id' or both 'tree' and 'leafIndex' must be provided".to_string(),
            ));
        }
    };

    Ok(storage.get_asset_signatures(
        tree_id,
        leaf_idx,
        before_sequence,
        after_sequence,
        page,
        sort_direction,
        limit,
    ))
}
