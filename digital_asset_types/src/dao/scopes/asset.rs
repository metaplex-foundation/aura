use crate::dao::{
    asset, asset_authority, asset_creators, asset_data, asset_grouping, FullAsset, GroupingSize,
    Pagination,
};

use crate::dao::asset::Column;
use crate::dao::sea_orm_active_enums::{
    ChainMutability, Mutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass,
    SpecificationVersions,
};

use log::error;
use rocks_db::asset::{
    AssetAuthority, AssetCollection, AssetDynamicDetails, AssetLeaf, AssetOwner, AssetStaticDetails,
};
use rocks_db::offchain_data::OffChainData;
use rocks_db::Storage;
use sea_orm::prelude::{DateTimeWithTimeZone, Json};
use sea_orm::{entity::*, query::*, ConnectionTrait, DbErr, FromQueryResult, Order};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;

pub const PROCESSING_METADATA_STATE: &str = "processing";

pub fn paginate(
    pagination: &Pagination,
    limit: u64,
    condition: &str,
    values: Vec<Value>,
) -> Result<(String, Vec<Value>, Option<u64>), DbErr> {
    let mut condition = condition.to_string();
    let mut values = values;
    let mut offset = None;
    match pagination {
        Pagination::Keyset { before, after } => {
            if let Some(b) = before {
                values.push(Set(b.as_slice()).into_value().ok_or(DbErr::Custom(format!(
                    "cannot get value from before: {:?}",
                    b
                )))?);
                condition = format!("{} AND ast_pubkey < ${}", condition, values.len());
            }
            if let Some(a) = after {
                values.push(Set(a.as_slice()).into_value().ok_or(DbErr::Custom(format!(
                    "cannot get value from after: {:?}",
                    a
                )))?);
                condition = format!("{} AND ast_pubkey > ${}", condition, values.len());
            }
        }
        Pagination::Page { page } => {
            if *page > 0 {
                offset = Some((page - 1) * limit);
            }
        }
    }

    Ok((condition.to_string(), values, offset))
}

pub async fn get_by_creator(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    creator: Vec<u8>,
    only_verified: bool,
    sort_by: Option<Column>,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
) -> Result<Vec<FullAsset>, DbErr> {
    let mut condition = "SELECT asc_pubkey FROM assets_v3 WHERE asc_creator = $1".to_string();
    if only_verified {
        condition = format!("{} AND asc_verified = true", condition);
    }
    let values = vec![Set(creator.as_slice())
        .into_value()
        .ok_or(DbErr::Custom(format!(
            "cannot get value from creator: {:?}",
            creator
        )))?];

    get_by_related_condition(
        conn,
        rocks_db,
        &condition,
        values,
        sort_by,
        sort_direction,
        pagination,
        limit,
        true,
    )
    .await
}

pub async fn get_grouping(
    conn: &impl ConnectionTrait,
    group_key: String,
    group_value: String,
) -> Result<GroupingSize, DbErr> {
    if group_value != "collection".to_string() {
        return Ok(GroupingSize { size: 0 });
    }

    let query = "SELECT COUNT(*) FROM assets_v3 WHERE ast_collection = $1 AND ast_is_collection_verified = true";

    let size = conn
        .query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            &query,
            [Set(bs58::decode(group_key)
                .into_vec()
                .map_err(|e| DbErr::Custom(e.to_string()))?
                .as_slice())
            .into_value()
            .ok_or(DbErr::Custom("cannot get rows count".to_string()))?],
        ))
        .await?
        .iter()
        .map(|res| res.try_get::<i64>("", "count").unwrap_or_default())
        .collect::<Vec<_>>()
        .last()
        .map(|i| *i)
        .unwrap_or_default();

    Ok(GroupingSize { size: size as u64 })
}

pub async fn get_by_grouping(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    group_value: Vec<u8>,
    group_key: String,
    sort_by: Option<Column>,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
) -> Result<Vec<FullAsset>, DbErr> {
    if group_key != "collection".to_string() {
        return Ok(vec![]);
    }

    let condition = "SELECT ast_pubkey FROM assets_v3 WHERE ast_collection = $1 AND ast_is_collection_verified = true";
    let values = vec![Set(group_value.clone())
        .into_value()
        .ok_or(DbErr::Custom(format!(
            "cannot get value from group_key: {:?}",
            group_value
        )))?];

    get_by_related_condition(
        conn,
        rocks_db,
        condition,
        values,
        sort_by,
        sort_direction,
        pagination,
        limit,
        false,
    )
    .await
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct AssetID {
    ast_pubkey: Vec<u8>,
}

pub async fn get_assets_by_owner(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    owner: Pubkey,
    sort_by: Option<Column>,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
) -> Result<Vec<FullAsset>, DbErr> {
    let condition = "SELECT ast_pubkey FROM assets_v3 WHERE ast_owner = $1";
    let values = vec![Set(owner.to_bytes().to_vec().as_slice())
        .into_value()
        .ok_or(DbErr::Custom(format!(
            "cannot get value from owner: {:?}",
            owner
        )))?];

    get_by_related_condition(
        conn,
        rocks_db,
        &condition,
        values,
        sort_by,
        sort_direction,
        pagination,
        limit,
        false,
    )
    .await
}

pub async fn get_by_authority(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    authority: Vec<u8>,
    sort_by: Option<asset::Column>,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
) -> Result<Vec<FullAsset>, DbErr> {
    let condition = "SELECT ast_pubkey FROM assets_v3 WHERE ast_authority = $1";
    let values = vec![Set(authority.as_slice())
        .into_value()
        .ok_or(DbErr::Custom(format!(
            "cannot get value from authority: {:?}",
            authority
        )))?];

    get_by_related_condition(
        conn,
        rocks_db,
        condition,
        values,
        sort_by,
        sort_direction,
        pagination,
        limit,
        false,
    )
    .await
}

async fn get_by_related_condition(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    condition: &str,
    values: Vec<Value>,
    _sort_by: Option<Column>,
    _sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
    need_creators_join: bool,
) -> Result<Vec<FullAsset>, DbErr> {
    let condition = &format!("{} AND ast_supply > 0", condition);

    let (mut condition, values, offset) = paginate(pagination, limit, condition, values)?;
    if need_creators_join {
        condition = format!(
            "{} LEFT JOIN asset_creators_v3 ON ast_pubkey = asc_pubkey",
            condition
        );
    }

    condition = format!("{} LIMIT {}", condition, limit);
    if let Some(offset) = offset {
        condition = format!("{} OFFSET {}", condition, offset);
    }

    get_related_for_assets(
        conn,
        rocks_db,
        Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &condition, values),
    )
    .await
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct AssetPubkey {
    ast_pubkey: Vec<u8>,
}

pub async fn get_related_for_assets(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    statement: Statement,
) -> Result<Vec<FullAsset>, DbErr> {
    let pubkeys = conn
        .query_all(statement)
        .await?
        .iter()
        .map(|q| AssetPubkey::from_query_result(q, "").unwrap())
        .collect::<Vec<_>>();

    if pubkeys.is_empty() {
        return Ok(vec![]);
    }

    let converted_pubkeys = pubkeys
        .iter()
        .map(|asset| Pubkey::try_from(asset.ast_pubkey.clone()).unwrap_or_default())
        .collect::<Vec<_>>();

    let asset_selected_maps =
        get_asset_selected_maps(conn, rocks_db, converted_pubkeys.clone()).await?;

    let assets = converted_pubkeys
        .into_iter()
        .filter_map(|id| asset_selected_maps_into_full_asset(&id, &asset_selected_maps))
        .collect::<Vec<_>>();

    Ok(assets)
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct AssetWithMetadata {
    ast_pubkey: Vec<u8>,
    ast_delegate: Option<Vec<u8>>,
    ast_owner: Option<Vec<u8>>,
    ast_authority: Option<Vec<u8>>,
    ast_collection: Option<Vec<u8>>,
    ast_is_collection_verified: bool,
    ast_collection_seq: Option<i64>,
    ast_is_compressed: bool,
    ast_is_compressible: bool,
    ast_is_frozen: bool,
    ast_supply: Option<i64>,
    ast_seq: Option<i64>,
    ast_tree_id: Option<Vec<u8>>,
    ast_leaf: Option<Vec<u8>>,
    ast_nonce: Option<i64>,
    ast_royalty_target_type: RoyaltyTargetType,
    ast_royalty_target: Option<Vec<u8>>,
    ast_royalty_amount: i64,
    ast_created_at: i64,
    ast_is_burnt: bool,
    ast_slot_updated: Option<i64>,
    ast_data_hash: Option<String>,
    ast_creator_hash: Option<String>,
    ast_owner_delegate_seq: Option<i64>,
    ast_was_decompressed: bool,
    ast_leaf_seq: Option<i64>,
    ast_specification_version: SpecificationVersions,
    ast_specification_asset_class: Option<SpecificationAssetClass>,
    ast_owner_type: OwnerType,
    ast_onchain_data: Option<String>,
    ofd_metadata_url: Option<String>,
    ofd_metadata: Option<String>,
    ofd_chain_data_mutability: Option<ChainMutability>,
}

impl AssetWithMetadata {
    pub fn into_asset_model(
        &self,
        owner: Option<String>,
        owners: &HashMap<Pubkey, String>,
        id: Pubkey,
    ) -> asset::Model {
        let creator_hash = self
            .ast_creator_hash
            .clone()
            .as_ref()
            .map(|s| s.trim())
            .map(|s| s.replace("\\x", ""))
            .and_then(|hash| hex::decode(&hash).ok())
            .map(|bytes| bs58::encode(bytes).into_string());
        let data_hash = self
            .ast_data_hash
            .clone()
            .as_ref()
            .map(|s| s.trim())
            .map(|s| s.replace("\\x", ""))
            .and_then(|hash| hex::decode(&hash).ok())
            .map(|bytes| bs58::encode(bytes).into_string());

        asset::Model {
            id: self.ast_pubkey.clone(),
            alt_id: None,
            specification_version: Some(self.ast_specification_version.clone()),
            specification_asset_class: self.ast_specification_asset_class.clone(),
            owner: self
                .ast_owner
                .clone()
                .or(owner.map(|owner| {
                    Pubkey::from_str(&owner)
                        .unwrap_or_default()
                        .to_bytes()
                        .to_vec()
                }))
                .or(owners.get(&id).map(|owner| {
                    Pubkey::from_str(owner)
                        .unwrap_or_default()
                        .to_bytes()
                        .to_vec()
                })),
            owner_type: self.ast_owner_type.clone(),
            delegate: self.ast_delegate.clone(),
            frozen: self.ast_is_frozen,
            supply: self.ast_supply.unwrap_or_default(),
            supply_mint: Some(self.ast_pubkey.clone()),
            compressed: self.ast_is_compressed,
            compressible: self.ast_is_compressible,
            seq: self.ast_seq,
            tree_id: self.ast_tree_id.clone(),
            leaf: self.ast_leaf.clone(),
            nonce: self.ast_nonce,
            royalty_target_type: self.ast_royalty_target_type.clone(),
            royalty_target: self.ast_royalty_target.clone(),
            royalty_amount: self.ast_royalty_amount as i32,
            asset_data: Some(self.ast_pubkey.clone()),
            created_at: Some(self.ast_created_at.clone()),
            burnt: self.ast_is_burnt,
            slot_updated: self.ast_slot_updated,
            data_hash,
            creator_hash,
            owner_delegate_seq: self.ast_owner_delegate_seq,
            was_decompressed: self.ast_was_decompressed,
            leaf_seq: self.ast_leaf_seq,
        }
    }
    pub fn into_asset_data_model(&self) -> Result<asset_data::Model, DbErr> {
        let mut metadata = self.ofd_metadata.clone().unwrap_or("{}".to_string());

        if metadata == PROCESSING_METADATA_STATE {
            metadata = "{}".to_string();
        }

        Ok(asset_data::Model {
            id: self.ast_pubkey.clone(),
            chain_data_mutability: self
                .ofd_chain_data_mutability
                .clone()
                .unwrap_or(ChainMutability::Mutable),
            chain_data: Json::from_str(&self.ast_onchain_data.clone().unwrap_or("{}".to_string()))
                .map_err(|e| DbErr::Custom(e.to_string()))?,
            metadata_url: self.ofd_metadata_url.clone().unwrap_or_default(),
            metadata_mutability: Mutability::Immutable,
            metadata: Json::from_str(metadata.as_str())
                .map_err(|e| DbErr::Custom(e.to_string()))?,
            slot_updated: self.ast_slot_updated.unwrap_or_default(),
            reindex: None,
        })
    }

    pub fn into_asset_authority_model(&self) -> asset_authority::Model {
        asset_authority::Model {
            id: 0,
            asset_id: self.ast_pubkey.clone(),
            scopes: None,
            authority: self.ast_authority.clone().unwrap_or(vec![0; 32]),
            seq: 0,
            slot_updated: 0,
        }
    }
    pub fn into_asset_grouping_model(&self) -> asset_grouping::Model {
        asset_grouping::Model {
            id: 0,
            asset_id: self.ast_pubkey.clone(),
            group_key: "collection".to_string(),
            group_value: self
                .ast_collection
                .clone()
                .map(|pk| bs58::encode(pk.clone()).into_string()),
            seq: self.ast_seq,
            slot_updated: self.ast_slot_updated,
            verified: Some(self.ast_is_collection_verified),
            group_info_seq: self.ast_collection_seq,
        }
    }
}

fn convert_rocks_offchain_data(
    asset_pubkey: &Pubkey,
    offchain_data: &OffChainData,
    asset_dynamic_data: &HashMap<Pubkey, AssetDynamicDetails>,
) -> Result<asset_data::Model, DbErr> {
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
            .unwrap_or_default()
            .as_ref(),
    )
    .unwrap();

    Ok(asset_data::Model {
        id: dynamic_data.pubkey.to_bytes().to_vec(),
        chain_data_mutability: ChainMutability::Mutable,
        chain_data: ch_data,
        metadata_url: offchain_data.url.clone(),
        metadata_mutability: Mutability::Immutable,
        metadata: Json::from_str(metadata.as_str()).map_err(|e| DbErr::Custom(e.to_string()))?,
        slot_updated: dynamic_data.slot_updated as i64,
        reindex: None,
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

    let binding = AssetLeaf::default();
    let leaf = assets_leaf
        .get(asset_pubkey)
        .map(|a| a.clone())
        .unwrap_or(&binding); // Asset can do not have leaf, but we still can make conversion

    let tree_id = if leaf.tree_id == Pubkey::default() {
        None
    } else {
        Some(leaf.tree_id.to_bytes().to_vec())
    };
    let slot_updated = vec![
        dynamic_data.slot_updated,
        owner.slot_updated,
        leaf.slot_updated,
    ]
    .into_iter()
    .max()
    .unwrap(); // unwrap here is save, because vec is not empty

    Ok(asset::Model {
        id: static_data.pubkey.to_bytes().to_vec(),
        alt_id: None,
        specification_version: Some(SpecificationVersions::V1),
        specification_asset_class: Some(static_data.specification_asset_class.clone().into()),
        owner: Some(owner.owner.to_bytes().to_vec()),
        owner_type: owner.owner_type.clone().into(),
        delegate: owner.delegate.map(|pk| pk.to_bytes().to_vec()),
        frozen: dynamic_data.is_frozen,
        supply: dynamic_data
            .supply
            .map(|supply| supply as i64)
            .unwrap_or_default(),
        supply_mint: Some(static_data.pubkey.to_bytes().to_vec()),
        compressed: dynamic_data.is_compressed,
        compressible: dynamic_data.is_compressible,
        seq: dynamic_data.seq.map(|u| u.try_into().ok()).flatten(),
        tree_id,
        leaf: leaf.leaf.clone(),
        nonce: leaf.nonce.map(|nonce| nonce as i64),
        royalty_target_type: static_data.royalty_target_type.clone().into(),
        royalty_target: None, // TODO
        royalty_amount: dynamic_data.royalty_amount as i32,
        asset_data: Some(static_data.pubkey.to_bytes().to_vec()),
        burnt: dynamic_data.is_burnt,
        created_at: Some(static_data.created_at),
        slot_updated: Some(slot_updated as i64),
        data_hash: leaf.data_hash.map(|h| h.to_string()),
        creator_hash: leaf.creator_hash.map(|h| h.to_string()),
        owner_delegate_seq: owner.owner_delegate_seq.map(|seq| seq as i64),
        was_decompressed: dynamic_data.was_decompressed,
        leaf_seq: leaf.leaf_seq.map(|seq| seq as i64),
    })
}

impl From<rocks_db::asset::SpecificationAssetClass> for SpecificationAssetClass {
    fn from(value: rocks_db::asset::SpecificationAssetClass) -> Self {
        match value {
            rocks_db::asset::SpecificationAssetClass::FungibleAsset => {
                SpecificationAssetClass::FungibleAsset
            }
            rocks_db::asset::SpecificationAssetClass::FungibleToken => {
                SpecificationAssetClass::FungibleToken
            }
            rocks_db::asset::SpecificationAssetClass::IdentityNft => {
                SpecificationAssetClass::IdentityNft
            }
            rocks_db::asset::SpecificationAssetClass::Nft => SpecificationAssetClass::Nft,
            rocks_db::asset::SpecificationAssetClass::NonTransferableNft => {
                SpecificationAssetClass::NonTransferableNft
            }
            rocks_db::asset::SpecificationAssetClass::Print => SpecificationAssetClass::Print,
            rocks_db::asset::SpecificationAssetClass::PrintableNft => {
                SpecificationAssetClass::PrintableNft
            }
            rocks_db::asset::SpecificationAssetClass::ProgrammableNft => {
                SpecificationAssetClass::ProgrammableNft
            }
            rocks_db::asset::SpecificationAssetClass::TransferRestrictedNft => {
                SpecificationAssetClass::TransferRestrictedNft
            }
            rocks_db::asset::SpecificationAssetClass::Unknown => SpecificationAssetClass::Unknown,
        }
    }
}

impl From<rocks_db::asset::OwnerType> for OwnerType {
    fn from(value: rocks_db::asset::OwnerType) -> Self {
        match value {
            rocks_db::asset::OwnerType::Single => OwnerType::Single,
            rocks_db::asset::OwnerType::Token => OwnerType::Token,
            rocks_db::asset::OwnerType::Unknown => OwnerType::Unknown,
        }
    }
}

impl From<rocks_db::asset::RoyaltyTargetType> for RoyaltyTargetType {
    fn from(value: rocks_db::asset::RoyaltyTargetType) -> Self {
        match value {
            rocks_db::asset::RoyaltyTargetType::Creators => RoyaltyTargetType::Creators,
            rocks_db::asset::RoyaltyTargetType::Fanout => RoyaltyTargetType::Fanout,
            rocks_db::asset::RoyaltyTargetType::Single => RoyaltyTargetType::Single,
            rocks_db::asset::RoyaltyTargetType::Unknown => RoyaltyTargetType::Unknown,
        }
    }
}

fn convert_rocks_authority_model(
    asset_pubkey: &Pubkey,
    assets_authority: &HashMap<Pubkey, AssetAuthority>,
) -> asset_authority::Model {
    let authority = assets_authority
        .get(asset_pubkey)
        .map(|a| a.clone())
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
) -> asset_grouping::Model {
    let collection = assets_collection.get(asset_pubkey);

    asset_grouping::Model {
        id: 0,
        asset_id: asset_pubkey.to_bytes().to_vec(),
        group_key: "collection".to_string(),
        group_value: collection.map(|asset| asset.collection.to_string()),
        seq: collection.map(|asset| asset.slot_updated as i64),
        slot_updated: collection.map(|asset| asset.slot_updated as i64),
        verified: collection.map(|asset| asset.is_collection_verified),
        group_info_seq: collection.and_then(|asset| asset.collection_seq.map(|s| s as i64)),
    }
}

fn convert_rocks_creators_model(
    asset_pubkey: &Pubkey,
    assets_dynamic_data: &HashMap<Pubkey, AssetDynamicDetails>,
) -> Vec<asset_creators::Model> {
    let dynamic_data = assets_dynamic_data
        .get(asset_pubkey)
        .map(|a| a.clone())
        .unwrap_or(AssetDynamicDetails::default());

    dynamic_data
        .creators
        .iter()
        .enumerate()
        .map(|(position, creator)| asset_creators::Model {
            id: 0,
            asset_id: asset_pubkey.to_bytes().to_vec(),
            creator: creator.creator.to_bytes().to_vec(),
            share: creator.creator_share as i32,
            verified: creator.creator_verified,
            seq: Some(dynamic_data.slot_updated as i64),
            slot_updated: Some(dynamic_data.slot_updated as i64),
            position: position as i16,
        })
        .collect::<Vec<_>>()
}

#[derive(FromQueryResult, Debug, Clone, PartialEq)]
struct Creator {
    asc_asset: Vec<u8>,
    asc_creator: Vec<u8>,
    asc_share: i32,
    asc_verified: bool,
    asc_seq: i64,
    asc_slot_updated: i64,
    asc_position: i64,
}

impl Creator {
    pub fn into_asset_creators_model(&self) -> asset_creators::Model {
        asset_creators::Model {
            id: 0,
            asset_id: self.asc_asset.clone(),
            creator: self.asc_creator.clone(),
            share: self.asc_share,
            verified: self.asc_verified,
            seq: Some(self.asc_seq),
            slot_updated: Some(self.asc_slot_updated),
            position: self.asc_position as i16,
        }
    }
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

struct AssetSelectedMaps {
    assets_static: HashMap<Pubkey, AssetStaticDetails>,
    assets_dynamic: HashMap<Pubkey, AssetDynamicDetails>,
    assets_authority: HashMap<Pubkey, AssetAuthority>,
    assets_collection: HashMap<Pubkey, AssetCollection>,
    assets_owner: HashMap<Pubkey, AssetOwner>,
    assets_leaf: HashMap<Pubkey, AssetLeaf>,
    offchain_data: HashMap<String, OffChainData>,
    urls: HashMap<String, String>,
}

async fn get_asset_selected_maps(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
) -> Result<AssetSelectedMaps, DbErr> {
    let assets_static = fetch_asset_data!(rocks_db, asset_static_data, asset_ids);
    let assets_dynamic = fetch_asset_data!(rocks_db, asset_dynamic_data, asset_ids);
    let assets_authority = fetch_asset_data!(rocks_db, asset_authority_data, asset_ids);
    let assets_collection = fetch_asset_data!(rocks_db, asset_collection_data, asset_ids);
    let assets_owner = fetch_asset_data!(rocks_db, asset_owner_data, asset_ids);
    let assets_leaf = fetch_asset_data!(rocks_db, asset_leaf_data, asset_ids);

    let query = format!("SELECT
                    ast_pubkey,
                    (SELECT mtd_url from metadata WHERE ast_metadata_url_id = mtd_id) AS ast_metadata_url
                    FROM assets_v3
                WHERE ast_pubkey IN ({});", asset_ids
        .iter()
        .enumerate()
        .map(|(index, _)| format!("${}", index + 1))
        .collect::<Vec<_>>()
        .join(", "));
    let query_values = asset_ids
        .iter()
        .map(|asset_pk| {
            Set(asset_pk.to_bytes().as_slice())
                .into_value()
                .ok_or(DbErr::Custom(format!(
                    "cannot get value from asset_id: {:?}",
                    asset_pk
                )))
        })
        .collect::<Result<Vec<_>, DbErr>>()?;
    let urls: HashMap<_, _> = conn
        .query_all(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            &query,
            query_values.clone(),
        ))
        .await?
        .iter()
        .map(|q| AssetWithURL::from_query_result(q, "").unwrap())
        .collect::<Vec<_>>()
        .into_iter()
        .map(|asset| {
            (
                bs58::encode(asset.ast_pubkey.as_slice()).into_string(),
                asset.ast_metadata_url.unwrap_or_default().clone(),
            )
        })
        .collect();

    let offchain_data = rocks_db
        .asset_offchain_data
        .batch_get(urls.clone().into_values().collect::<Vec<_>>())
        .await
        .map_err(|e| DbErr::Custom(e.to_string()))?
        .into_iter()
        .filter_map(|asset| asset.map(|a| (a.url.clone(), a)))
        .collect::<HashMap<_, _>>();

    Ok(AssetSelectedMaps {
        assets_static,
        assets_dynamic,
        assets_authority,
        assets_collection,
        assets_owner,
        assets_leaf,
        offchain_data,
        urls,
    })
}

fn asset_selected_maps_into_full_asset(
    id: &Pubkey,
    asset_selected_maps: &AssetSelectedMaps,
) -> Option<FullAsset> {
    match asset_selected_maps
        .urls
        .get(&id.to_string())
        .and_then(|url| asset_selected_maps.offchain_data.get(url))
    {
        Some(offchain_data) => {
            match convert_rocks_offchain_data(
                &id,
                offchain_data,
                &asset_selected_maps.assets_dynamic,
            ) {
                Ok(data) => convert_rocks_asset_model(
                    &id,
                    &asset_selected_maps.assets_static,
                    &asset_selected_maps.assets_owner,
                    &asset_selected_maps.assets_dynamic,
                    &asset_selected_maps.assets_leaf,
                )
                .ok()
                .and_then(|asset| {
                    Some(FullAsset {
                        asset,
                        data,
                        authorities: vec![convert_rocks_authority_model(
                            &id,
                            &asset_selected_maps.assets_authority,
                        )],
                        creators: convert_rocks_creators_model(
                            &id,
                            &asset_selected_maps.assets_dynamic,
                        ),
                        groups: vec![convert_rocks_grouping_model(
                            &id,
                            &asset_selected_maps.assets_collection,
                        )],
                    })
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
        None => None,
    }
}

pub async fn get_by_ids(
    conn: &impl ConnectionTrait,
    rocks_db: Arc<Storage>,
    asset_ids: Vec<Pubkey>,
) -> Result<Vec<Option<FullAsset>>, DbErr> {
    // need to pass only unique asset_ids to select query
    // index need to save order of IDs in response
    let mut unique_asset_ids_map = HashMap::new();
    for (index, id) in asset_ids.iter().enumerate() {
        unique_asset_ids_map
            .entry(id.clone())
            .or_insert_with(Vec::new)
            .push(index);
    }

    let unique_asset_ids: Vec<_> = unique_asset_ids_map.keys().cloned().collect();
    let asset_selected_maps =
        get_asset_selected_maps(conn, rocks_db, unique_asset_ids.clone()).await?;

    let mut results = vec![None; asset_ids.len()];
    for id in unique_asset_ids {
        let res = asset_selected_maps_into_full_asset(&id, &asset_selected_maps);

        if let Some(indexes) = unique_asset_ids_map.get(&id) {
            for &index in indexes {
                results[index] = res.clone();
            }
        }
    }

    Ok(results)
}
