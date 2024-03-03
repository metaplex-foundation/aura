pub use full_asset::*;
pub use generated::*;

use self::sea_orm_active_enums::{
    OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
};

mod converters;
mod full_asset;
mod generated;
pub mod scopes;
pub use converters::*;
use entities::api_req_params::{
    GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner, SearchAssets,
};
use interface::error::UsecaseError;
use usecase::validation::{validate_opt_pubkey, validate_pubkey};

pub struct GroupingSize {
    pub size: u64,
}

pub enum Pagination {
    Keyset {
        before: Option<String>,
        after: Option<String>,
    },
    Page {
        page: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionType {
    Any,
    All,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchAssetsQuery {
    // Conditions
    pub negate: Option<bool>,
    /// Defaults to [ConditionType::All]
    pub condition_type: Option<ConditionType>,
    pub specification_version: Option<SpecificationVersions>,
    pub specification_asset_class: Option<SpecificationAssetClass>,
    pub owner_address: Option<Vec<u8>>,
    pub owner_type: Option<OwnerType>,
    pub creator_address: Option<Vec<u8>>,
    pub creator_verified: Option<bool>,
    pub authority_address: Option<Vec<u8>>,
    pub grouping: Option<(String, Vec<u8>)>,
    pub delegate: Option<Vec<u8>>,
    pub frozen: Option<bool>,
    pub supply: Option<u64>,
    pub supply_mint: Option<Vec<u8>>,
    pub compressed: Option<bool>,
    pub compressible: Option<bool>,
    pub royalty_target_type: Option<RoyaltyTargetType>,
    pub royalty_target: Option<Vec<u8>>,
    pub royalty_amount: Option<u32>,
    pub burnt: Option<bool>,
    pub json_uri: Option<String>,
}

impl TryFrom<SearchAssets> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(search_assets: SearchAssets) -> Result<Self, Self::Error> {
        let grouping = search_assets
            .grouping
            .map(|(key, val)| {
                if key != "collection" {
                    return Err(UsecaseError::InvalidGroupingKey(key));
                }
                validate_pubkey(val).map(|pubkey| (key, pubkey.to_bytes().to_vec()))
            })
            .transpose()?;
        Ok(SearchAssetsQuery {
            negate: search_assets.negate,
            condition_type: search_assets.condition_type.map(|s| s.into()),
            owner_address: validate_opt_pubkey(&search_assets.owner_address)?,
            owner_type: search_assets
                .owner_type
                .map(|s| crate::rpc::OwnershipModel::from(s).into()),
            creator_address: validate_opt_pubkey(&search_assets.creator_address)?,
            creator_verified: search_assets.creator_verified,
            authority_address: validate_opt_pubkey(&search_assets.authority_address)?,
            grouping,
            delegate: validate_opt_pubkey(&search_assets.delegate)?,
            frozen: search_assets.frozen,
            supply: search_assets.supply,
            supply_mint: validate_opt_pubkey(&search_assets.supply_mint)?,
            compressed: search_assets.compressed,
            compressible: search_assets.compressible,
            royalty_target_type: search_assets
                .royalty_target_type
                .map(|s| crate::rpc::RoyaltyModel::from(s).into()),
            royalty_target: validate_opt_pubkey(&search_assets.royalty_target)?,
            royalty_amount: search_assets.royalty_amount,
            burnt: search_assets.burnt,
            json_uri: search_assets.json_uri,
            specification_version: search_assets
                .interface
                .clone()
                .map(|s| (&crate::rpc::Interface::from(s)).into()),
            specification_asset_class: search_assets
                .interface
                .map(|s| (&crate::rpc::Interface::from(s)).into())
                .filter(|v| v != &SpecificationAssetClass::Unknown),
        })
    }
}

impl TryFrom<GetAssetsByAuthority> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_authority: GetAssetsByAuthority) -> Result<Self, Self::Error> {
        Ok(SearchAssetsQuery {
            authority_address: Some(
                validate_pubkey(asset_authority.authority_address)
                    .map(|k| k.to_bytes().to_vec())?,
            ),
            ..Default::default()
        })
    }
}

impl TryFrom<GetAssetsByCreator> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_creator: GetAssetsByCreator) -> Result<Self, Self::Error> {
        Ok(SearchAssetsQuery {
            creator_address: Some(
                validate_pubkey(asset_creator.creator_address).map(|k| k.to_bytes().to_vec())?,
            ),
            creator_verified: asset_creator.only_verified,
            ..Default::default()
        })
    }
}

impl TryFrom<GetAssetsByGroup> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_group: GetAssetsByGroup) -> Result<Self, Self::Error> {
        if asset_group.group_key != "collection" {
            return Err(UsecaseError::InvalidGroupingKey(asset_group.group_key));
        }

        Ok(SearchAssetsQuery {
            grouping: Some((
                asset_group.group_key,
                validate_pubkey(asset_group.group_value).map(|k| k.to_bytes().to_vec())?,
            )),
            ..Default::default()
        })
    }
}

impl TryFrom<GetAssetsByOwner> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_owner: GetAssetsByOwner) -> Result<Self, Self::Error> {
        Ok(SearchAssetsQuery {
            creator_address: Some(
                validate_pubkey(asset_owner.owner_address).map(|k| k.to_bytes().to_vec())?,
            ),
            ..Default::default()
        })
    }
}
