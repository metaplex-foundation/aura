use entities::{
    api_req_params::{
        GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner, SearchAssets,
        SearchConditionType,
    },
    enums::{
        OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions, TokenType,
    },
};
use interface::error::UsecaseError;
use thiserror::Error;
use usecase::validation::{validate_opt_pubkey_vec, validate_pubkey};

use crate::api::dapi::asset::COLLECTION_GROUP_KEY;

pub struct GroupingSize {
    pub size: u64,
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
    pub supply: Option<AssetSupply>,
    pub supply_mint: Option<Vec<u8>>,
    pub compressed: Option<bool>,
    pub compressible: Option<bool>,
    pub royalty_target_type: Option<RoyaltyTargetType>,
    pub royalty_target: Option<Vec<u8>>,
    pub royalty_amount: Option<u32>,
    pub burnt: Option<bool>,
    pub json_uri: Option<String>,
    pub token_type: Option<TokenType>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssetSupply {
    Greater(u64),
    Equal(u64),
}

impl TryFrom<SearchAssets> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(search_assets: SearchAssets) -> Result<Self, Self::Error> {
        let grouping = search_assets
            .grouping
            .map(|(key, val)| {
                if key != COLLECTION_GROUP_KEY {
                    return Err(UsecaseError::InvalidGroupingKey(key));
                }
                validate_pubkey(val).map(|pubkey| (key, pubkey.to_bytes().to_vec()))
            })
            .transpose()?;
        Ok(SearchAssetsQuery {
            negate: search_assets.negate,
            condition_type: search_assets.condition_type.map(|s| s.into()),
            owner_address: validate_opt_pubkey_vec(&search_assets.owner_address)?,
            owner_type: search_assets.owner_type.map(|s| s.into()),
            creator_address: validate_opt_pubkey_vec(&search_assets.creator_address)?,
            creator_verified: search_assets.creator_verified,
            authority_address: validate_opt_pubkey_vec(&search_assets.authority_address)?,
            grouping,
            delegate: validate_opt_pubkey_vec(&search_assets.delegate)?,
            frozen: search_assets.frozen,
            supply: search_assets.supply.map(AssetSupply::Equal),
            supply_mint: validate_opt_pubkey_vec(&search_assets.supply_mint)?,
            compressed: search_assets.compressed,
            compressible: search_assets.compressible,
            royalty_target_type: search_assets.royalty_target_type.map(|s| s.into()),
            royalty_target: validate_opt_pubkey_vec(&search_assets.royalty_target)?,
            royalty_amount: search_assets.royalty_amount,
            burnt: search_assets.burnt,
            json_uri: search_assets.json_uri,
            specification_version: search_assets.interface.clone().map(|s| s.into()),
            specification_asset_class: search_assets
                .interface
                .map(|s| s.into())
                .filter(|v| v != &SpecificationAssetClass::Unknown),
            token_type: search_assets.token_type,
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
            supply: Some(AssetSupply::Greater(0)),
            ..Default::default()
        })
    }
}

impl TryFrom<GetAssetsByCreator> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_creator: GetAssetsByCreator) -> Result<Self, Self::Error> {
        let creator_verified = if let Some(false) = asset_creator.only_verified {
            None
        } else {
            asset_creator.only_verified
        };

        Ok(SearchAssetsQuery {
            creator_address: Some(
                validate_pubkey(asset_creator.creator_address).map(|k| k.to_bytes().to_vec())?,
            ),
            creator_verified,
            supply: Some(AssetSupply::Greater(0)),
            ..Default::default()
        })
    }
}

impl TryFrom<GetAssetsByGroup> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_group: GetAssetsByGroup) -> Result<Self, Self::Error> {
        if asset_group.group_key != COLLECTION_GROUP_KEY {
            return Err(UsecaseError::InvalidGroupingKey(asset_group.group_key));
        }

        Ok(SearchAssetsQuery {
            grouping: Some((
                asset_group.group_key,
                validate_pubkey(asset_group.group_value).map(|k| k.to_bytes().to_vec())?,
            )),
            supply: Some(AssetSupply::Greater(0)),
            ..Default::default()
        })
    }
}

impl TryFrom<GetAssetsByOwner> for SearchAssetsQuery {
    type Error = UsecaseError;
    fn try_from(asset_owner: GetAssetsByOwner) -> Result<Self, Self::Error> {
        Ok(SearchAssetsQuery {
            owner_address: Some(
                validate_pubkey(asset_owner.owner_address).map(|k| k.to_bytes().to_vec())?,
            ),
            supply: Some(AssetSupply::Greater(0)),
            ..Default::default()
        })
    }
}

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Incompatible Grouping Key: {0}")]
    IncompatibleGroupingKey(String),
}

impl TryFrom<SearchAssetsQuery> for postgre_client::model::SearchAssetsFilter {
    type Error = ConversionError;

    fn try_from(query: SearchAssetsQuery) -> Result<Self, Self::Error> {
        let collection = query
            .grouping
            .map(|(key, val)| {
                if key != COLLECTION_GROUP_KEY {
                    return Err(ConversionError::IncompatibleGroupingKey(key));
                }
                Ok(val)
            })
            .transpose()?;
        Ok(Self {
            specification_version: query.specification_version.map(|v| v.into()),
            specification_asset_class: query.specification_asset_class.map(|v| v.into()),
            owner_address: query.owner_address,
            owner_type: query.owner_type.map(|v| v.into()),
            creator_address: query.creator_address,
            creator_verified: query.creator_verified,
            authority_address: query.authority_address,
            collection,
            delegate: query.delegate,
            frozen: query.frozen,
            supply: query.supply.map(|s| s.into()),
            supply_mint: query.supply_mint,
            compressed: query.compressed,
            compressible: query.compressible,
            royalty_target_type: query.royalty_target_type.map(|v| v.into()),
            royalty_target: query.royalty_target,
            royalty_amount: query.royalty_amount,
            burnt: query.burnt,
            json_uri: query.json_uri,
            token_type: query.token_type,
        })
    }
}

impl From<AssetSupply> for postgre_client::model::AssetSupply {
    fn from(supply: AssetSupply) -> Self {
        match supply {
            AssetSupply::Equal(s) => Self::Equal(s),
            AssetSupply::Greater(s) => Self::Greater(s),
        }
    }
}

impl From<SearchConditionType> for ConditionType {
    fn from(search_condition_type: SearchConditionType) -> Self {
        match search_condition_type {
            SearchConditionType::All => Self::All,
            SearchConditionType::Any => Self::Any,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::dapi::converters::SearchAssetsQuery;

    #[test]
    fn test_search_assets_filter_from_search_assets_query_conversion_error() {
        let query = SearchAssetsQuery {
            grouping: Some(("not_collection".to_string(), "test".to_string().into_bytes())),
            ..Default::default()
        };
        let result = postgre_client::model::SearchAssetsFilter::try_from(query);
        assert!(result.is_err());
    }
}
