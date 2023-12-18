use thiserror::Error;

use crate::rpc::filter::{AssetSortBy, AssetSortDirection, AssetSorting};

use super::{
    sea_orm_active_enums::{
        OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
    },
    ConditionType, SearchAssetsQuery,
};

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
                if key != "collection" {
                    return Err(ConversionError::IncompatibleGroupingKey(key))
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
            collection: collection,
            delegate: query.delegate,
            frozen: query.frozen,
            supply: query.supply,
            supply_mint: query.supply_mint,
            compressed: query.compressed,
            compressible: query.compressible,
            royalty_target_type: query.royalty_target_type.map(|v| v.into()),
            royalty_target: query.royalty_target,
            royalty_amount: query.royalty_amount,
            burnt: query.burnt,
            json_uri: query.json_uri,
        })
    }
}

impl From<SpecificationVersions> for postgre_client::model::SpecificationVersions {
    fn from(specification_version: SpecificationVersions) -> Self {
        match specification_version {
            SpecificationVersions::Unknown => Self::Unknown,
            SpecificationVersions::V0 => Self::V0,
            SpecificationVersions::V1 => Self::V1,
            SpecificationVersions::V2 => Self::V2,
        }
    }
}

impl From<SpecificationAssetClass> for postgre_client::model::SpecificationAssetClass {
    fn from(specification_asset_class: SpecificationAssetClass) -> Self {
        match specification_asset_class {
            SpecificationAssetClass::Unknown => Self::Unknown,
            SpecificationAssetClass::FungibleAsset => Self::FungibleAsset,
            SpecificationAssetClass::FungibleToken => Self::FungibleToken,
            SpecificationAssetClass::IdentityNft => Self::IdentityNft,
            SpecificationAssetClass::Nft => Self::Nft,
            SpecificationAssetClass::NonTransferableNft => Self::NonTransferableNft,
            SpecificationAssetClass::Print => Self::Print,
            SpecificationAssetClass::PrintableNft => Self::PrintableNft,
            SpecificationAssetClass::ProgrammableNft => Self::ProgrammableNft,
            SpecificationAssetClass::TransferRestrictedNft => Self::TransferRestrictedNft,
        }
    }
}

impl From<OwnerType> for postgre_client::model::OwnerType {
    fn from(owner_type: OwnerType) -> Self {
        match owner_type {
            OwnerType::Unknown => Self::Unknown,
            OwnerType::Token => Self::Token,
            OwnerType::Single => Self::Single,
        }
    }
}

impl From<RoyaltyTargetType> for postgre_client::model::RoyaltyTargetType {
    fn from(royalty_target_type: RoyaltyTargetType) -> Self {
        match royalty_target_type {
            RoyaltyTargetType::Unknown => Self::Unknown,
            RoyaltyTargetType::Creators => Self::Creators,
            RoyaltyTargetType::Fanout => Self::Fanout,
            RoyaltyTargetType::Single => Self::Single,
        }
    }
}

impl From<AssetSorting> for postgre_client::model::AssetSorting {
    fn from(sorting: AssetSorting) -> Self {
        Self {
            sort_by: sorting.sort_by.into(),
            sort_direction: sorting
                .sort_direction
                .map_or(postgre_client::model::AssetSortDirection::Desc, |v| {
                    v.into()
                }),
        }
    }
}

impl From<AssetSortBy> for postgre_client::model::AssetSortBy {
    fn from(sort_by: AssetSortBy) -> Self {
        match sort_by {
            AssetSortBy::Created => Self::SlotCreated,
            _ => Self::SlotUpdated,
        }
    }
}

impl From<AssetSortDirection> for postgre_client::model::AssetSortDirection {
    fn from(sort_direction: AssetSortDirection) -> Self {
        match sort_direction {
            AssetSortDirection::Asc => Self::Asc,
            AssetSortDirection::Desc => Self::Desc,
        }
    }
}

impl From<crate::rpc::filter::SearchConditionType> for ConditionType {
    fn from(search_condition_type: crate::rpc::filter::SearchConditionType) -> Self {
        match search_condition_type {
            crate::rpc::filter::SearchConditionType::All => Self::All,
            crate::rpc::filter::SearchConditionType::Any => Self::Any,
        }
    }
}

impl From<crate::rpc::OwnershipModel> for OwnerType {
    fn from(ownership_model: crate::rpc::OwnershipModel) -> Self {
        match ownership_model {
            crate::rpc::OwnershipModel::Single => Self::Single,
            crate::rpc::OwnershipModel::Token => Self::Token,
        }
    }
}

impl From<&crate::rpc::Interface> for SpecificationAssetClass {
    fn from(interface: &crate::rpc::Interface) -> Self {
        match interface {
            crate::rpc::Interface::FungibleAsset => Self::FungibleAsset,
            crate::rpc::Interface::Identity => Self::IdentityNft,
            crate::rpc::Interface::Nft
            | crate::rpc::Interface::V1NFT
            | crate::rpc::Interface::LegacyNft => Self::Nft,
            crate::rpc::Interface::V1PRINT => Self::Print,
            crate::rpc::Interface::ProgrammableNFT => Self::ProgrammableNft,
            crate::rpc::Interface::Custom | crate::rpc::Interface::Executable => Self::Unknown,
        }
    }
}

impl From<&crate::rpc::Interface> for SpecificationVersions {
    fn from(interface: &crate::rpc::Interface) -> Self {
        match interface {
            crate::rpc::Interface::LegacyNft => Self::V0,
            _ => Self::V1,
        }
    }
}

impl From<crate::rpc::RoyaltyModel> for RoyaltyTargetType {
    fn from(royalty_model: crate::rpc::RoyaltyModel) -> Self {
        match royalty_model {
            crate::rpc::RoyaltyModel::Creators => Self::Creators,
            crate::rpc::RoyaltyModel::Fanout => Self::Fanout,
            crate::rpc::RoyaltyModel::Single => Self::Single,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dao::SearchAssetsQuery;

    #[test]
    fn test_search_assets_filter_from_search_assets_query_conversion_error() {
        let query = SearchAssetsQuery {
            grouping: Some((
                "not_collection".to_string(),
                "test".to_string().into_bytes(),
            )),
            ..Default::default()
        };
        let result = postgre_client::model::SearchAssetsFilter::try_from(query);
        assert!(result.is_err());
    }
}
