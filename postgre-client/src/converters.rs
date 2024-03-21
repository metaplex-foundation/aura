use crate::model::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions};

impl From<entities::enums::SpecificationVersions> for SpecificationVersions {
    fn from(specification_version: entities::enums::SpecificationVersions) -> Self {
        match specification_version {
            entities::enums::SpecificationVersions::Unknown => SpecificationVersions::Unknown,
            entities::enums::SpecificationVersions::V0 => SpecificationVersions::V0,
            entities::enums::SpecificationVersions::V1 => SpecificationVersions::V1,
            entities::enums::SpecificationVersions::V2 => SpecificationVersions::V2,
        }
    }
}

impl From<entities::enums::RoyaltyTargetType> for RoyaltyTargetType {
    fn from(royalty_target_type: entities::enums::RoyaltyTargetType) -> Self {
        match royalty_target_type {
            entities::enums::RoyaltyTargetType::Unknown => RoyaltyTargetType::Unknown,
            entities::enums::RoyaltyTargetType::Creators => RoyaltyTargetType::Creators,
            entities::enums::RoyaltyTargetType::Fanout => RoyaltyTargetType::Fanout,
            entities::enums::RoyaltyTargetType::Single => RoyaltyTargetType::Single,
        }
    }
}

impl From<entities::enums::SpecificationAssetClass> for SpecificationAssetClass {
    fn from(specification_asset_class: entities::enums::SpecificationAssetClass) -> Self {
        match specification_asset_class {
            entities::enums::SpecificationAssetClass::Unknown => SpecificationAssetClass::Unknown,
            entities::enums::SpecificationAssetClass::FungibleToken => {
                SpecificationAssetClass::FungibleToken
            }
            entities::enums::SpecificationAssetClass::FungibleAsset => {
                SpecificationAssetClass::FungibleAsset
            }
            entities::enums::SpecificationAssetClass::Nft => SpecificationAssetClass::Nft,
            entities::enums::SpecificationAssetClass::PrintableNft => {
                SpecificationAssetClass::PrintableNft
            }
            entities::enums::SpecificationAssetClass::ProgrammableNft => {
                SpecificationAssetClass::ProgrammableNft
            }
            entities::enums::SpecificationAssetClass::Print => SpecificationAssetClass::Print,
            entities::enums::SpecificationAssetClass::TransferRestrictedNft => {
                SpecificationAssetClass::TransferRestrictedNft
            }
            entities::enums::SpecificationAssetClass::NonTransferableNft => {
                SpecificationAssetClass::NonTransferableNft
            }
            entities::enums::SpecificationAssetClass::IdentityNft => {
                SpecificationAssetClass::IdentityNft
            }
            entities::enums::SpecificationAssetClass::MplCoreAsset => {
                SpecificationAssetClass::MplCoreAsset
            }
            entities::enums::SpecificationAssetClass::MplCoreCollection => {
                SpecificationAssetClass::MplCoreCollection
            }
        }
    }
}

impl From<entities::enums::OwnerType> for OwnerType {
    fn from(owner_type: entities::enums::OwnerType) -> Self {
        match owner_type {
            entities::enums::OwnerType::Unknown => OwnerType::Unknown,
            entities::enums::OwnerType::Token => OwnerType::Token,
            entities::enums::OwnerType::Single => OwnerType::Single,
        }
    }
}
