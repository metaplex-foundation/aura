use crate::model::{
    AssetIndex, Creator, OwnerType, RoyaltyTargetType, SpecificationAssetClass,
    SpecificationVersions,
};

impl From<rocks_db::asset::SpecificationVersions> for SpecificationVersions {
    fn from(specification_version: rocks_db::asset::SpecificationVersions) -> Self {
        match specification_version {
            rocks_db::asset::SpecificationVersions::Unknown => SpecificationVersions::Unknown,
            rocks_db::asset::SpecificationVersions::V0 => SpecificationVersions::V0,
            rocks_db::asset::SpecificationVersions::V1 => SpecificationVersions::V1,
            rocks_db::asset::SpecificationVersions::V2 => SpecificationVersions::V2,
        }
    }
}

impl From<rocks_db::asset::RoyaltyTargetType> for RoyaltyTargetType {
    fn from(royalty_target_type: rocks_db::asset::RoyaltyTargetType) -> Self {
        match royalty_target_type {
            rocks_db::asset::RoyaltyTargetType::Unknown => RoyaltyTargetType::Unknown,
            rocks_db::asset::RoyaltyTargetType::Creators => RoyaltyTargetType::Creators,
            rocks_db::asset::RoyaltyTargetType::Fanout => RoyaltyTargetType::Fanout,
            rocks_db::asset::RoyaltyTargetType::Single => RoyaltyTargetType::Single,
        }
    }
}

impl From<rocks_db::asset::SpecificationAssetClass> for SpecificationAssetClass {
    fn from(specification_asset_class: rocks_db::asset::SpecificationAssetClass) -> Self {
        match specification_asset_class {
            rocks_db::asset::SpecificationAssetClass::Unknown => SpecificationAssetClass::Unknown,
            rocks_db::asset::SpecificationAssetClass::FungibleToken => {
                SpecificationAssetClass::FungibleToken
            }
            rocks_db::asset::SpecificationAssetClass::FungibleAsset => {
                SpecificationAssetClass::FungibleAsset
            }
            rocks_db::asset::SpecificationAssetClass::Nft => SpecificationAssetClass::Nft,
            rocks_db::asset::SpecificationAssetClass::PrintableNft => {
                SpecificationAssetClass::PrintableNft
            }
            rocks_db::asset::SpecificationAssetClass::ProgrammableNft => {
                SpecificationAssetClass::ProgrammableNft
            }
            rocks_db::asset::SpecificationAssetClass::Print => SpecificationAssetClass::Print,
            rocks_db::asset::SpecificationAssetClass::TransferRestrictedNft => {
                SpecificationAssetClass::TransferRestrictedNft
            }
            rocks_db::asset::SpecificationAssetClass::NonTransferableNft => {
                SpecificationAssetClass::NonTransferableNft
            }
            rocks_db::asset::SpecificationAssetClass::IdentityNft => {
                SpecificationAssetClass::IdentityNft
            }
        }
    }
}

impl From<rocks_db::asset::OwnerType> for OwnerType {
    fn from(owner_type: rocks_db::asset::OwnerType) -> Self {
        match owner_type {
            rocks_db::asset::OwnerType::Unknown => OwnerType::Unknown,
            rocks_db::asset::OwnerType::Token => OwnerType::Token,
            rocks_db::asset::OwnerType::Single => OwnerType::Single,
        }
    }
}

impl From<&rocks_db::asset::AssetIndex> for AssetIndex {
    fn from(asset_index: &rocks_db::asset::AssetIndex) -> Self {
        AssetIndex {
            pubkey: asset_index.pubkey.to_bytes().to_vec(),
            specification_version: SpecificationVersions::from(asset_index.specification_version),
            specification_asset_class: SpecificationAssetClass::from(
                asset_index.specification_asset_class,
            ),
            royalty_target_type: RoyaltyTargetType::from(asset_index.royalty_target_type),
            slot_created: asset_index.slot_created,
            owner_type: asset_index
                .owner_type
                .map(|owner_type| OwnerType::from(owner_type)),
            owner: asset_index.owner.map(|owner| owner.to_bytes().to_vec()),
            delegate: asset_index
                .delegate
                .map(|delegate| delegate.to_bytes().to_vec()),
            authority: asset_index
                .authority
                .map(|authority| authority.to_bytes().to_vec()),
            collection: asset_index
                .collection
                .map(|collection| collection.to_bytes().to_vec()),
            is_collection_verified: asset_index.is_collection_verified,
            royalty_amount: asset_index.royalty_amount,
            is_burnt: asset_index.is_burnt,
            is_compressible: asset_index.is_compressible,
            is_compressed: asset_index.is_compressed,
            is_frozen: asset_index.is_frozen,
            supply: asset_index.supply,
            metadata_url: asset_index.metadata_url.clone(),
            slot_updated: asset_index.slot_updated,
            creators: asset_index
                .creators
                .iter()
                .map(|creator| Creator::from(creator))
                .collect(),
        }
    }
}

impl From<&rocks_db::asset::Creator> for Creator {
    fn from(creator: &rocks_db::asset::Creator) -> Self {
        Creator {
            creator: creator.creator.to_bytes().to_vec(),
            creator_verified: creator.creator_verified,
        }
    }
}
