use crate::gapfiller::{
    AssetDetails, AssetLeaf, Creator, DynamicBoolField, DynamicBytesField, DynamicEnumField,
    DynamicUint32Field, DynamicUint64Field, OwnerType, RoyaltyTargetType, SpecificationAssetClass,
    SpecificationVersions, AssetCollection, ChainDataV1, TokenStandard, UseMethod, Uses,
};
use entities::models::CompleteAssetDetails;
use solana_sdk::pubkey::Pubkey;
impl From<CompleteAssetDetails> for AssetDetails {
    fn from(value: CompleteAssetDetails) -> Self {
        Self {
            pubkey: value.pubkey.to_bytes().to_vec(),
            specification_asset_class: SpecificationAssetClass::from(
                value.specification_asset_class,
            )
            .into(),
            royalty_target_type: RoyaltyTargetType::from(value.royalty_target_type).into(),
            slot_created: value.slot_created,
            is_compressible: Some(value.is_compressible.into()),
            is_compressed: Some(value.is_compressed.into()),
            is_frozen: Some(value.is_frozen.into()),
            supply: value.supply.map(|v| v.into()),
            seq: value.seq.map(|v| v.into()),
            is_burnt: Some(value.is_burnt.into()),
            was_decompressed: Some(value.was_decompressed.into()),
            creators: value.creators.iter().map(|c| Creator::from(c)).collect(),
            royalty_amount: Some(value.royalty_amount.into()),
            authority: Some(value.authority.into()),
            owner: Some(value.owner.into()),
            delegate: value.delegate.map(|v| v.into()),
            owner_type: Some(value.owner_type.into()),
            owner_delegate_seq: value.owner_delegate_seq.map(|v| v.into()),
            leaves: value
                .leaves
                .iter()
                .map(|leaf| AssetLeaf::from(leaf))
                .collect(),
            collection: value.collection.map(|v| (&v).into()),
            chain_data: value.onchain_data.map(|v| v.into()),
        }
    }
}

impl From<(bool, u64)> for DynamicBoolField {
    fn from(value: (bool, u64)) -> Self {
        Self {
            value: value.0,
            slot_updated: value.1,
        }
    }
}

impl From<(u64, u64)> for DynamicUint64Field {
    fn from(value: (u64, u64)) -> Self {
        Self {
            value: value.0,
            slot_updated: value.1,
        }
    }
}

impl From<(u16, u64)> for DynamicUint32Field {
    fn from(value: (u16, u64)) -> Self {
        Self {
            value: value.0 as u32,
            slot_updated: value.1,
        }
    }
}

impl From<(Pubkey, u64)> for DynamicBytesField {
    fn from(value: (Pubkey, u64)) -> Self {
        Self {
            value: value.0.to_bytes().to_vec(),
            slot_updated: value.1,
        }
    }
}

impl From<(entities::enums::OwnerType, u64)> for DynamicEnumField {
    fn from(value: (entities::enums::OwnerType, u64)) -> Self {
        Self {
            value: OwnerType::from(value.0).into(),
            slot_updated: value.1,
        }
    }
}

impl From<&entities::models::Creator> for Creator {
    fn from(value: &entities::models::Creator) -> Self {
        Self {
            creator: value.creator.to_bytes().to_vec(),
            creator_verified: value.creator_verified,
            creator_share: value.creator_share as u32,
        }
    }
}

impl From<&entities::models::AssetLeaf> for AssetLeaf {
    fn from(value: &entities::models::AssetLeaf) -> Self {
        Self {
            tree_id: value.tree_id.to_bytes().to_vec(),
            leaf: value.leaf.clone().unwrap_or_default(),
            nonce: value.nonce.unwrap_or_default(),
            data_hash: value
                .data_hash
                .map(|h| h.to_bytes().to_vec())
                .unwrap_or_default(),
            creator_hash: value
                .creator_hash
                .map(|h| h.to_bytes().to_vec())
                .unwrap_or_default(),
            leaf_seq: value.leaf_seq.unwrap_or_default(),
            slot_updated: value.slot_updated,
        }
    }
}

impl From<&entities::models::AssetCollection> for AssetCollection {
    fn from(value: &entities::models::AssetCollection) -> Self {
        Self {
            collection: value.collection.to_bytes().to_vec(),
            is_collection_verified: value.is_collection_verified,
            collection_seq: value.collection_seq.unwrap_or_default(),
            slot_updated: value.slot_updated,
        }
    }
}

impl From<(entities::models::ChainDataV1, u64)> for ChainDataV1 {
    fn from(value: (entities::models::ChainDataV1, u64)) -> Self {
        let (value, slot_updated) = value;
        Self {
            name: value.name.clone(),
            symbol: value.symbol.clone(),
            edition_nonce: value.edition_nonce.unwrap_or_default() as u32,
            primary_sale_happened: value.primary_sale_happened,
            token_standard: value.token_standard.map(|v| TokenStandard::from(v).into()).unwrap_or_default(),
            uses: value.uses.map(|v| v.into()),
            slot_updated:slot_updated,
        }
    }
}

impl From<entities::models::Uses> for Uses {
    fn from(value: entities::models::Uses) -> Self {
        Self {
            use_method: UseMethod::from(value.use_method).into(),
            remaining: value.remaining,
            total: value.total,
        }
    }
}
macro_rules! impl_from_enum {
    ($src:ty, $dst:ident, $($variant:ident),*) => {
        impl From<$src> for $dst {
            fn from(value: $src) -> Self {
                match value {
                    $(
                        <$src>::$variant => $dst::$variant,
                    )*
                }
            }
        }
    };
}

impl_from_enum!(
    entities::enums::SpecificationVersions,
    SpecificationVersions,
    Unknown,
    V0,
    V1,
    V2
);
impl_from_enum!(
    entities::enums::SpecificationAssetClass,
    SpecificationAssetClass,
    Unknown,
    FungibleToken,
    FungibleAsset,
    Nft,
    PrintableNft,
    ProgrammableNft,
    Print,
    TransferRestrictedNft,
    NonTransferableNft,
    IdentityNft
);
impl_from_enum!(
    entities::enums::RoyaltyTargetType,
    RoyaltyTargetType,
    Unknown,
    Creators,
    Fanout,
    Single
);
impl_from_enum!(
    entities::enums::OwnerType,
    OwnerType,
    Unknown,
    Token,
    Single
);
impl_from_enum!(
    entities::enums::TokenStandard,
    TokenStandard,
    NonFungible,
    FungibleAsset,
    Fungible,
    NonFungibleEdition,
    ProgrammableNonFungible,
    ProgrammableNonFungibleEdition
);
impl_from_enum!(
    entities::enums::UseMethod,
    UseMethod,
    Burn,
    Multiple,
    Single
);
