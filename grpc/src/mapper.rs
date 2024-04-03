use crate::gapfiller::{
    AssetCollection, AssetDetails, AssetLeaf, ChainDataV1, ChainMutability, ClItem, ClLeaf,
    Creator, DynamicBoolField, DynamicBytesField, DynamicChainMutability, DynamicCreatorsField,
    DynamicEnumField, DynamicStringField, DynamicUint32Field, DynamicUint64Field, EditionV1,
    MasterEdition, OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
    TokenStandard, UseMethod, Uses,
};
use entities::models::{CompleteAssetDetails, Updated};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
impl From<CompleteAssetDetails> for AssetDetails {
    fn from(value: CompleteAssetDetails) -> Self {
        let delegate = value.delegate.value.map(|key| DynamicBytesField {
            value: key.to_bytes().to_vec(),
            slot_updated: value.delegate.slot_updated,
            seq_updated: value.delegate.get_upd_ver_seq(),
        });
        let owner = value.owner.value.map(|key| DynamicBytesField {
            value: key.to_bytes().to_vec(),
            slot_updated: value.owner.slot_updated,
            seq_updated: value.owner.get_upd_ver_seq(),
        });

        let owner_delegate_seq = value
            .owner_delegate_seq
            .value
            .map(|seq| DynamicUint64Field {
                value: seq,
                slot_updated: value.owner_delegate_seq.slot_updated,
                seq_updated: value.owner_delegate_seq.get_upd_ver_seq(),
            });

        Self {
            pubkey: value.pubkey.to_bytes().to_vec(),
            specification_asset_class: SpecificationAssetClass::from(
                value.specification_asset_class,
            )
            .into(),
            edition_address: value.edition_address.map(|e| e.to_bytes().to_vec()),
            royalty_target_type: RoyaltyTargetType::from(value.royalty_target_type).into(),
            slot_created: value.slot_created,
            is_compressible: Some(value.is_compressible.into()),
            is_compressed: Some(value.is_compressed.into()),
            is_frozen: Some(value.is_frozen.into()),
            supply: value.supply.map(|v| v.into()),
            seq: value.seq.map(|v| v.into()),
            is_burnt: Some(value.is_burnt.into()),
            was_decompressed: Some(value.was_decompressed.into()),
            creators: Some(value.creators.into()),
            royalty_amount: Some(value.royalty_amount.into()),
            authority: Some(value.authority.into()),
            owner,
            delegate,
            owner_type: Some(value.owner_type.into()),
            owner_delegate_seq,
            chain_mutability: value.chain_mutability.map(|v| v.into()),
            lamports: value.lamports.map(|v| v.into()),
            executable: value.executable.map(|v| v.into()),
            metadata_owner: value.metadata_owner.map(|v| v.into()),
            asset_leaf: value.asset_leaf.map(|v| v.into()),
            collection: value.collection.map(|v| v.into()),
            chain_data: value.onchain_data.map(|v| v.into()),
            cl_leaf: value.cl_leaf.map(|v| v.into()),
            cl_items: value.cl_items.into_iter().map(ClItem::from).collect(),
            edition: value.edition.map(|e| e.into()),
            master_edition: value.master_edition.map(|e| e.into()),
        }
    }
}

impl From<AssetDetails> for CompleteAssetDetails {
    fn from(value: AssetDetails) -> Self {
        Self {
            pubkey: Pubkey::try_from(value.pubkey).unwrap_or_default(),
            specification_asset_class: entities::enums::SpecificationAssetClass::from(
                SpecificationAssetClass::try_from(value.specification_asset_class)
                    .unwrap_or_default(),
            ),
            royalty_target_type: entities::enums::RoyaltyTargetType::from(
                RoyaltyTargetType::try_from(value.royalty_target_type).unwrap_or_default(),
            ),
            slot_created: value.slot_created,
            is_compressible: value.is_compressible.map(Into::into).unwrap_or_default(),
            is_compressed: value.is_compressed.map(Into::into).unwrap_or_default(),
            is_frozen: value.is_frozen.map(Into::into).unwrap_or_default(),
            supply: value.supply.map(Into::into),
            seq: value.seq.map(Into::into),
            is_burnt: value.is_burnt.map(Into::into).unwrap_or_default(),
            was_decompressed: value.was_decompressed.map(Into::into).unwrap_or_default(),
            creators: value.creators.map(Into::into).unwrap_or_default(),
            royalty_amount: value.royalty_amount.map(Into::into).unwrap_or_default(),
            authority: value.authority.map(Into::into).unwrap_or_default(),
            owner: value.owner.map(Into::into).unwrap_or_default(),
            delegate: value.delegate.map(Into::into),
            owner_type: value.owner_type.map(Into::into).unwrap_or_default(),
            owner_delegate_seq: value.owner_delegate_seq.map(Into::into),
            asset_leaf: value.asset_leaf.map(Into::into),
            collection: value.collection.map(Into::into),
            onchain_data: value.chain_data.map(Into::into),
            cl_leaf: value.cl_leaf.map(Into::into),
            cl_items: value.cl_items.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Updated<bool>> for DynamicBoolField {
    fn from(value: Updated<bool>) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<u64>> for DynamicUint64Field {
    fn from(value: Updated<u64>) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<String>> for DynamicStringField {
    fn from(value: Updated<String>) -> Self {
        Self {
            value: value.clone().value,
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<u16>> for DynamicUint32Field {
    fn from(value: Updated<u16>) -> Self {
        Self {
            value: value.value as u32,
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<Pubkey>> for DynamicBytesField {
    fn from(value: Updated<Pubkey>) -> Self {
        Self {
            value: value.value.to_bytes().to_vec(),
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<entities::enums::OwnerType>> for DynamicEnumField {
    fn from(value: Updated<entities::enums::OwnerType>) -> Self {
        Self {
            value: OwnerType::from(value.value).into(),
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<entities::enums::ChainMutability>> for DynamicChainMutability {
    fn from(value: Updated<entities::enums::ChainMutability>) -> Self {
        Self {
            value: ChainMutability::from(value.value).into(),
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
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
impl From<Updated<Vec<entities::models::Creator>>> for DynamicCreatorsField {
    fn from(value: Updated<Vec<entities::models::Creator>>) -> Self {
        Self {
            creators: value.value.iter().map(|v| v.into()).collect(),
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<entities::models::AssetLeaf>> for AssetLeaf {
    fn from(value: Updated<entities::models::AssetLeaf>) -> Self {
        Self {
            tree_id: value.value.tree_id.to_bytes().to_vec(),
            leaf: value.value.leaf.clone(),
            nonce: value.value.nonce,
            data_hash: value.value.data_hash.map(|h| h.to_bytes().to_vec()),
            creator_hash: value.value.creator_hash.map(|h| h.to_bytes().to_vec()),
            leaf_seq: value.value.leaf_seq,
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<DynamicBoolField> for Updated<bool> {
    fn from(value: DynamicBoolField) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<DynamicUint64Field> for Updated<u64> {
    fn from(value: DynamicUint64Field) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<DynamicUint32Field> for Updated<u16> {
    fn from(value: DynamicUint32Field) -> Self {
        Self {
            value: value.value as u16,
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<DynamicBytesField> for Updated<Pubkey> {
    fn from(value: DynamicBytesField) -> Self {
        Self {
            value: Pubkey::try_from(value.value).unwrap_or_default(),
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<DynamicEnumField> for Updated<entities::enums::OwnerType> {
    fn from(value: DynamicEnumField) -> Self {
        Self {
            value: entities::enums::OwnerType::from(
                OwnerType::try_from(value.value).unwrap_or_default(),
            ),
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<Creator> for entities::models::Creator {
    fn from(value: Creator) -> Self {
        Self {
            creator: Pubkey::try_from(value.creator).unwrap_or_default(),
            creator_verified: value.creator_verified,
            creator_share: value.creator_share as u8,
        }
    }
}
impl From<DynamicCreatorsField> for Updated<Vec<entities::models::Creator>> {
    fn from(value: DynamicCreatorsField) -> Self {
        Self {
            value: value.creators.into_iter().map(|v| v.into()).collect(),
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<AssetLeaf> for Updated<entities::models::AssetLeaf> {
    fn from(value: AssetLeaf) -> Self {
        Self {
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
            value: entities::models::AssetLeaf {
                tree_id: Pubkey::try_from(value.tree_id).unwrap_or_default(),
                leaf: value.leaf.clone(),
                nonce: value.nonce,
                data_hash: value
                    .data_hash
                    .map(|h| Hash::from(<[u8; 32]>::try_from(h).unwrap_or_default())),
                creator_hash: value
                    .creator_hash
                    .map(|h| Hash::from(<[u8; 32]>::try_from(h).unwrap_or_default())),
                leaf_seq: value.leaf_seq,
            },
        }
    }
}

impl From<entities::models::ClLeaf> for ClLeaf {
    fn from(value: entities::models::ClLeaf) -> Self {
        Self {
            cli_leaf_idx: value.cli_leaf_idx,
            cli_tree_key: value.cli_tree_key.to_bytes().to_vec(),
            cli_node_idx: value.cli_node_idx,
        }
    }
}

impl From<entities::models::ClItem> for ClItem {
    fn from(value: entities::models::ClItem) -> Self {
        Self {
            cli_leaf_idx: value.cli_leaf_idx,
            cli_seq: value.cli_seq,
            cli_level: value.cli_level,
            cli_hash: value.cli_hash,
            cli_tree_key: value.cli_tree_key.to_bytes().to_vec(),
            cli_node_idx: value.cli_node_idx,
            slot_updated: value.slot_updated,
        }
    }
}

impl From<Updated<entities::models::AssetCollection>> for AssetCollection {
    fn from(value: Updated<entities::models::AssetCollection>) -> Self {
        Self {
            collection: value.value.collection.to_bytes().to_vec(),
            is_collection_verified: value.value.is_collection_verified,
            collection_seq: value.value.collection_seq,
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
        }
    }
}

impl From<Updated<entities::models::ChainDataV1>> for ChainDataV1 {
    fn from(value: Updated<entities::models::ChainDataV1>) -> Self {
        Self {
            name: value.value.name.clone(),
            symbol: value.value.symbol.clone(),
            edition_nonce: value.value.edition_nonce.map(|v| v as u32),
            primary_sale_happened: value.value.primary_sale_happened,
            token_standard: value
                .clone()
                .value
                .token_standard
                .map(|v| TokenStandard::from(v).into())
                .unwrap_or_default(),
            uses: value.clone().value.uses.map(|v| v.into()),
            slot_updated: value.slot_updated,
            seq_updated: value.get_upd_ver_seq(),
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

impl From<entities::models::MasterEdition> for MasterEdition {
    fn from(value: entities::models::MasterEdition) -> Self {
        Self {
            key: value.key.to_bytes().to_vec(),
            supply: value.supply,
            max_supply: value.max_supply,
            write_version: value.write_version,
        }
    }
}

impl From<entities::models::EditionV1> for EditionV1 {
    fn from(value: entities::models::EditionV1) -> Self {
        Self {
            key: value.key.to_bytes().to_vec(),
            parent: value.parent.to_bytes().to_vec(),
            edition: value.edition,
            write_version: value.write_version,
        }
    }
}


impl From<ClLeaf> for entities::models::ClLeaf {
    fn from(value: ClLeaf) -> Self {
        Self {
            cli_leaf_idx: value.cli_leaf_idx,
            cli_tree_key: Pubkey::try_from(value.cli_tree_key).unwrap_or_default(),
            cli_node_idx: value.cli_node_idx,
        }
    }
}

impl From<ClItem> for entities::models::ClItem {
    fn from(value: ClItem) -> Self {
        Self {
            cli_leaf_idx: value.cli_leaf_idx,
            cli_seq: value.cli_seq,
            cli_level: value.cli_level,
            cli_hash: value.cli_hash,
            cli_tree_key: Pubkey::try_from(value.cli_tree_key).unwrap_or_default(),
            cli_node_idx: value.cli_node_idx,
            slot_updated: value.slot_updated,
        }
    }
}

impl From<AssetCollection> for Updated<entities::models::AssetCollection> {
    fn from(value: AssetCollection) -> Self {
        Self {
            value: entities::models::AssetCollection {
                collection: Pubkey::try_from(value.collection).unwrap_or_default(),
                is_collection_verified: value.is_collection_verified,
                collection_seq: value.collection_seq,
            },
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<ChainDataV1> for Updated<entities::models::ChainDataV1> {
    fn from(value: ChainDataV1) -> Self {
        Self {
            value: entities::models::ChainDataV1 {
                name: value.name.clone(),
                symbol: value.symbol.clone(),
                edition_nonce: value.edition_nonce.map(|v| v as u8),
                primary_sale_happened: value.primary_sale_happened,
                token_standard: Some(entities::enums::TokenStandard::from(
                    TokenStandard::try_from(value.token_standard).unwrap_or_default(),
                )),
                uses: value.uses.map(|v| v.into()),
            },
            slot_updated: value.slot_updated,
            seq: value.seq_updated,
        }
    }
}

impl From<Uses> for entities::models::Uses {
    fn from(value: Uses) -> Self {
        Self {
            use_method: entities::enums::UseMethod::from(
                UseMethod::try_from(value.use_method).unwrap_or_default(),
            ),
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

        impl From<$dst> for $src {
            fn from(value: $dst) -> Self {
                match value {
                    $(
                        $dst::$variant => <$src>::$variant,
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
    IdentityNft,
    MplCoreAsset,
    MplCoreCollection
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
impl_from_enum!(
    entities::enums::ChainMutability,
    ChainMutability,
    Immutable,
    Mutable
);
