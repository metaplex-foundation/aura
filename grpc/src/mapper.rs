use crate::gapfiller::{
    AssetCollection, AssetDetails, AssetLeaf, ChainDataV1, ChainMutability, ClItem, ClLeaf,
    Creator, DynamicBoolField, DynamicBytesField, DynamicChainMutability, DynamicCreatorsField,
    DynamicEnumField, DynamicStringField, DynamicUint32Field, DynamicUint64Field, EditionV1,
    MasterEdition, OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
    TokenStandard, UpdateVersionValue, UseMethod, Uses,
};
use entities::models::{CompleteAssetDetails, UpdateVersion, Updated};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
impl From<CompleteAssetDetails> for AssetDetails {
    fn from(value: CompleteAssetDetails) -> Self {
        let delegate = value.delegate.value.map(|key| DynamicBytesField {
            value: key.to_bytes().to_vec(),
            slot_updated: value.delegate.slot_updated,
            update_version: value.delegate.update_version.map(Into::into),
        });
        let owner = value.owner.value.map(|key| DynamicBytesField {
            value: key.to_bytes().to_vec(),
            slot_updated: value.owner.slot_updated,
            update_version: value.owner.update_version.map(Into::into),
        });

        let owner_delegate_seq = value
            .owner_delegate_seq
            .value
            .map(|seq| DynamicUint64Field {
                value: seq,
                slot_updated: value.owner_delegate_seq.slot_updated,
                update_version: value.owner_delegate_seq.update_version.map(Into::into),
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

impl TryFrom<AssetDetails> for CompleteAssetDetails {
    type Error = String;

    fn try_from(value: AssetDetails) -> Result<Self, Self::Error> {
        let owner_delegate_seq = value
            .owner_delegate_seq
            .map(|val| {
                Updated::new(
                    val.slot_updated,
                    val.update_version.map(Into::into),
                    Some(val.value),
                )
            })
            .unwrap_or_default();

        Ok(Self {
            pubkey: Pubkey::try_from(value.pubkey).unwrap_or_default(),
            specification_asset_class: entities::enums::SpecificationAssetClass::from(
                SpecificationAssetClass::try_from(value.specification_asset_class)
                    .unwrap_or_default(),
            ),
            royalty_target_type: entities::enums::RoyaltyTargetType::from(
                RoyaltyTargetType::try_from(value.royalty_target_type).unwrap_or_default(),
            ),
            slot_created: value.slot_created,
            edition_address: value
                .edition_address
                .map(TryInto::try_into)
                .transpose()
                .map_err(|e| format!("{:?}", e))?,
            is_compressible: value.is_compressible.map(Into::into).unwrap_or_default(),
            is_compressed: value.is_compressed.map(Into::into).unwrap_or_default(),
            is_frozen: value.is_frozen.map(Into::into).unwrap_or_default(),
            supply: value.supply.map(Into::into),
            seq: value.seq.map(Into::into),
            is_burnt: value.is_burnt.map(Into::into).unwrap_or_default(),
            was_decompressed: value.was_decompressed.map(Into::into).unwrap_or_default(),
            creators: value.creators.map(Into::into).unwrap_or_default(),
            royalty_amount: value.royalty_amount.map(Into::into).unwrap_or_default(),
            url: Default::default(),
            chain_mutability: None,
            lamports: value.lamports.map(Into::into),
            executable: value.executable.map(Into::into),
            metadata_owner: value.metadata_owner.map(Into::into),
            authority: value
                .authority
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
            owner: value
                .owner
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
            delegate: value
                .delegate
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
            owner_type: value.owner_type.map(Into::into).unwrap_or_default(),
            owner_delegate_seq,
            asset_leaf: value.asset_leaf.map(TryInto::try_into).transpose()?,
            collection: value.collection.map(TryInto::try_into).transpose()?,
            onchain_data: value.chain_data.map(Into::into),
            cl_leaf: value.cl_leaf.map(TryInto::try_into).transpose()?,
            cl_items: value
                .cl_items
                .into_iter()
                .flat_map(TryInto::try_into)
                .collect(),
            edition: value.edition.map(TryInto::try_into).transpose()?,
            master_edition: value.master_edition.map(TryInto::try_into).transpose()?,
        })
    }
}

impl From<UpdateVersion> for UpdateVersionValue {
    fn from(value: UpdateVersion) -> Self {
        match value {
            UpdateVersion::Sequence(seq) => Self {
                r#type: crate::gapfiller::UpdateVersion::Sequence.into(),
                value: seq,
            },
            UpdateVersion::WriteVersion(wv) => Self {
                r#type: crate::gapfiller::UpdateVersion::WriteVersion.into(),
                value: wv,
            },
        }
    }
}

impl From<UpdateVersionValue> for UpdateVersion {
    fn from(value: UpdateVersionValue) -> Self {
        if value.r#type == crate::gapfiller::UpdateVersion::Sequence as i32 {
            return Self::Sequence(value.value);
        }
        Self::WriteVersion(value.value)
    }
}

impl From<Updated<bool>> for DynamicBoolField {
    fn from(value: Updated<bool>) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<Updated<u64>> for DynamicUint64Field {
    fn from(value: Updated<u64>) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<Updated<String>> for DynamicStringField {
    fn from(value: Updated<String>) -> Self {
        Self {
            value: value.clone().value,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<Updated<u16>> for DynamicUint32Field {
    fn from(value: Updated<u16>) -> Self {
        Self {
            value: value.value as u32,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<Updated<Pubkey>> for DynamicBytesField {
    fn from(value: Updated<Pubkey>) -> Self {
        Self {
            value: value.value.to_bytes().to_vec(),
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<Updated<entities::enums::OwnerType>> for DynamicEnumField {
    fn from(value: Updated<entities::enums::OwnerType>) -> Self {
        Self {
            value: OwnerType::from(value.value).into(),
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<Updated<entities::enums::ChainMutability>> for DynamicChainMutability {
    fn from(value: Updated<entities::enums::ChainMutability>) -> Self {
        Self {
            value: ChainMutability::from(value.value).into(),
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
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
            update_version: value.update_version.map(Into::into),
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
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<DynamicBoolField> for Updated<bool> {
    fn from(value: DynamicBoolField) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<DynamicUint64Field> for Updated<u64> {
    fn from(value: DynamicUint64Field) -> Self {
        Self {
            value: value.value,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<DynamicUint32Field> for Updated<u16> {
    fn from(value: DynamicUint32Field) -> Self {
        Self {
            value: value.value as u16,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl From<DynamicStringField> for Updated<String> {
    fn from(value: DynamicStringField) -> Self {
        Self {
            value: value.clone().value,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl TryFrom<DynamicBytesField> for Updated<Option<Pubkey>> {
    type Error = String;

    fn try_from(value: DynamicBytesField) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.slot_updated,
            value.update_version.map(Into::into),
            Some(Pubkey::try_from(value.value).map_err(|e| format!("{:?}", e))?),
        ))
    }
}

impl TryFrom<DynamicBytesField> for Updated<Pubkey> {
    type Error = String;

    fn try_from(value: DynamicBytesField) -> Result<Self, Self::Error> {
        Ok(Self {
            value: Pubkey::try_from(value.value).map_err(|e| format!("{:?}", e))?,
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        })
    }
}

// TODO
impl From<DynamicEnumField> for Updated<entities::enums::OwnerType> {
    fn from(value: DynamicEnumField) -> Self {
        Self {
            value: entities::enums::OwnerType::from(
                OwnerType::try_from(value.value).unwrap_or_default(),
            ),
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl TryFrom<Creator> for entities::models::Creator {
    type Error = String;

    fn try_from(value: Creator) -> Result<Self, Self::Error> {
        Ok(Self {
            creator: Pubkey::try_from(value.creator).map_err(|e| format!("{:?}", e))?,
            creator_verified: value.creator_verified,
            creator_share: value.creator_share as u8,
        })
    }
}
impl From<DynamicCreatorsField> for Updated<Vec<entities::models::Creator>> {
    fn from(value: DynamicCreatorsField) -> Self {
        Self {
            value: value
                .creators
                .into_iter()
                .flat_map(TryInto::try_into)
                .collect(),
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        }
    }
}

impl TryFrom<AssetLeaf> for Updated<entities::models::AssetLeaf> {
    type Error = String;

    fn try_from(value: AssetLeaf) -> Result<Self, Self::Error> {
        Ok(Self {
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
            value: entities::models::AssetLeaf {
                tree_id: Pubkey::try_from(value.tree_id).map_err(|e| format!("{:?}", e))?,
                leaf: value.leaf.clone(),
                nonce: value.nonce,
                data_hash: value
                    .data_hash
                    .map(|h| {
                        Ok::<_, String>(Hash::from(
                            <[u8; 32]>::try_from(h).map_err(|e| format!("{:?}", e))?,
                        ))
                    })
                    .transpose()?,
                creator_hash: value
                    .creator_hash
                    .map(|h| {
                        Ok::<_, String>(Hash::from(
                            <[u8; 32]>::try_from(h).map_err(|e| format!("{:?}", e))?,
                        ))
                    })
                    .transpose()?,
                leaf_seq: value.leaf_seq,
            },
        })
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
            update_version: value.update_version.map(Into::into),
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
            update_version: value.update_version.map(Into::into),
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

impl TryFrom<MasterEdition> for entities::models::MasterEdition {
    type Error = String;

    fn try_from(value: MasterEdition) -> Result<Self, Self::Error> {
        Ok(Self {
            key: Pubkey::try_from(value.key).map_err(|e| format!("{:?}", e))?,
            supply: value.supply,
            max_supply: value.max_supply,
            write_version: value.write_version,
        })
    }
}

impl TryFrom<EditionV1> for entities::models::EditionV1 {
    type Error = String;

    fn try_from(value: EditionV1) -> Result<Self, Self::Error> {
        Ok(Self {
            key: Pubkey::try_from(value.key).map_err(|e| format!("{:?}", e))?,
            parent: Pubkey::try_from(value.parent).map_err(|e| format!("{:?}", e))?,
            edition: value.edition,
            write_version: value.write_version,
        })
    }
}

impl TryFrom<ClLeaf> for entities::models::ClLeaf {
    type Error = String;

    fn try_from(value: ClLeaf) -> Result<Self, Self::Error> {
        Ok(Self {
            cli_leaf_idx: value.cli_leaf_idx,
            cli_tree_key: Pubkey::try_from(value.cli_tree_key).map_err(|e| format!("{:?}", e))?,
            cli_node_idx: value.cli_node_idx,
        })
    }
}

impl TryFrom<ClItem> for entities::models::ClItem {
    type Error = String;

    fn try_from(value: ClItem) -> Result<Self, Self::Error> {
        Ok(Self {
            cli_leaf_idx: value.cli_leaf_idx,
            cli_seq: value.cli_seq,
            cli_level: value.cli_level,
            cli_hash: value.cli_hash,
            cli_tree_key: Pubkey::try_from(value.cli_tree_key).map_err(|e| format!("{:?}", e))?,
            cli_node_idx: value.cli_node_idx,
            slot_updated: value.slot_updated,
        })
    }
}

impl TryFrom<AssetCollection> for Updated<entities::models::AssetCollection> {
    type Error = String;

    fn try_from(value: AssetCollection) -> Result<Self, Self::Error> {
        Ok(Self {
            value: entities::models::AssetCollection {
                collection: Pubkey::try_from(value.collection).map_err(|e| format!("{:?}", e))?,
                is_collection_verified: value.is_collection_verified,
                collection_seq: value.collection_seq,
            },
            slot_updated: value.slot_updated,
            update_version: value.update_version.map(Into::into),
        })
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
            update_version: value.update_version.map(Into::into),
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
