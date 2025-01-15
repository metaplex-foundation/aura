use std::cmp::Ordering;

use entities::{enums::*, models::*};
use solana_sdk::pubkey::Pubkey;

use crate::generated::asset_generated::asset as fb;

macro_rules! impl_from_enum {
    ($src:ty, $dst:ty, $($variant:ident),*) => {
        impl From<$src> for $dst {
            fn from(value: $src) -> Self {
                match value {
                    $(
                        <$src>::$variant => <$dst>::$variant,
                    )*
                }
            }
        }

        impl From<$dst> for $src {
            fn from(value: $dst) -> Self {
                match value {
                    $(
                        <$dst>::$variant => <$src>::$variant,
                    )*
                    _ => <$src>::Unknown,
                }
            }
        }
    };
}

impl_from_enum!(
    SpecificationAssetClass,
    fb::SpecificationAssetClass,
    Unknown,
    FungibleToken,
    FungibleAsset,
    Nft,
    PrintableNft,
    ProgrammableNft,
    MplCoreAsset,
    MplCoreCollection
);

impl_from_enum!(RoyaltyTargetType, fb::RoyaltyTargetType, Unknown, Creators, Fanout, Single);
impl_from_enum!(OwnerType, fb::OwnerType, Unknown, Token, Single);

impl From<ChainMutability> for fb::ChainMutability {
    fn from(value: ChainMutability) -> Self {
        match value {
            ChainMutability::Mutable => fb::ChainMutability::Mutable,
            ChainMutability::Immutable => fb::ChainMutability::Immutable,
        }
    }
}

impl From<fb::ChainMutability> for ChainMutability {
    fn from(value: fb::ChainMutability) -> Self {
        match value {
            fb::ChainMutability::Mutable => ChainMutability::Mutable,
            fb::ChainMutability::Immutable => ChainMutability::Immutable,
            _ => ChainMutability::Immutable,
        }
    }
}

impl<'a> fb::AssetCompleteDetails<'a> {
    pub fn get_slot_updated(&'a self) -> u64 {
        // Collect the slot_updated values from all available fields
        let slots = [
            self.dynamic_details().and_then(|d| d.is_compressible()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.is_compressed()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.is_frozen()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.supply()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.seq()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.is_burnt()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.was_decompressed()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.onchain_data()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.creators()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.royalty_amount()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.url()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.chain_mutability()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.lamports()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.executable()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.metadata_owner()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.raw_name()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.mpl_core_plugins()).map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_unknown_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.rent_epoch()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.num_minted()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.current_size()).map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.plugins_json_version()).map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_external_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_unknown_external_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details().and_then(|d| d.mint_extensions()).map(|c| c.slot_updated()),
            self.authority().map(|a| a.slot_updated()),
            self.owner().and_then(|o| o.owner()).map(|o| o.slot_updated()),
            self.owner().and_then(|o| o.delegate()).map(|o| o.slot_updated()),
            self.owner().and_then(|o| o.owner_type()).map(|o| o.slot_updated()),
            self.owner().and_then(|o| o.owner_delegate_seq()).map(|o| o.slot_updated()),
            self.collection().and_then(|c| c.collection()).map(|o| o.slot_updated()),
            self.collection().and_then(|c| c.is_collection_verified()).map(|o| o.slot_updated()),
            self.collection().and_then(|c| c.authority()).map(|o| o.slot_updated()),
        ];
        // Filter out None values and find the maximum slot_updated
        slots.iter().filter_map(|&slot| slot).max().unwrap_or(0)
    }
}

impl<'a> From<fb::Creator<'a>> for Creator {
    fn from(value: fb::Creator<'a>) -> Self {
        Creator {
            creator: Pubkey::try_from(value.creator().unwrap().bytes()).unwrap(),
            creator_verified: value.creator_verified(),
            creator_share: value.creator_share() as u8,
        }
    }
}

impl<'a> From<fb::AssetCompleteDetails<'a>> for AssetIndex {
    fn from(value: fb::AssetCompleteDetails<'a>) -> Self {
        let pubkey = Pubkey::try_from(value.pubkey().unwrap().bytes()).unwrap();
        AssetIndex {
            pubkey,
            specification_version: SpecificationVersions::V1,
            specification_asset_class: value
                .static_details()
                .map(|v| v.specification_asset_class().into())
                .unwrap_or_default(),
            royalty_target_type: value
                .static_details()
                .map(|a| a.royalty_target_type().into())
                .unwrap_or_default(),
            slot_created: value.static_details().map(|a| a.created_at()).unwrap_or_default(),
            owner_type: value.owner().and_then(|o| o.owner_type()).map(|u| u.value().into()),
            owner: value
                .owner()
                .and_then(|o| o.owner())
                .and_then(|u| u.value())
                .map(|k| Pubkey::try_from(k.bytes()).unwrap()),
            delegate: value
                .owner()
                .and_then(|o| o.delegate())
                .and_then(|u| u.value())
                .map(|k| Pubkey::try_from(k.bytes()).unwrap()),
            authority: value
                .authority()
                .and_then(|a| a.authority())
                .map(|k| Pubkey::try_from(k.bytes()).unwrap()),
            collection: value
                .collection()
                .and_then(|c| c.collection())
                .and_then(|u| u.value())
                .map(|k| Pubkey::try_from(k.bytes()).unwrap()),
            is_collection_verified: value
                .collection()
                .and_then(|c| c.is_collection_verified())
                .map(|u| u.value()),
            creators: value
                .dynamic_details()
                .and_then(|d| d.creators())
                .and_then(|u| u.value())
                .map(|v| v.iter().map(Creator::from).collect())
                .unwrap_or_default(),
            royalty_amount: value
                .dynamic_details()
                .and_then(|u| u.royalty_amount())
                .map(|d| d.value() as i64)
                .unwrap_or_default(),
            is_burnt: value
                .dynamic_details()
                .and_then(|d| d.is_burnt())
                .map(|u| u.value())
                .unwrap_or_default(),
            is_compressible: value
                .dynamic_details()
                .and_then(|d| d.is_compressible())
                .map(|u| u.value())
                .unwrap_or_default(),
            is_compressed: value
                .dynamic_details()
                .and_then(|d| d.is_compressed())
                .map(|u| u.value())
                .unwrap_or_default(),
            is_frozen: value
                .dynamic_details()
                .and_then(|d| d.is_frozen())
                .map(|u| u.value())
                .unwrap_or_default(),
            supply: value.dynamic_details().and_then(|d| d.supply()).map(|u| u.value() as i64),
            update_authority: None, // requires mpl core collections
            metadata_url: value
                .dynamic_details()
                .and_then(|d| d.url())
                .and_then(|u| u.value())
                .filter(|s| !s.is_empty())
                .map(|s| UrlWithStatus::new(s, false)),
            slot_updated: value.get_slot_updated() as i64,
            fungible_asset_mint: None,
            fungible_asset_balance: None,
        }
    }
}

impl PartialOrd for fb::UpdateVersion<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.version_type(), other.version_type()) {
            (fb::UpdateVersionType::Sequence, fb::UpdateVersionType::Sequence)
            | (fb::UpdateVersionType::WriteVersion, fb::UpdateVersionType::WriteVersion) => {
                self.version_value().partial_cmp(&other.version_value())
            },
            // this is asset decompress case. Update with write version field is always most recent
            (fb::UpdateVersionType::Sequence, fb::UpdateVersionType::WriteVersion) => {
                Some(Ordering::Less)
            },
            (fb::UpdateVersionType::WriteVersion, fb::UpdateVersionType::Sequence) => None,
            _ => None,
        }
    }
}
macro_rules! impl_partial_ord_for_updated {
    ($name:ident) => {
        impl PartialOrd for fb::$name<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(match self.update_version().partial_cmp(&other.update_version()) {
                    Some(std::cmp::Ordering::Equal) => {
                        self.slot_updated().cmp(&other.slot_updated())
                    },
                    Some(ord) => ord,
                    None => self.slot_updated().cmp(&other.slot_updated()),
                })
            }
        }
    };
}

impl_partial_ord_for_updated!(UpdatedBool);
impl_partial_ord_for_updated!(UpdatedU64);
impl_partial_ord_for_updated!(UpdatedU32);
impl_partial_ord_for_updated!(UpdatedString);
impl_partial_ord_for_updated!(UpdatedPubkey);
impl_partial_ord_for_updated!(UpdatedOptionalPubkey);
impl_partial_ord_for_updated!(UpdatedCreators);
impl_partial_ord_for_updated!(UpdatedChainMutability);
impl_partial_ord_for_updated!(UpdatedOwnerType);

impl fb::AssetAuthority<'_> {
    pub fn compare(&self, other: &Self) -> Ordering {
        if let (Some(self_write_version), Some(other_write_version)) = unsafe {
            (
                self._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None),
                other._tab.get::<u64>(fb::AssetAuthority::VT_WRITE_VERSION, None),
            )
        } {
            self_write_version.cmp(&other_write_version)
        } else {
            self.slot_updated().cmp(&other.slot_updated())
        }
    }
}
