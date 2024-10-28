use crate::asset_generated::asset as fb;
use crate::enums::*;
use crate::models::*;

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
    Print,
    TransferRestrictedNft,
    NonTransferableNft,
    IdentityNft,
    MplCoreAsset,
    MplCoreCollection
);

impl_from_enum!(
    RoyaltyTargetType,
    fb::RoyaltyTargetType,
    Unknown,
    Creators,
    Fanout,
    Single
);
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
            self.dynamic_details()
                .and_then(|d| d.is_compressible())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.is_compressed())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.is_frozen())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.supply())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.seq())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.is_burnt())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.was_decompressed())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.onchain_data())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.creators())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.royalty_amount())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.url())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.chain_mutability())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.lamports())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.executable())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.metadata_owner())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.raw_name())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_unknown_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.rent_epoch())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.num_minted())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.current_size())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.plugins_json_version())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_external_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mpl_core_unknown_external_plugins())
                .map(|c| c.slot_updated()),
            self.dynamic_details()
                .and_then(|d| d.mint_extensions())
                .map(|c| c.slot_updated()),
            self.authority().map(|a| a.slot_updated()),
            self.owner()
                .and_then(|o| o.owner())
                .map(|o| o.slot_updated()),
            self.owner()
                .and_then(|o| o.delegate())
                .map(|o| o.slot_updated()),
            self.owner()
                .and_then(|o| o.owner_type())
                .map(|o| o.slot_updated()),
            self.owner()
                .and_then(|o| o.owner_delegate_seq())
                .map(|o| o.slot_updated()),
            self.collection()
                .and_then(|c| c.collection())
                .map(|o| o.slot_updated()),
            self.collection()
                .and_then(|c| c.is_collection_verified())
                .map(|o| o.slot_updated()),
            self.collection()
                .and_then(|c| c.authority())
                .map(|o| o.slot_updated()),
        ];
        // Filter out None values and find the maximum slot_updated
        slots.iter().filter_map(|&slot| slot).max().unwrap_or(0)
    }
}
