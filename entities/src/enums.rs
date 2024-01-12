use num_derive::FromPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum RoyaltyTargetType {
    #[default]
    Unknown,
    Creators,
    Fanout,
    Single,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum SpecificationVersions {
    #[default]
    Unknown,
    V0,
    V1,
    V2,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum SpecificationAssetClass {
    #[default]
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
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Default)]
pub enum OwnerType {
    #[default]
    Unknown,
    Token,
    Single,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TokenStandard {
    NonFungible,                    // This is a master edition
    FungibleAsset,                  // A token with metadata that can also have attributes
    Fungible,                       // A token with simple metadata
    NonFungibleEdition,             // This is a limited edition
    ProgrammableNonFungible,        // NonFungible with programmable configuration
    ProgrammableNonFungibleEdition, // NonFungible with programmable configuration
}

#[derive(Serialize, Deserialize, Debug, Clone, FromPrimitive)]
pub enum UseMethod {
    Burn,
    Multiple,
    Single,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Copy)]
pub enum ChainMutability {
    // Original implementation also contain "Unknown"
    // enum variant, which is default. But we do not saved any
    // previous versions of ChainMutability, so if we will want to
    // use unwrap_or_default() on Option<ChainMutability>, it is
    // convenient to have Immutable variant as default, because
    // previous we marked all ChainData as Immutable
    #[default]
    Immutable,
    Mutable,
}
