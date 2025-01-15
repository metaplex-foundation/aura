use crate::models::{
    BurntMetadataSlot, CoreAssetFee, EditionMetadata, EditionV1, IndexableAssetWithAccountInfo,
    InscriptionDataInfo, InscriptionInfo, MasterEdition, MetadataInfo, Mint, TokenAccount,
};
use num_derive::FromPrimitive;
use schemars::JsonSchema;
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
    MplCoreAsset,
    MplCoreCollection,
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

impl From<blockbuster::token_metadata::types::TokenStandard> for TokenStandard {
    fn from(value: blockbuster::token_metadata::types::TokenStandard) -> Self {
        match value {
            blockbuster::token_metadata::types::TokenStandard::NonFungible => {
                TokenStandard::NonFungible
            }
            blockbuster::token_metadata::types::TokenStandard::FungibleAsset => {
                TokenStandard::FungibleAsset
            }
            blockbuster::token_metadata::types::TokenStandard::Fungible => TokenStandard::Fungible,
            blockbuster::token_metadata::types::TokenStandard::NonFungibleEdition => {
                TokenStandard::NonFungibleEdition
            }
            blockbuster::token_metadata::types::TokenStandard::ProgrammableNonFungible => {
                TokenStandard::ProgrammableNonFungible
            }
            blockbuster::token_metadata::types::TokenStandard::ProgrammableNonFungibleEdition => {
                TokenStandard::ProgrammableNonFungibleEdition
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, FromPrimitive, PartialEq, JsonSchema)]
pub enum UseMethod {
    Burn,
    Multiple,
    Single,
}

impl From<String> for UseMethod {
    fn from(s: String) -> Self {
        match &*s {
            "Burn" => UseMethod::Burn,
            "Single" => UseMethod::Single,
            "Multiple" => UseMethod::Multiple,
            _ => UseMethod::Single,
        }
    }
}

impl From<blockbuster::token_metadata::types::UseMethod> for UseMethod {
    fn from(value: blockbuster::token_metadata::types::UseMethod) -> Self {
        match value {
            blockbuster::token_metadata::types::UseMethod::Burn => UseMethod::Burn,
            blockbuster::token_metadata::types::UseMethod::Multiple => UseMethod::Multiple,
            blockbuster::token_metadata::types::UseMethod::Single => UseMethod::Single,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Copy, PartialEq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema)]
pub enum Interface {
    #[serde(rename = "V1_NFT")]
    V1NFT,
    #[serde(rename = "V1_PRINT")]
    V1PRINT,
    #[serde(rename = "LEGACY_NFT")]
    LegacyNft,
    #[serde(rename = "V2_NFT")]
    Nft,
    #[serde(rename = "FungibleAsset")]
    FungibleAsset,
    #[serde(rename = "FungibleToken")]
    FungibleToken,
    #[serde(rename = "Custom")]
    Custom,
    #[serde(rename = "Identity")]
    Identity,
    #[serde(rename = "Executable")]
    Executable,
    #[serde(rename = "ProgrammableNFT")]
    ProgrammableNFT,
    #[serde(rename = "MplCoreAsset")]
    MplCoreAsset,
    #[serde(rename = "MplCoreCollection")]
    MplCoreCollection,
}

impl From<Interface> for SpecificationAssetClass {
    fn from(interface: Interface) -> Self {
        match interface {
            Interface::FungibleAsset => Self::FungibleAsset,
            Interface::FungibleToken => Self::FungibleToken,
            Interface::Nft | Interface::V1NFT | Interface::LegacyNft => Self::Nft,
            Interface::V1PRINT => Self::PrintableNft,
            Interface::ProgrammableNFT => Self::ProgrammableNft,
            Interface::Custom | Interface::Executable | Interface::Identity => Self::Unknown,
            Interface::MplCoreAsset => Self::MplCoreAsset,
            Interface::MplCoreCollection => Self::MplCoreCollection,
        }
    }
}

impl From<(&SpecificationVersions, &SpecificationAssetClass)> for Interface {
    fn from(i: (&SpecificationVersions, &SpecificationAssetClass)) -> Self {
        match i {
            (SpecificationVersions::V1, SpecificationAssetClass::Nft) => Interface::V1NFT,
            (SpecificationVersions::V1, SpecificationAssetClass::PrintableNft) => Interface::V1NFT,
            (SpecificationVersions::V0, SpecificationAssetClass::Nft) => Interface::LegacyNft,
            (SpecificationVersions::V1, SpecificationAssetClass::ProgrammableNft) => {
                Interface::ProgrammableNFT
            }
            (_, SpecificationAssetClass::FungibleAsset) => Interface::FungibleAsset,
            (_, SpecificationAssetClass::FungibleToken) => Interface::FungibleToken,
            (_, SpecificationAssetClass::MplCoreAsset) => Interface::MplCoreAsset,
            (_, SpecificationAssetClass::MplCoreCollection) => Interface::MplCoreCollection,
            _ => Interface::Custom,
        }
    }
}

impl From<Interface> for (SpecificationVersions, SpecificationAssetClass) {
    fn from(val: Interface) -> Self {
        match val {
            Interface::V1NFT => (SpecificationVersions::V1, SpecificationAssetClass::Nft),
            Interface::LegacyNft => (SpecificationVersions::V0, SpecificationAssetClass::Nft),
            Interface::ProgrammableNFT => (
                SpecificationVersions::V1,
                SpecificationAssetClass::ProgrammableNft,
            ),
            Interface::V1PRINT => (SpecificationVersions::V1, SpecificationAssetClass::PrintableNft),
            Interface::FungibleAsset => (
                SpecificationVersions::V1,
                SpecificationAssetClass::FungibleAsset,
            ),
            Interface::MplCoreAsset => (
                SpecificationVersions::V1,
                SpecificationAssetClass::MplCoreAsset,
            ),
            Interface::MplCoreCollection => (
                SpecificationVersions::V1,
                SpecificationAssetClass::MplCoreCollection,
            ),
            _ => (SpecificationVersions::V1, SpecificationAssetClass::Unknown),
        }
    }
}

impl From<Interface> for SpecificationVersions {
    fn from(interface: Interface) -> Self {
        match interface {
            Interface::LegacyNft => Self::V0,
            _ => Self::V1,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema)]
pub enum OwnershipModel {
    #[serde(rename = "single")]
    Single,
    #[serde(rename = "token")]
    Token,
}

impl From<OwnershipModel> for OwnerType {
    fn from(ownership_model: OwnershipModel) -> Self {
        match ownership_model {
            OwnershipModel::Single => Self::Single,
            OwnershipModel::Token => Self::Token,
        }
    }
}

impl From<OwnerType> for OwnershipModel {
    fn from(s: OwnerType) -> Self {
        match s {
            OwnerType::Token => OwnershipModel::Token,
            OwnerType::Single => OwnershipModel::Single,
            _ => OwnershipModel::Single,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema)]
pub enum RoyaltyModel {
    #[serde(rename = "creators")]
    Creators,
    #[serde(rename = "fanout")]
    Fanout,
    #[serde(rename = "single")]
    Single,
}

impl From<RoyaltyTargetType> for RoyaltyModel {
    fn from(s: RoyaltyTargetType) -> Self {
        match s {
            RoyaltyTargetType::Creators => RoyaltyModel::Creators,
            RoyaltyTargetType::Fanout => RoyaltyModel::Fanout,
            RoyaltyTargetType::Single => RoyaltyModel::Single,
            _ => RoyaltyModel::Creators,
        }
    }
}

impl From<RoyaltyModel> for RoyaltyTargetType {
    fn from(royalty_model: RoyaltyModel) -> Self {
        match royalty_model {
            RoyaltyModel::Creators => Self::Creators,
            RoyaltyModel::Fanout => Self::Fanout,
            RoyaltyModel::Single => Self::Single,
        }
    }
}

#[derive(
    serde_derive::Deserialize,
    serde_derive::Serialize,
    PartialEq,
    Debug,
    Eq,
    Hash,
    sqlx::Type,
    Copy,
    Clone,
    Default,
)]
#[sqlx(type_name = "task_status", rename_all = "lowercase")]
pub enum TaskStatus {
    #[default]
    Pending,
    Running,
    Success,
    Failed,
}

#[derive(Serialize, Deserialize, Debug, Clone, FromPrimitive)]
pub enum BatchMintState {
    Uploaded,
    ValidationFail,
    ValidationComplete,
    UploadedToArweave,
    FailUploadToArweave,
    FailSendingTransaction,
    Complete,
}

#[derive(Serialize, Deserialize, Debug, Clone, FromPrimitive)]
pub enum PersistingBatchMintState {
    ReceivedTransaction,
    FailedToPersist,
    SuccessfullyDownload,
    SuccessfullyValidate,
    StoredUpdate,
}

#[derive(Serialize, Deserialize, Debug, Clone, FromPrimitive, PartialEq)]
pub enum FailedBatchMintState {
    DownloadFailed,
    ChecksumVerifyFailed,
    BatchMintVerifyFailed,
    FileSerialization,
}

impl From<FailedBatchMintState> for u8 {
    fn from(value: FailedBatchMintState) -> Self {
        match value {
            FailedBatchMintState::DownloadFailed => 0,
            FailedBatchMintState::ChecksumVerifyFailed => 1,
            FailedBatchMintState::BatchMintVerifyFailed => 2,
            FailedBatchMintState::FileSerialization => 3,
        }
    }
}

impl TryFrom<u8> for FailedBatchMintState {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FailedBatchMintState::DownloadFailed),
            1 => Ok(FailedBatchMintState::ChecksumVerifyFailed),
            2 => Ok(FailedBatchMintState::BatchMintVerifyFailed),
            3 => Ok(FailedBatchMintState::FileSerialization),
            _ => Err("Wrong enum value".to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TokenMetadataEdition {
    EditionV1(EditionV1),
    MasterEdition(MasterEdition),
}

pub enum UnprocessedAccount {
    MetadataInfo(MetadataInfo),
    Token(TokenAccount),
    Mint(Box<Mint>),
    Edition(EditionMetadata),
    BurnMetadata(BurntMetadataSlot),
    BurnMplCore(BurntMetadataSlot),
    MplCore(IndexableAssetWithAccountInfo),
    Inscription(InscriptionInfo),
    InscriptionData(InscriptionDataInfo),
    MplCoreFee(CoreAssetFee),
}

impl std::fmt::Debug for UnprocessedAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnprocessedAccount::MetadataInfo(_) => write!(f, "UnprocessedAccount::MetadataInfo"),
            UnprocessedAccount::Token(_) => write!(f, "UnprocessedAccount::Token"),
            UnprocessedAccount::Mint(_) => write!(f, "UnprocessedAccount::Mint"),
            UnprocessedAccount::Edition(_) => write!(f, "UnprocessedAccount::Edition"),
            UnprocessedAccount::BurnMetadata(_) => write!(f, "UnprocessedAccount::BurnMetadata"),
            UnprocessedAccount::BurnMplCore(_) => write!(f, "UnprocessedAccount::BurnMplCore"),
            UnprocessedAccount::MplCore(_) => write!(f, "UnprocessedAccount::MplCore"),
            UnprocessedAccount::Inscription(_) => write!(f, "UnprocessedAccount::Inscription"),
            UnprocessedAccount::InscriptionData(_) => {
                write!(f, "UnprocessedAccount::InscriptionData")
            }
            UnprocessedAccount::MplCoreFee(_) => write!(f, "UnprocessedAccount::MplCoreFee"),
        }
    }
}

impl From<UnprocessedAccount> for &str {
    fn from(value: UnprocessedAccount) -> Self {
        match value {
            UnprocessedAccount::MetadataInfo(_) => "MetadataInfo",
            UnprocessedAccount::Token(_) => "TokenAccount",
            UnprocessedAccount::Mint(_) => "Mint",
            UnprocessedAccount::Edition(_) => "Edition",
            UnprocessedAccount::BurnMetadata(_) => "BurnMetadata",
            UnprocessedAccount::BurnMplCore(_) => "BurnMplCore",
            UnprocessedAccount::MplCore(_) => "MplCore",
            UnprocessedAccount::Inscription(_) => "Inscription",
            UnprocessedAccount::InscriptionData(_) => "InscriptionData",
            UnprocessedAccount::MplCoreFee(_) => "MplCoreFee",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TokenType {
    Fungible,
    NonFungible,
    #[serde(rename = "regularNFT")]
    RegularNFT,
    #[serde(rename = "compressedNFT")]
    CompressedNFT,
    All,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum AssetType {
    NonFungible = 1,
    Fungible = 2,
}

pub const ASSET_TYPES: [AssetType; 2] = [AssetType::Fungible, AssetType::NonFungible];
