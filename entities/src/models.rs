use crate::enums::{
    BatchMintState, ChainMutability, FailedBatchMintState, OwnerType, PersistingBatchMintState,
    RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions, TaskStatus,
    TokenMetadataEdition, TokenStandard, UnprocessedAccount, UseMethod,
};
use base64::engine::general_purpose;
use base64::Engine;
use blockbuster::programs::mpl_core_program::MplCoreAccountData;
use blockbuster::programs::token_extensions::{MintAccountExtensions, TokenAccountExtensions};
use libreplex_inscriptions::Inscription;
use mpl_token_metadata::accounts::Metadata;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};
use sqlx::types::chrono;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq, Hash)]
pub struct UrlWithStatus {
    pub metadata_url: String,
    pub is_downloaded: bool,
}

impl UrlWithStatus {
    pub fn new(metadata_url: &str, is_downloaded: bool) -> Self {
        Self {
            metadata_url: metadata_url.trim().replace('\0', "").to_string(),
            is_downloaded,
        }
    }
    pub fn get_metadata_id(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        // triming the url to remove any leading or trailing whitespaces,
        // as some of the legacy versions of the database have contained the urls with whitespaces
        let url = self.metadata_url.trim();
        hasher.update(url);
        hasher.finalize().to_vec()
    }
}

// AssetIndex is the struct that is stored in the postgres database and is used to query the asset pubkeys.
// Contains values that from multiple tables
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct AssetIndex {
    // immutable fields
    pub pubkey: Pubkey,
    pub specification_version: SpecificationVersions,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub slot_created: i64,
    // mutable fields
    pub owner_type: Option<OwnerType>,
    pub owner: Option<Pubkey>,
    pub delegate: Option<Pubkey>,
    pub authority: Option<Pubkey>,
    pub collection: Option<Pubkey>,
    pub is_collection_verified: Option<bool>,
    pub creators: Vec<Creator>,
    pub royalty_amount: i64,
    pub is_burnt: bool,
    pub is_compressible: bool,
    pub is_compressed: bool,
    pub is_frozen: bool,
    pub supply: Option<i64>,
    pub metadata_url: Option<UrlWithStatus>,
    pub update_authority: Option<Pubkey>,
    pub slot_updated: i64,
    pub fungible_tokens: Vec<FungibleToken>,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub struct FungibleToken {
    pub owner: Pubkey,
    pub asset: Pubkey,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Creator {
    pub creator: Pubkey,
    pub creator_verified: bool,
    // In percentages, NOT basis points
    pub creator_share: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct OffChainData {
    pub url: String,
    pub metadata: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CompleteAssetDetails {
    // From AssetStaticDetails
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub slot_created: u64,
    pub edition_address: Option<Pubkey>,

    // From AssetDynamicDetails as Tuples
    pub is_compressible: Updated<bool>,
    pub is_compressed: Updated<bool>,
    pub is_frozen: Updated<bool>,
    pub supply: Option<Updated<u64>>,
    pub seq: Option<Updated<u64>>,
    pub is_burnt: Updated<bool>,
    pub was_decompressed: Updated<bool>,
    pub onchain_data: Option<Updated<ChainDataV1>>,
    pub creators: Updated<Vec<Creator>>,
    pub royalty_amount: Updated<u16>,
    pub url: Updated<String>,
    pub chain_mutability: Option<Updated<ChainMutability>>,
    pub lamports: Option<Updated<u64>>,
    pub executable: Option<Updated<bool>>,
    pub metadata_owner: Option<Updated<String>>,
    pub raw_name: Option<Updated<String>>,
    pub mpl_core_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_plugins: Option<Updated<String>>,
    pub rent_epoch: Option<Updated<u64>>,
    pub num_minted: Option<Updated<u32>>,
    pub current_size: Option<Updated<u32>>,
    pub plugins_json_version: Option<Updated<u32>>,
    pub mpl_core_external_plugins: Option<Updated<String>>,
    pub mpl_core_unknown_external_plugins: Option<Updated<String>>,
    pub mint_extensions: Option<Updated<String>>,

    // From AssetAuthority as Tuple
    pub authority: Updated<Pubkey>,

    // From AssetOwner as Tuples
    pub owner: Updated<Option<Pubkey>>,
    pub delegate: Updated<Option<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Updated<Option<u64>>,

    // Separate fields
    pub asset_leaf: Option<Updated<AssetLeaf>>,
    pub collection: Option<AssetCollection>,

    // Cl elements
    pub cl_leaf: Option<ClLeaf>,
    pub cl_items: Vec<ClItem>,

    // TokenMetadataEdition
    pub edition: Option<EditionV1>,
    pub master_edition: Option<MasterEdition>,

    // OffChainData
    pub offchain_data: Option<OffChainData>,
}

/// Leaf information about compressed asset
/// Nonce - is basically the leaf index. It takes from tree supply.
/// NOTE: leaf index is not the same as node index. Leaf index is specifically the index of the leaf in the tree.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct AssetLeaf {
    pub tree_id: Pubkey,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<u64>,
    pub data_hash: Option<Hash>,
    pub creator_hash: Option<Hash>,
    pub leaf_seq: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Uses {
    pub use_method: UseMethod,
    pub remaining: u64,
    pub total: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChainDataV1 {
    pub name: String,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edition_nonce: Option<u8>,
    pub primary_sale_happened: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_standard: Option<TokenStandard>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uses: Option<Uses>,
}

impl ChainDataV1 {
    pub fn sanitize(&mut self) {
        self.name = self.name.trim().replace('\0', "");
        self.symbol = self.symbol.trim().replace('\0', "");
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCollection {
    pub collection: Updated<Pubkey>,
    pub is_collection_verified: Updated<bool>,
    pub authority: Updated<Option<Pubkey>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct ClItem {
    pub cli_node_idx: u64,
    pub cli_tree_key: Pubkey,
    pub cli_leaf_idx: Option<u64>,
    pub cli_seq: u64,
    pub cli_level: u64,
    pub cli_hash: Vec<u8>,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct ClLeaf {
    pub cli_leaf_idx: u64,
    pub cli_tree_key: Pubkey,
    pub cli_node_idx: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum UpdateVersion {
    Sequence(u64),
    WriteVersion(u64),
}

impl PartialOrd for UpdateVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (UpdateVersion::Sequence(x), UpdateVersion::Sequence(y)) => x.partial_cmp(y),
            (UpdateVersion::WriteVersion(x), UpdateVersion::WriteVersion(y)) => x.partial_cmp(y),
            // this is asset decompress case. Update with write version field is always most recent
            (UpdateVersion::Sequence(_), UpdateVersion::WriteVersion(_)) => Some(Ordering::Less),
            (UpdateVersion::WriteVersion(_), UpdateVersion::Sequence(_)) => None,
        }
    }
}

impl UpdateVersion {
    pub fn get_seq(&self) -> Option<u64> {
        match self {
            Self::Sequence(seq) => Some(*seq),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Updated<T> {
    pub slot_updated: u64,
    pub update_version: Option<UpdateVersion>,
    pub value: T,
}

impl<T> Updated<T> {
    pub fn new(slot_updated: u64, update_version: Option<UpdateVersion>, value: T) -> Self {
        Self {
            slot_updated,
            update_version,
            value,
        }
    }

    pub fn get_upd_ver_seq(&self) -> Option<u64> {
        if let Some(upd_ver) = &self.update_version {
            upd_ver.get_seq()
        } else {
            None
        }
    }
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct BufferedTransaction {
    pub transaction: Vec<u8>,
    // this flag tells if the transaction should be mapped from extrnode flatbuffer to mplx flatbuffer structure
    // data from geyser should be mapped and data from BG should not
    pub map_flatbuffer: bool,
}

#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct SignatureWithSlot {
    pub signature: Signature,
    pub slot: u64,
}

#[derive(Default)]
pub struct TreeState {
    pub tree: Pubkey,
    pub seq: u64,
    pub slot: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EditionData {
    pub key: Pubkey,
    pub supply: u64,
    pub max_supply: Option<u64>,
    pub edition_number: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MasterEdition {
    pub key: Pubkey,
    pub supply: u64,
    pub max_supply: Option<u64>,
    pub write_version: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EditionV1 {
    pub key: Pubkey,
    pub parent: Pubkey,
    pub edition: u64,
    pub write_version: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PubkeyWithSlot {
    pub pubkey: Pubkey,
    pub slot: u64,
}

#[derive(Default)]
pub struct ForkedItem {
    pub tree: Pubkey,
    pub seq: u64,
    pub node_idx: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetSignatureKey {
    pub tree: Pubkey,
    pub leaf_idx: u64,
    pub seq: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetSignature {
    pub tx: String,
    pub instruction: String,
    pub slot: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AssetSignatureWithPagination {
    pub asset_signatures: Vec<AssetSignature>,
    pub before: Option<u64>,
    pub after: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct TokenAccountOwnerIdxKey {
    pub owner: Pubkey,
    pub token_account: Pubkey,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct TokenAccountMintOwnerIdxKey {
    pub mint: Pubkey,
    pub owner: Pubkey,
    pub token_account: Pubkey,
}

#[derive(Debug, Clone)]
pub struct TokenAccountIterableIdx {
    pub mint: Option<Pubkey>,
    pub owner: Pubkey,
    pub token_account: Pubkey,
    pub is_zero_balance: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct ResponseTokenAccount {
    pub address: String,
    pub mint: String,
    pub owner: String,
    pub amount: u64,
    pub delegated_amount: u64,
    pub frozen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_extensions: Option<serde_json::Value>,
}

pub struct TokenAccResponse {
    pub token_acc: ResponseTokenAccount,
    pub sorting_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct Task {
    pub ofd_metadata_url: String,
    pub ofd_locked_until: Option<chrono::DateTime<chrono::Utc>>,
    pub ofd_attempts: i32,
    pub ofd_max_attempts: i32,
    pub ofd_error: Option<String>,
    pub ofd_status: TaskStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawBlock {
    pub slot: u64,
    pub block: solana_transaction_status::UiConfirmedBlock,
}

#[derive(Debug, Clone)]
pub struct JsonDownloadTask {
    pub metadata_url: String,
    pub status: TaskStatus,
    pub attempts: i16,
    pub max_attempts: i16,
}

#[derive(Debug, Clone)]
pub struct BatchMintWithState {
    pub file_name: String,
    pub state: BatchMintState,
    pub error: Option<String>,
    pub url: Option<String>,
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BatchMintToVerify {
    pub file_hash: String,
    pub url: String,
    pub created_at_slot: u64,
    pub signature: Signature,
    pub download_attempts: u8,
    pub persisting_state: PersistingBatchMintState,
    pub staker: Pubkey,
    pub collection_mint: Option<Pubkey>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FailedBatchMint {
    pub status: FailedBatchMintState,
    pub file_hash: String,
    pub url: String,
    pub created_at_slot: u64,
    pub signature: Signature,
    pub download_attempts: u8,
    pub staker: Pubkey,
}

impl Default for JsonDownloadTask {
    fn default() -> Self {
        Self {
            metadata_url: "".to_string(),
            status: TaskStatus::Pending,
            attempts: 1,
            max_attempts: 10,
        }
    }
}

pub struct CoreFee {
    pub pubkey: Pubkey,
    pub is_paid: bool,
    pub current_balance: u64,
    pub minimum_rent: u64,
    pub slot_updated: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(default)]
pub struct CoreFeesAccount {
    pub address: String,
    pub current_balance: i64,
    pub minimum_rent: i64,
}

#[derive(Debug, Clone)]
pub struct CoreFeesAccountWithSortingID {
    pub sorting_id: String,
    pub fees_account: CoreFeesAccount,
}

impl From<(&[u8], i64, CoreFeesAccount)> for CoreFeesAccountWithSortingID {
    fn from((pubkey, slot, value): (&[u8], i64, CoreFeesAccount)) -> Self {
        let mut key = slot.to_be_bytes().to_vec();
        key.extend_from_slice(pubkey);
        let sorting_id = general_purpose::STANDARD_NO_PAD.encode(key);

        Self {
            sorting_id,
            fees_account: value,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LeafSignatureAllData {
    pub tree: Pubkey,
    pub signature: Signature,
    pub leaf_idx: u64,
    pub slot_sequences: HashMap<u64, HashSet<u64>>,
}

#[derive(Clone)]
pub struct MetadataInfo {
    pub metadata: Metadata,
    pub slot_updated: u64,
    pub write_version: u64,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub executable: bool,
    pub metadata_owner: Option<String>,
}

#[derive(Clone)]
pub struct EditionMetadata {
    pub edition: TokenMetadataEdition,
    pub write_version: u64,
    pub slot_updated: u64,
}

#[derive(Clone, Debug)]
pub struct BurntMetadataSlot {
    pub slot_updated: u64,
    pub write_version: u64,
}

#[derive(Clone)]
pub struct IndexableAssetWithAccountInfo {
    pub indexable_asset: MplCoreAccountData,
    pub lamports: u64,
    pub executable: bool,
    pub slot_updated: u64,
    pub write_version: u64,
    pub rent_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenAccount {
    pub pubkey: Pubkey,
    pub mint: Pubkey,
    pub delegate: Option<Pubkey>,
    pub owner: Pubkey,
    pub extensions: Option<TokenAccountExtensions>,
    pub frozen: bool,
    pub delegated_amount: i64,
    pub slot_updated: i64,
    pub amount: i64,
    pub write_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mint {
    pub pubkey: Pubkey,
    pub slot_updated: i64,
    pub supply: i64,
    pub decimals: i32,
    pub mint_authority: Option<Pubkey>,
    pub freeze_authority: Option<Pubkey>,
    pub extensions: Option<MintAccountExtensions>,
    pub write_version: u64,
}

pub struct InscriptionInfo {
    pub inscription: Inscription,
    pub write_version: u64,
    pub slot_updated: u64,
}

#[derive(Clone)]
pub struct InscriptionDataInfo {
    pub inscription_data: Vec<u8>,
    pub write_version: u64,
    pub slot_updated: u64,
}

#[derive(Clone)]
pub struct CoreAssetFee {
    pub indexable_asset: MplCoreAccountData,
    pub data: Vec<u8>,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub slot_updated: u64,
    pub write_version: u64,
}

pub struct UnprocessedAccountMessage {
    pub account: UnprocessedAccount,
    pub key: Pubkey,
    pub id: String,
}

pub struct BufferedTxWithID {
    pub tx: BufferedTransaction,
    pub id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_with_status() {
        let url = "http://example.com".to_string();
        let url_with_status = UrlWithStatus {
            metadata_url: url,
            is_downloaded: false,
        };
        let metadata_id = url_with_status.get_metadata_id();
        assert_eq!(metadata_id.len(), 32);
        assert_eq!(
            hex::encode(metadata_id),
            "f0e6a6a97042a4f1f1c87f5f7d44315b2d852c2df5c7991cc66241bf7072d1c4"
        );
    }

    #[test]
    fn test_url_with_status_trimmed_on_untrimmed_data() {
        let url = "  http://example.com  ".to_string();
        let url_with_status = UrlWithStatus {
            metadata_url: url,
            is_downloaded: false,
        };
        let metadata_id = url_with_status.get_metadata_id();
        assert_eq!(metadata_id.len(), 32);
        assert_eq!(
            hex::encode(metadata_id),
            "f0e6a6a97042a4f1f1c87f5f7d44315b2d852c2df5c7991cc66241bf7072d1c4"
        );
    }

    #[test]
    fn test_new_url_with_status_trimmes_url() {
        let url = "  http://example.com  ".to_string();
        let url_with_status: UrlWithStatus = UrlWithStatus::new(&url, false);
        assert_eq!(url_with_status.metadata_url, "http://example.com");
    }
}
