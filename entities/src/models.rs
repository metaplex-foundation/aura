use crate::enums::{
    ChainMutability, OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
    TokenStandard, UseMethod,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Eq, Hash)]
pub struct UrlWithStatus {
    pub metadata_url: String,
    pub is_downloaded: bool,
}

impl UrlWithStatus {
    pub fn get_metadata_id(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        let url = self.metadata_url.trim();
        hasher.update(url);
        hasher.finalize().to_vec()
    }
}

// AssetIndex is the struct that is stored in the postgres database and is used to query the asset pubkeys.
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
    pub slot_updated: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Creator {
    pub creator: Pubkey,
    pub creator_verified: bool,
    // In percentages, NOT basis points
    pub creator_share: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CompleteAssetDetails {
    // From AssetStaticDetails
    pub pubkey: Pubkey,
    pub specification_asset_class: SpecificationAssetClass,
    pub royalty_target_type: RoyaltyTargetType,
    pub slot_created: u64,

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

    // From AssetAuthority as Tuple
    pub authority: Updated<Pubkey>,

    // From AssetOwner as Tuples
    pub owner: Updated<Pubkey>,
    pub delegate: Updated<Option<Pubkey>>,
    pub owner_type: Updated<OwnerType>,
    pub owner_delegate_seq: Updated<Option<u64>>,

    // Separate fields
    pub asset_leaf: Option<Updated<AssetLeaf>>,
    pub collection: Option<Updated<AssetCollection>>,

    // Cl elements
    pub cl_leaf: Option<ClLeaf>,
    pub cl_items: Vec<ClItem>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_mutability: Option<ChainMutability>, // TODO: move this feild to AssetDynamicDetails struct
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lamports: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_owner: Option<String>,
}

impl ChainDataV1 {
    pub fn sanitize(&mut self) {
        self.name = self.name.trim().replace('\0', "");
        self.symbol = self.symbol.trim().replace('\0', "");
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AssetCollection {
    pub collection: Pubkey,
    pub is_collection_verified: bool,
    pub collection_seq: Option<u64>,
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

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub struct Updated<T> {
    pub slot_updated: u64,
    pub seq: Option<u64>,
    pub value: T,
}

impl<T> Updated<T> {
    pub fn new(slot_updated: u64, seq: Option<u64>, value: T) -> Self {
        Self {
            slot_updated,
            seq,
            value,
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

#[derive(Debug, Clone, PartialEq, Default)]
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

#[cfg(test)]
mod tests{
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
        assert_eq!(hex::encode(metadata_id),"f0e6a6a97042a4f1f1c87f5f7d44315b2d852c2df5c7991cc66241bf7072d1c4");
    }

    #[test]
    fn test_url_with_status_trimmed() {
        let url = "  http://example.com  ".to_string();
        let url_with_status = UrlWithStatus {
            metadata_url: url,
            is_downloaded: false,
        };
        let metadata_id = url_with_status.get_metadata_id();
        assert_eq!(metadata_id.len(), 32);
        assert_eq!(hex::encode(metadata_id),"f0e6a6a97042a4f1f1c87f5f7d44315b2d852c2df5c7991cc66241bf7072d1c4");
    }
}