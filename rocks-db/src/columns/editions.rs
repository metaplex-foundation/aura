use entities::enums::TokenMetadataEdition;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

use crate::{
    column::TypedColumn,
    columns::asset::TokenMetadataEditionParentIndex,
    errors::StorageError,
    key_encoders::{decode_pubkey, decode_pubkey_u64, encode_pubkey, encode_pubkey_u64},
    Result,
};

impl TypedColumn for TokenMetadataEdition {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "TOKEN_METADATA_EDITION"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self::ValueType> {
        serde_cbor::from_slice(bytes).map_err(|e| StorageError::Common(e.to_string()))
    }

    fn encode(v: &Self::ValueType) -> Result<Vec<u8>> {
        serde_cbor::to_vec(&v).map_err(|e| StorageError::Common(e.to_string()))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct EditionIndexKey {
    pub pub_key: Pubkey,
    pub edition: u64,
}

impl TypedColumn for TokenMetadataEditionParentIndex {
    type KeyType = EditionIndexKey;
    type ValueType = Self;
    const NAME: &'static str = "TOKEN_METADATA_EDITION_PARENT_INDEX"; // Name of the column family

    fn encode_key(search_key: EditionIndexKey) -> Vec<u8> {
        encode_pubkey_u64(search_key.pub_key, search_key.edition)
    }

    fn decode_key(encoded_key: Vec<u8>) -> Result<Self::KeyType> {
        let (pub_key, edition) = decode_pubkey_u64(encoded_key)?;
        Ok(EditionIndexKey { pub_key, edition })
    }

    fn decode(bytes: &[u8]) -> Result<Self::ValueType> {
        serde_cbor::from_slice(bytes).map_err(|e| StorageError::Common(e.to_string()))
    }

    fn encode(v: &Self::ValueType) -> Result<Vec<u8>> {
        serde_cbor::to_vec(&v).map_err(|e| StorageError::Common(e.to_string()))
    }
}

pub fn merge_token_metadata_edition(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut write_version = 0;
    if let Some(existing_val) = existing_val {
        match serde_cbor::from_slice(existing_val) {
            Ok(TokenMetadataEdition::MasterEdition(value)) => {
                write_version = value.write_version;
                result = existing_val.to_vec();
            },
            Ok(TokenMetadataEdition::EditionV1(value)) => {
                write_version = value.write_version;
                result = existing_val.to_vec();
            },
            Err(e) => {
                error!("RocksDB: TokenMetadataEdition deserialize existing_val: {}", e)
            },
        }
    }

    for op in operands {
        match serde_cbor::from_slice(op) {
            Ok(TokenMetadataEdition::MasterEdition(new_val)) => {
                if new_val.write_version > write_version || result.is_empty() {
                    write_version = new_val.write_version;
                    result = op.to_vec();
                }
            },
            Ok(TokenMetadataEdition::EditionV1(new_val)) => {
                if new_val.write_version > write_version || result.is_empty() {
                    write_version = new_val.write_version;
                    result = op.to_vec();
                }
            },
            Err(e) => {
                error!("RocksDB: TokenMetadataEdition deserialize new_val: {}", e)
            },
        }
    }

    Some(result)
}

pub fn merge_token_metadata_parent_index_edition(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut write_version = 0;
    if let Some(existing_val) = existing_val {
        match serde_cbor::from_slice::<TokenMetadataEditionParentIndex>(existing_val) {
            Ok(value) => {
                write_version = value.write_version;
                result = existing_val.to_vec();
            },
            Err(e) => {
                error!("RocksDB: AssetCollection deserialize existing_val: {}", e)
            },
        }
    }

    for op in operands {
        match serde_cbor::from_slice::<TokenMetadataEditionParentIndex>(op) {
            Ok(new_val) => {
                if new_val.write_version > write_version || result.is_empty() {
                    write_version = new_val.write_version;
                    result = op.to_vec();
                }
            },
            Err(e) => {
                error!("RocksDB: TokenMetadataEdition deserialize new_val: {}", e)
            },
        }
    }

    Some(result)
}
