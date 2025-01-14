use entities::enums::TokenMetadataEdition;
use rocksdb::MergeOperands;
use solana_sdk::pubkey::Pubkey;
use tracing::error;

use crate::{
    column::TypedColumn,
    errors::StorageError,
    key_encoders::{decode_pubkey, encode_pubkey},
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
