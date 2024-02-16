use crate::column::TypedColumn;
use crate::key_encoders::{decode_pubkey, encode_pubkey};
use crate::Result;
use entities::models::{EditionV1, MasterEdition};
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TokenMetadataEdition {
    EditionV1(EditionV1),
    MasterEdition(MasterEdition),
}

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
}

impl TokenMetadataEdition {
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
                }
                Ok(TokenMetadataEdition::EditionV1(value)) => {
                    write_version = value.write_version;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!(
                        "RocksDB: TokenMetadataEdition deserialize existing_val: {}",
                        e
                    )
                }
            }
        }

        for op in operands {
            match serde_cbor::from_slice(op) {
                Ok(TokenMetadataEdition::MasterEdition(new_val)) => {
                    if new_val.write_version > write_version {
                        write_version = new_val.write_version;
                        result = op.to_vec();
                    }
                }
                Ok(TokenMetadataEdition::EditionV1(new_val)) => {
                    if new_val.write_version > write_version {
                        write_version = new_val.write_version;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: TokenMetadataEdition deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}
