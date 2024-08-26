use crate::column::TypedColumn;
use crate::key_encoders::{decode_pubkey, encode_pubkey};
use crate::{impl_merge_values, Result};
use bincode::deserialize;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Inscription {
    pub authority: Pubkey,
    pub root: Pubkey,
    pub media_type: String,
    pub encoding_type: String,
    pub inscription_data: Pubkey,
    pub order: u64,
    pub size: u32,
    pub validation_hash: Option<String>,
    pub write_version: u64,
}

impl TypedColumn for Inscription {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "INSCRIPTION"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}
impl_merge_values!(Inscription);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InscriptionData {
    pub data: Vec<u8>,
    pub write_version: u64,
}

impl TypedColumn for InscriptionData {
    type KeyType = Pubkey;
    type ValueType = Self;
    // The value type is the Asset struct itself
    const NAME: &'static str = "INSCRIPTION_DATA"; // Name of the column family

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        encode_pubkey(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_pubkey(bytes)
    }
}
impl_merge_values!(InscriptionData);
