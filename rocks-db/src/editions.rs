use crate::column::TypedColumn;
use crate::key_encoders::{decode_pubkey, encode_pubkey};
use crate::Result;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TokenMetadataEdition {
    EditionV1(EditionV1),
    MasterEdition(MasterEdition),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MasterEdition {
    pub supply: u64,
    pub max_supply: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EditionV1 {
    pub parent: Pubkey,
    pub edition: u64,
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
