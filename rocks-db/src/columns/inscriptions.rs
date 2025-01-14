use bincode::deserialize;
use entities::models::InscriptionInfo;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::error;

use crate::{
    column::TypedColumn,
    impl_merge_values,
    key_encoders::{decode_pubkey, encode_pubkey},
    Result,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Inscription {
    pub authority: Pubkey,
    pub root: Pubkey,
    pub content_type: String,
    pub encoding: String,
    pub inscription_data_account: Pubkey,
    pub order: u64,
    pub size: u32,
    pub validation_hash: Option<String>,
    pub write_version: u64,
}

impl From<&InscriptionInfo> for Inscription {
    fn from(value: &InscriptionInfo) -> Self {
        Self {
            authority: value.inscription.authority,
            root: value.inscription.root,
            content_type: value.inscription.media_type.convert_to_string(),
            encoding: value.inscription.encoding_type.convert_to_string(),
            inscription_data_account: value.inscription.inscription_data,
            order: value.inscription.order,
            size: value.inscription.size,
            validation_hash: value.inscription.validation_hash.clone(),
            write_version: value.write_version,
        }
    }
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
    pub pubkey: Pubkey,
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
