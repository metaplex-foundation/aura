use crate::column::TypedColumn;
use crate::key_encoders::{decode_string, encode_string};
use crate::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum ChainDataMutability {
    Immutable,
    Mutable,
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OffChainData {
    pub url: String,
    pub metadata: String,
}

impl TypedColumn for OffChainData {
    type KeyType = String;
    type ValueType = Self; // The value type is the Asset struct itself
    const NAME: &'static str = "OFFCHAIN_DATA"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}
