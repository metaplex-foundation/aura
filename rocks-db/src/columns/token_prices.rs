use serde::{Deserialize, Serialize};

use crate::{
    column::TypedColumn,
    key_encoders::{decode_string, encode_string},
    Result,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TokenPrice {
    pub price: f64,
}

impl TypedColumn for TokenPrice {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "TOKEN_PRICES";

    fn encode_key(pubkey: String) -> Vec<u8> {
        encode_string(pubkey)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}
