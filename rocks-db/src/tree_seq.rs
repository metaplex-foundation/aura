use crate::column::TypedColumn;
use crate::key_encoders;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeSeqIdx {
    pub slot: u64,
}

impl TypedColumn for TreeSeqIdx {
    const NAME: &'static str = "TREE_SEQ_IDX";

    type KeyType = (Pubkey, u64);
    type ValueType = Self;

    fn encode_key(key: (Pubkey, u64)) -> Vec<u8> {
        key_encoders::encode_pubkey_u64(key.0, key.1)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        key_encoders::decode_pubkey_u64(bytes)
    }
}
