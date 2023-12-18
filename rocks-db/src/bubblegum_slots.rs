use crate::column::TypedColumn;
use crate::key_encoders::{decode_u64, encode_u64};
use crate::{Result, Storage};
use bincode::deserialize;
use log::error;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::bs58::encode;
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::events::ChangeLogEventV1;

pub const BUBBLEGUM_SLOTS_PREFIX: &str = "s";

#[derive(Serialize, Deserialize, Debug)]
pub struct BubblegumSlots {}

impl TypedColumn for BubblegumSlots {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "BUBBLEGUM_SLOTS";

    fn encode_key(slot: String) -> Vec<u8> {
        slot.into_bytes()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        Ok(String::from_utf8(bytes).unwrap())
    }
}

pub fn form_bubblegum_slots_key(slot: u64) -> String {
    format!("{}{}", BUBBLEGUM_SLOTS_PREFIX, slot)
}

pub fn bubblegum_slots_key_to_value(key: String) -> u64 {
    key[BUBBLEGUM_SLOTS_PREFIX.len()..].parse::<u64>().unwrap()
}
