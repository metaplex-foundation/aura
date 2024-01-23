use serde::{Deserialize, Serialize};

use crate::column::TypedColumn;
use crate::Result;

pub const BUBBLEGUM_SLOTS_PREFIX: &str = "s";

#[derive(Serialize, Deserialize, Debug, Clone)]
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
