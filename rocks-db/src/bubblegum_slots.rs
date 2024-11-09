use serde::{Deserialize, Serialize};

use crate::column::TypedColumn;
use crate::key_encoders::{decode_u64, encode_u64};
use crate::Result;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForceReingestableSlots {}

impl TypedColumn for ForceReingestableSlots {
    type KeyType = u64;
    type ValueType = Self;
    const NAME: &'static str = "FORCE_REINGESTABLE_SLOTS";

    fn encode_key(slot: u64) -> Vec<u8> {
        encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64(bytes)
    }
}
