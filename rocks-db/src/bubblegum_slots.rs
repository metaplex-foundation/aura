use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::deserialize;
use interface::slots_dumper::SlotGetter;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::column::TypedColumn;
use crate::key_encoders::{decode_u64, encode_u64};
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestableSlots {}

impl TypedColumn for IngestableSlots {
    type KeyType = u64;
    type ValueType = Self;
    const NAME: &'static str = "INGESTABLE_SLOTS";

    fn encode_key(slot: u64) -> Vec<u8> {
        encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64(bytes)
    }
}

pub struct BubblegumSlotGetter {
    rocks_client: Arc<crate::Storage>,
}

impl BubblegumSlotGetter {
    pub fn new(rocks_client: Arc<crate::Storage>) -> BubblegumSlotGetter {
        BubblegumSlotGetter { rocks_client }
    }
}

#[async_trait]
impl SlotGetter for BubblegumSlotGetter {
    fn get_unprocessed_slots_iter(&self) -> impl Iterator<Item = u64> {
        self.rocks_client
            .bubblegum_slots
            .iter_start()
            .filter_map(|k| k.ok())
            .map(|(k, _)| String::from_utf8(k.to_vec()))
            .filter_map(|k| k.ok())
            .map(bubblegum_slots_key_to_value)
    }

    async fn mark_slots_processed(&self, slots: Vec<u64>) -> core::result::Result<(), String> {
        self.rocks_client
            .ingestable_slots
            .put_batch(slots.iter().fold(HashMap::new(), |mut acc, slot| {
                acc.insert(*slot, IngestableSlots {});
                acc
            }))
            .await
            .map_err(|e| e.to_string())?;
        self.rocks_client
            .bubblegum_slots
            .delete_batch(slots.iter().map(|k| form_bubblegum_slots_key(*k)).collect())
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}

pub struct IngestableSlotGetter {
    rocks_client: Arc<crate::Storage>,
}

impl IngestableSlotGetter {
    pub fn new(rocks_client: Arc<crate::Storage>) -> IngestableSlotGetter {
        IngestableSlotGetter { rocks_client }
    }
}

#[async_trait]
impl SlotGetter for IngestableSlotGetter {
    fn get_unprocessed_slots_iter(&self) -> impl Iterator<Item = u64> {
        self.rocks_client
            .ingestable_slots
            .iter_start()
            .filter_map(|k| k.ok())
            .map(|(k, _)| IngestableSlots::decode_key(k.to_vec()))
            .filter_map(|k| k.ok())
    }

    async fn mark_slots_processed(&self, slots: Vec<u64>) -> core::result::Result<(), String> {
        self.rocks_client
            .ingestable_slots
            .delete_batch(slots.clone())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerForceReingestableSlots {
    pub slot: u64,
    pub processed: bool,
}

impl TypedColumn for PeerForceReingestableSlots {
    type KeyType = u64;
    type ValueType = Self;
    const NAME: &'static str = "PEER_FORCE_REINGESTABLE_SLOTS";

    fn encode_key(slot: u64) -> Vec<u8> {
        encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_u64(bytes)
    }
}

impl PeerForceReingestableSlots {
    pub fn merge_peer_force_reingestable_slots(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        if let Some(existing_val) = existing_val {
            match deserialize::<Self>(existing_val) {
                Ok(_) => {
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!(
                        "RocksDB: PeerForceReingestableSlots deserialize existing_val: {}",
                        e
                    )
                }
            }
        }

        for op in operands {
            match deserialize::<Self>(op) {
                Ok(new_val) => {
                    if new_val.processed {
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!(
                        "RocksDB: PeerForceReingestableSlots deserialize new_val: {}",
                        e
                    )
                }
            }
        }

        Some(result)
    }
}
