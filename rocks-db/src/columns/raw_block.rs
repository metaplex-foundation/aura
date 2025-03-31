use std::sync::Arc;

use async_trait::async_trait;
use entities::models::{RawBlock, RawBlockDeprecated, RawBlockWithTransactions};
use interface::{
    error::StorageError as InterfaceStorageError, signature_persistence::BlockProducer,
};
use serde::{Deserialize, Serialize};

use crate::{column::TypedColumn, errors::StorageError, key_encoders, SlotStorage};

impl TypedColumn for RawBlockDeprecated {
    type KeyType = u64;

    type ValueType = Self;
    const NAME: &'static str = "RAW_BLOCK_CBOR_ENCODED";

    fn encode_key(slot: u64) -> Vec<u8> {
        key_encoders::encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        key_encoders::decode_u64(bytes)
    }

    fn decode(bytes: &[u8]) -> crate::Result<Self::ValueType> {
        serde_cbor::from_slice(bytes).map_err(|e| StorageError::Common(e.to_string()))
    }

    fn encode(v: &Self::ValueType) -> crate::Result<Vec<u8>> {
        serde_cbor::to_vec(&v).map_err(|e| StorageError::Common(e.to_string()))
    }
}

impl TypedColumn for RawBlock {
    type KeyType = u64;

    type ValueType = Self;
    const NAME: &'static str = "RAW_BLOCK";

    fn encode_key(slot: u64) -> Vec<u8> {
        key_encoders::encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        key_encoders::decode_u64(bytes)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MissedSlotsIdx;

impl TypedColumn for MissedSlotsIdx {
    type KeyType = (u64, u64);

    type ValueType = Self;
    const NAME: &'static str = "MISSED_SLOTS_IDX";

    fn encode_key((seq, slot): Self::KeyType) -> Vec<u8> {
        key_encoders::encode_u64x2((seq, slot)).to_vec()
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        key_encoders::decode_u64x2(&bytes)
    }

    fn encode(_v: &Self::ValueType) -> crate::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn decode(_bytes: &[u8]) -> crate::Result<Self::ValueType> {
        Ok(Self)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SlotConsistencyCheckpoint;

impl TypedColumn for SlotConsistencyCheckpoint {
    type KeyType = u64;

    type ValueType = Self;
    const NAME: &'static str = "SLOT_CONSISTENCY_CHECKPOINT";

    fn encode_key(slot: Self::KeyType) -> Vec<u8> {
        key_encoders::encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        key_encoders::decode_u64(bytes)
    }

    fn encode(_v: &Self::ValueType) -> crate::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn decode(_bytes: &[u8]) -> crate::Result<Self::ValueType> {
        Ok(Self)
    }
}

#[async_trait]
impl BlockProducer for SlotStorage {
    async fn get_block(
        &self,
        slot: u64,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<RawBlockWithTransactions, InterfaceStorageError> {
        let raw_block = self
            .raw_blocks
            .get_async(slot)
            .await
            .map_err(|e| InterfaceStorageError::Common(e.to_string()))?;
        if raw_block.is_none() {
            if let Some(backup_provider) = backup_provider {
                let none_bp: Option<Arc<SlotStorage>> = None;
                let block = backup_provider.get_block(slot, none_bp).await?;
                tracing::info!("Got block from backup provider for slot: {}", slot);
                return Ok(block);
            }
        }
        raw_block.map(|b| b.block).ok_or({
            let err_msg = format!("Cannot get raw block with slot: '{slot}'!");
            InterfaceStorageError::NotFound(err_msg)
        })
    }
}
