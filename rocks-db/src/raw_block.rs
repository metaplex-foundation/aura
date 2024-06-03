use std::sync::Arc;

use crate::{column::TypedColumn, key_encoders, Storage};
use async_trait::async_trait;
use entities::models::RawBlock;
use interface::{
    error::BlockConsumeError,
    signature_persistence::{BlockConsumer, BlockProducer},
};
use log::error;

impl TypedColumn for RawBlock {
    type KeyType = u64;

    type ValueType = Self;
    const NAME: &'static str = "RAW_BLOCK_CBOR_ENCODED";

    fn encode_key(slot: u64) -> Vec<u8> {
        key_encoders::encode_u64(slot)
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        key_encoders::decode_u64(bytes)
    }
}

#[async_trait]
impl BlockConsumer for Storage {
    async fn consume_block(
        &self,
        slot: u64,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> Result<(), BlockConsumeError> {
        let raw_block = RawBlock { slot, block };
        self.raw_blocks_cbor
            .put_cbor_encoded(raw_block.slot, raw_block.clone())
            .await
            .map_err(|e| {
                error!(
                    "Failed to put raw block for slot: {}, error: {}",
                    raw_block.slot, e
                );
                BlockConsumeError::PersistenceErr(e.into())
            })
    }

    async fn already_processed_slot(&self, slot: u64) -> Result<bool, BlockConsumeError> {
        self.raw_blocks_cbor
            .get_cbor_encoded(slot)
            .await
            .map(|r| r.is_some())
            .map_err(|e| {
                tracing::error!("Failed to get raw block for slot: {}, error: {}", slot, e);
                BlockConsumeError::PersistenceErr(e.into())
            })
    }
}

#[async_trait]
impl BlockProducer for Storage {
    async fn get_block(
        &self,
        slot: u64,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<solana_transaction_status::UiConfirmedBlock, interface::error::StorageError> {
        let raw_block = self
            .raw_blocks_cbor
            .get_cbor_encoded(slot)
            .await
            .map_err(|e| interface::error::StorageError::Common(e.to_string()))?;
        if raw_block.is_none() {
            if let Some(backup_provider) = backup_provider {
                let none_bp: Option<Arc<Storage>> = None;
                let block = backup_provider.get_block(slot, none_bp).await?;
                tracing::info!("Got block from backup provider for slot: {}", slot);
                self.consume_block(slot, block.clone())
                    .await
                    .map_err(|_| interface::error::StorageError::NotFound)?;
                return Ok(block);
            }
        }
        raw_block
            .map(|b| b.block)
            .ok_or(interface::error::StorageError::NotFound)
    }
}
