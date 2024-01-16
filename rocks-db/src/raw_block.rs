use crate::{column::TypedColumn, key_encoders, Storage};
use async_trait::async_trait;
use interface::signature_persistence::BlockConsumer;
use log::error;
use serde::{Deserialize, Serialize};
use solana_transaction_status::{BlockEncodingOptions, TransactionDetails};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RawBlock {
    pub slot: u64,
    pub block: solana_transaction_status::UiConfirmedBlock,
}

impl TypedColumn for RawBlock {
    const NAME: &'static str = "RAW_BLOCK";

    type KeyType = u64;
    type ValueType = Self;

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
        block: solana_transaction_status::ConfirmedBlock,
    ) -> Result<(), String> {
        let slot = block.parent_slot;
        let encoded = block
            .encode_with_options(
                solana_transaction_status::UiTransactionEncoding::Base64,
                BlockEncodingOptions {
                    transaction_details: TransactionDetails::Full,
                    show_rewards: true,
                    max_supported_transaction_version: Some(u8::MAX),
                },
            )
            .map_err(|e| e.to_string())?;
        let raw_block = RawBlock {
            slot,
            block: encoded,
        };
        let res = self.raw_blocks.put_async(raw_block.slot, raw_block.clone()).await.map_err(|e|e.to_string());
        if let Err(e) = res {
            error!(
                "Failed to put raw block for slot: {}, error: {}",
                raw_block.slot, e
            );
            return Err(e);
        }
        Ok(())
    }
}
