use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{
    decode_failed_batch_mint_key, decode_string, encode_failed_batch_mint_key, encode_string,
};
use crate::{Result, Storage};
use bincode::deserialize;
use bubblegum_batch_sdk::model::BatchMint;
use entities::enums::{FailedBatchMintState, PersistingBatchMintState};
use entities::models::{BatchMintToVerify, FailedBatchMint};
use tracing::error;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FailedBatchMintKey {
    pub status: FailedBatchMintState,
    pub hash: String,
}

// queue
impl TypedColumn for BatchMintToVerify {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "BATCH_MINT_TO_VERIFY"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

pub fn merge_batch_mint_to_verify(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut slot = 0;
    if let Some(existing_val) = existing_val {
        match deserialize::<BatchMintToVerify>(existing_val) {
            Ok(value) => {
                slot = value.created_at_slot;
                result = existing_val.to_vec();
            }
            Err(e) => {
                error!("RocksDB: BatchMintToVerify deserialize existing_val: {}", e)
            }
        }
    }

    for op in operands {
        match deserialize::<BatchMintToVerify>(op) {
            Ok(new_val) => {
                if new_val.created_at_slot > slot {
                    slot = new_val.created_at_slot;
                    result = op.to_vec();
                }
            }
            Err(e) => {
                error!("RocksDB: BatchMintToVerify deserialize new_val: {}", e)
            }
        }
    }

    Some(result)
}

impl TypedColumn for FailedBatchMint {
    type KeyType = FailedBatchMintKey;
    type ValueType = Self;
    const NAME: &'static str = "FAILED_BATCH_MINT"; // Name of the column family

    fn encode_key(key: FailedBatchMintKey) -> Vec<u8> {
        encode_failed_batch_mint_key(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_failed_batch_mint_key(bytes)
    }
}

pub fn merge_failed_batch_mint(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut slot = 0;
    if let Some(existing_val) = existing_val {
        match deserialize::<FailedBatchMint>(existing_val) {
            Ok(value) => {
                slot = value.created_at_slot;
                result = existing_val.to_vec();
            }
            Err(e) => {
                error!("RocksDB: FailedBatchMint deserialize existing_val: {}", e)
            }
        }
    }

    for op in operands {
        match deserialize::<FailedBatchMint>(op) {
            Ok(new_val) => {
                if new_val.created_at_slot > slot {
                    slot = new_val.created_at_slot;
                    result = op.to_vec();
                }
            }
            Err(e) => {
                error!("RocksDB: FailedBatchMint deserialize new_val: {}", e)
            }
        }
    }

    Some(result)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BatchMintWithStaker {
    pub batch_mint: BatchMint,
    pub staker: Pubkey,
}

impl TypedColumn for BatchMintWithStaker {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "BATCH_MINTS"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

impl Storage {
    pub async fn fetch_batch_mint_for_verifying(
        &self,
    ) -> Result<(Option<BatchMintToVerify>, Option<BatchMint>)> {
        let first_value = self
            .batch_mint_to_verify
            .iter_start()
            .next()
            .transpose()
            .map_err(StorageError::RocksDb)?
            .map(|(_, value)| bincode::deserialize::<BatchMintToVerify>(value.as_ref()))
            .transpose()?;

        if let Some(batch_mint) = &first_value {
            let batch_mint = self.batch_mints.get(batch_mint.file_hash.clone())?;
            return Ok((first_value, batch_mint.map(|r| r.batch_mint)));
        }
        Ok((first_value, None))
    }

    pub async fn drop_batch_mint_from_queue(&self, file_hash: String) -> Result<()> {
        self.batch_mint_to_verify.delete(file_hash)
    }

    pub async fn save_batch_mint_as_failed(
        &self,
        status: FailedBatchMintState,
        batch_mint: &BatchMintToVerify,
    ) -> Result<()> {
        let key = FailedBatchMintKey {
            status: status.clone(),
            hash: batch_mint.file_hash.clone(),
        };
        let value = FailedBatchMint {
            status,
            file_hash: batch_mint.file_hash.clone(),
            url: batch_mint.url.clone(),
            created_at_slot: batch_mint.created_at_slot,
            signature: batch_mint.signature,
            download_attempts: batch_mint.download_attempts + 1,
            staker: batch_mint.staker,
        };
        self.failed_batch_mints.put_async(key, value).await
    }

    pub async fn inc_batch_mint_to_verify_download_attempts(
        &self,
        batch_mint_to_verify: &mut BatchMintToVerify,
    ) -> Result<()> {
        batch_mint_to_verify.download_attempts += 1;
        self.batch_mint_to_verify
            .put_async(
                batch_mint_to_verify.file_hash.clone(),
                BatchMintToVerify {
                    file_hash: batch_mint_to_verify.file_hash.clone(),
                    url: batch_mint_to_verify.url.clone(),
                    created_at_slot: batch_mint_to_verify.created_at_slot,
                    signature: batch_mint_to_verify.signature,
                    download_attempts: batch_mint_to_verify.download_attempts + 1,
                    persisting_state: PersistingBatchMintState::FailedToPersist,
                    staker: batch_mint_to_verify.staker,
                    collection_mint: batch_mint_to_verify.collection_mint,
                },
            )
            .await
    }
}
