use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{
    decode_failed_rollup_key, decode_string, encode_failed_rollup_key, encode_string,
};
use crate::{Result, Storage};
use bincode::deserialize;
use entities::enums::{FailedRollupState, PersistingRollupState};
use entities::models::{FailedRollup, RollupToVerify};
use entities::rollup::Rollup;
use log::error;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FailedRollupKey {
    pub status: FailedRollupState,
    pub hash: String,
}

// queue
impl TypedColumn for RollupToVerify {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "ROLLUP_TO_VERIFY"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

pub fn merge_rollup_to_verify(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut slot = 0;
    if let Some(existing_val) = existing_val {
        match deserialize::<RollupToVerify>(existing_val) {
            Ok(value) => {
                slot = value.created_at_slot;
                result = existing_val.to_vec();
            }
            Err(e) => {
                error!("RocksDB: RollupToVerify deserialize existing_val: {}", e)
            }
        }
    }

    for op in operands {
        match deserialize::<RollupToVerify>(op) {
            Ok(new_val) => {
                if new_val.created_at_slot > slot {
                    slot = new_val.created_at_slot;
                    result = op.to_vec();
                }
            }
            Err(e) => {
                error!("RocksDB: RollupToVerify deserialize new_val: {}", e)
            }
        }
    }

    Some(result)
}

impl TypedColumn for FailedRollup {
    type KeyType = FailedRollupKey;
    type ValueType = Self;
    const NAME: &'static str = "FAILED_ROLLUP"; // Name of the column family

    fn encode_key(key: FailedRollupKey) -> Vec<u8> {
        encode_failed_rollup_key(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_failed_rollup_key(bytes)
    }
}

pub fn merge_failed_rollup(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut slot = 0;
    if let Some(existing_val) = existing_val {
        match deserialize::<FailedRollup>(existing_val) {
            Ok(value) => {
                slot = value.created_at_slot;
                result = existing_val.to_vec();
            }
            Err(e) => {
                error!("RocksDB: FailedRollup deserialize existing_val: {}", e)
            }
        }
    }

    for op in operands {
        match deserialize::<FailedRollup>(op) {
            Ok(new_val) => {
                if new_val.created_at_slot > slot {
                    slot = new_val.created_at_slot;
                    result = op.to_vec();
                }
            }
            Err(e) => {
                error!("RocksDB: FailedRollup deserialize new_val: {}", e)
            }
        }
    }

    Some(result)
}

impl TypedColumn for Rollup {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "ROLLUPS"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

impl Storage {
    pub async fn fetch_rollup_for_verifying(
        &self,
    ) -> Result<(Option<RollupToVerify>, Option<Rollup>)> {
        let first_value = self
            .rollup_to_verify
            .iter_start()
            .next()
            .transpose()
            .map_err(StorageError::RocksDb)?
            .map(|(_, value)| bincode::deserialize::<RollupToVerify>(value.as_ref()))
            .transpose()?;

        if let Some(rollup) = &first_value {
            let rollup = self.rollups.get(rollup.file_hash.clone())?;
            return Ok((first_value, rollup));
        }
        Ok((first_value, None))
    }

    pub async fn drop_rollup_from_queue(&self, file_hash: String) -> Result<()> {
        self.rollup_to_verify.delete(file_hash)
    }

    pub async fn save_rollup_as_failed(
        &self,
        status: FailedRollupState,
        rollup: &RollupToVerify,
    ) -> Result<()> {
        let key = FailedRollupKey {
            status: status.clone(),
            hash: rollup.file_hash.clone(),
        };
        let value = FailedRollup {
            status,
            file_hash: rollup.file_hash.clone(),
            url: rollup.url.clone(),
            created_at_slot: rollup.created_at_slot,
            signature: rollup.signature,
            download_attempts: rollup.download_attempts + 1,
        };
        self.failed_rollups.put_async(key, value).await
    }

    pub async fn inc_rollup_to_verify_download_attempts(
        &self,
        rollup_to_verify: &mut RollupToVerify,
    ) -> Result<()> {
        rollup_to_verify.download_attempts += 1;
        self.rollup_to_verify
            .put_async(
                rollup_to_verify.file_hash.clone(),
                RollupToVerify {
                    file_hash: rollup_to_verify.file_hash.clone(),
                    url: rollup_to_verify.url.clone(),
                    created_at_slot: rollup_to_verify.created_at_slot,
                    signature: rollup_to_verify.signature,
                    download_attempts: rollup_to_verify.download_attempts + 1,
                    persisting_state: PersistingRollupState::FailedToPersist,
                },
            )
            .await
    }
}
