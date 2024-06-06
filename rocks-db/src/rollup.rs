use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{
    decode_string, dencode_failed_rollup_key, encode_failed_rollup_key, encode_string,
};
use crate::{Result, Storage};
use bincode::deserialize;
use entities::models::{FailedRollup, FailedRollupKey, RollupToVerify};
use entities::rollup::Rollup;
use log::error;
use rocksdb::MergeOperands;

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
        dencode_failed_rollup_key(bytes)
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
}
