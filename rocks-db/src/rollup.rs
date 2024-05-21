use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{decode_string, encode_string};
use crate::{Result, Storage};
use bincode::deserialize;
use entities::models::RollupToVerify;
use log::error;
use rocksdb::MergeOperands;

impl TypedColumn for RollupToVerify {
    type KeyType = String;
    type ValueType = Self; // The value type is the Asset struct itself
    const NAME: &'static str = "ROLLUP_TO_VERIFY"; // Name of the column family

    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

pub fn merge_rollup(
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

impl Storage {
    pub async fn fetch_rollup_for_verifying(&self) -> Result<Option<RollupToVerify>> {
        let mut iter = self.rollup_to_verify.iter_start();

        // TODO: change error
        let first_value = iter
            .next()
            .transpose()
            .map_err(|e| StorageError::Common(e.to_string()))?
            .map(|(_, value)| bincode::deserialize::<RollupToVerify>(value.as_ref()))
            .transpose()?;

        Ok(first_value)
    }

    pub async fn drop_rollup_from_queue(&self, file_hash: String) -> Result<()> {
        self.rollup_to_verify.delete(file_hash)
    }
}
