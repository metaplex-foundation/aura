use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{decode_string, encode_string};
use crate::{Result, Storage};
use entities::models::RollupToVerify;

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
}
