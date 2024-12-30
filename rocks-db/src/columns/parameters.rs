use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};

use crate::{column::TypedColumn, errors::StorageError, Result, Storage};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Parameter {
    LastBackfilledSlot,
    LastFetchedSlot,
    TopSeenSlot,
}

pub struct ParameterColumn<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> TypedColumn for ParameterColumn<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
{
    type KeyType = Parameter;
    type ValueType = Option<T>;
    const NAME: &'static str = "PARAMETERS";

    fn encode_key(key: Parameter) -> Vec<u8> {
        serde_cbor::to_vec(&key).unwrap()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        serde_cbor::from_slice(&bytes)
            .map_err(|e| StorageError::Common(format!("Failed to decode key: {}", e)))
    }

    fn encode(v: &Self::ValueType) -> Result<Vec<u8>> {
        serde_cbor::to_vec(v)
            .map_err(|e| StorageError::Common(format!("Failed to encode value: {}", e)))
    }

    fn decode(bytes: &[u8]) -> Result<Self::ValueType> {
        serde_cbor::from_slice(bytes)
            .map_err(|e| StorageError::Common(format!("Failed to decode value: {}", e)))
    }
}

impl Storage {
    pub async fn get_parameter<T>(&self, parameter: Parameter) -> Result<Option<T>>
    where
        T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
    {
        let key = ParameterColumn::<T>::encode_key(parameter.clone());
        let db = self.db.clone();
        let prameter_cloned = parameter.clone();
        let serialized_data = tokio::task::spawn_blocking(move || {
            db.get_cf(&db.cf_handle(ParameterColumn::<T>::NAME).unwrap(), key)
                .map_err(|e| {
                    StorageError::Common(format!(
                        "Failed to get parameter {:?} from db, error: {}",
                        prameter_cloned, e
                    ))
                })
        })
        .await
        .map_err(|e| {
            StorageError::Common(format!(
                "Failed to get parameter {:?} from db, error: {}",
                parameter, e
            ))
        })??;
        serialized_data
            .map(|s| serde_cbor::from_slice(s.as_slice()))
            .transpose()
            .map_err(|e| StorageError::Common(format!("Failed to decode parameter: {}", e)))
    }

    pub async fn put_parameter<T>(&self, parameter: Parameter, value: T) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
    {
        let serialized_data = serde_cbor::to_vec(&value)
            .map_err(|e| StorageError::Common(format!("Failed to serialize parameter: {}", e)))?;
        let key = ParameterColumn::<T>::encode_key(parameter);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.put_cf(
                &db.cf_handle(ParameterColumn::<T>::NAME).unwrap(),
                key,
                serialized_data,
            )
            .map_err(|e| StorageError::Common(format!("Failed to put parameter: {}", e)))
        })
        .await
        .map_err(|e| StorageError::Common(format!("Failed to put parameter: {}", e)))??;
        Ok(())
    }

    pub async fn merge_top_parameter<T>(&self, parameter: Parameter, value: T) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
    {
        let serialized_data = serde_cbor::to_vec(&value)
            .map_err(|e| StorageError::Common(format!("Failed to serialize parameter: {}", e)))?;
        let key = ParameterColumn::<T>::encode_key(parameter);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.merge_cf(
                &db.cf_handle(ParameterColumn::<T>::NAME).unwrap(),
                key,
                serialized_data,
            )
            .map_err(|e| StorageError::Common(format!("Failed to merge top parameter: {}", e)))
        })
        .await
        .map_err(|e| StorageError::Common(format!("Failed to merge top parameter: {}", e)))??;
        Ok(())
    }

    pub fn merge_top_parameter_with_batch<T>(
        &self,
        batch: &mut rocksdb::WriteBatch,
        parameter: Parameter,
        value: T,
    ) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
    {
        let serialized_data = serde_cbor::to_vec(&value)
            .map_err(|e| StorageError::Common(format!("Failed to serialize parameter: {}", e)))?;
        let key = ParameterColumn::<T>::encode_key(parameter);
        batch.merge_cf(
            &self.db.cf_handle(ParameterColumn::<T>::NAME).unwrap(),
            key,
            serialized_data,
        );
        Ok(())
    }

    pub async fn delete_parameter<T>(&self, parameter: Parameter) -> Result<()>
    where
        T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync,
    {
        let key = ParameterColumn::<T>::encode_key(parameter);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.delete_cf(&db.cf_handle(ParameterColumn::<T>::NAME).unwrap(), key)
                .map_err(|e| StorageError::Common(format!("Failed to delete parameter: {}", e)))
        })
        .await
        .map_err(|e| StorageError::Common(format!("Failed to delete parameter: {}", e)))??;
        Ok(())
    }
}

pub fn merge_top_parameter(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Option<u64> = None;
    if let Some(existing_val) = existing_val {
        match serde_cbor::from_slice::<u64>(existing_val) {
            Ok(value) => {
                result = Some(value);
            }
            Err(e) => {
                tracing::error!("RocksDB: u64 parameter deserialize existing_val: {}", e)
            }
        }
    }

    for op in operands {
        match serde_cbor::from_slice::<u64>(op) {
            Ok(new_val) => {
                result = Some(if let Some(mut current_val) = result {
                    current_val = current_val.max(new_val);
                    current_val
                } else {
                    new_val
                });
            }
            Err(e) => {
                tracing::error!("RocksDB: u64 parameter deserialize new_val: {}", e)
            }
        }
    }

    result.and_then(|result| serde_cbor::to_vec(&result).ok())
}
