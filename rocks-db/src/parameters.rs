use serde::{Deserialize, Serialize};

use crate::{column::TypedColumn, errors::StorageError, Result, Storage};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Parameter {
    LastBackfilledSlot,
    LastFetchedSlot,
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
}
