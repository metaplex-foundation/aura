use std::sync::Arc;

use async_trait::async_trait;
use entities::models::SignatureWithSlot;
use interface::error::StorageError;
use interface::signature_persistence::SignaturePersistence;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::{column::TypedColumn, Storage};
use bincode::serialize;

#[derive(Serialize, Deserialize, Debug)]
pub struct SignatureIdx {}

impl TypedColumn for SignatureIdx {
    type KeyType = (Pubkey, u64, Signature);
    type ValueType = Self;

    const NAME: &'static str = "SIGNATURE_IDX";

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        // create a key that is a concatenation of the pubkey, slot and the signature allocating memory immediately
        let pubkey_size = std::mem::size_of::<Pubkey>();
        let slot_size = std::mem::size_of::<u64>();
        let (pubkey, slot, sig) = index;
        let signature_size = bincode::serialized_size(&sig).unwrap() as usize;
        let mut key = Vec::with_capacity(pubkey_size + slot_size + signature_size);
        key.extend_from_slice(&pubkey.to_bytes());
        key.extend_from_slice(&slot.to_be_bytes());
        let sig = bincode::serialize(&sig).unwrap();
        key.extend_from_slice(sig.as_slice());
        key
    }

    fn decode_key(bytes: Vec<u8>) -> crate::Result<Self::KeyType> {
        let pubkey_size = std::mem::size_of::<Pubkey>();
        let slot_size = std::mem::size_of::<u64>();
        let signature_size = std::mem::size_of::<Signature>();
        if bytes.len() != pubkey_size + slot_size + signature_size {
            return Err(crate::StorageError::InvalidKeyLength);
        }
        let pubkey = Pubkey::try_from(&bytes[..pubkey_size])?;
        let slot = u64::from_be_bytes(bytes[pubkey_size..pubkey_size + slot_size].try_into()?);
        let sig = Signature::try_from(&bytes[pubkey_size + slot_size..])?;
        Ok((pubkey, slot, sig))
    }
}

#[async_trait]
impl SignaturePersistence for Storage {
    async fn persist_signature(
        &self,
        program_id: Pubkey,
        signature: SignatureWithSlot,
    ) -> Result<(), StorageError> {
        let slot = signature.slot;
        let signature = signature.signature;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            Self::put::<SignatureIdx>(db, (program_id, slot, signature), &SignatureIdx {}).map_err(|e| {
                StorageError::Common(format!(
                    "Failed to persist signature for program_id: {}, slot: {}, signature: {}, error: {}",
                    program_id, slot, signature, e
                ))
            })
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    async fn first_persisted_signature_for(
        &self,
        program_id: Pubkey,
    ) -> Result<Option<SignatureWithSlot>, StorageError> {
        let db = self.db.clone();
        let key = (program_id, 0, Signature::default());
        let res = tokio::task::spawn_blocking(move || {
            Self::first_key_after::<SignatureIdx>(db, key).map_err(|e| {
                StorageError::Common(format!(
                    "Failed to get first persisted signature for program_id: {}, error: {}",
                    program_id, e
                ))
            })
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?;
        let res = res?;

        Ok(res.map(|(_, slot, signature)| SignatureWithSlot { signature, slot }))
    }

    async fn drop_signatures_before(
        &self,
        program_id: Pubkey,
        signature: SignatureWithSlot,
    ) -> Result<(), StorageError> {
        let db = self.db.clone();
        let slot = signature.slot;
        let signature = signature.signature;
        let from = (program_id, 0, Signature::default());
        let to = (program_id, slot, signature);
        tokio::task::spawn_blocking(move || {
            Self::sync_drop_signatures_before::<SignatureIdx>(db, from, to)
                .map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    async fn missing_signatures(
        &self,
        program_id: Pubkey,
        signatures: Vec<SignatureWithSlot>,
    ) -> Result<Vec<SignatureWithSlot>, StorageError> {
        let db = self.db.clone();
        let keys = signatures
            .into_iter()
            .map(|s| (program_id, s.slot, s.signature))
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            Self::sync_missing_keys_of::<SignatureIdx>(db, keys)
                .map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
        .map(|keys| {
            keys.into_iter()
                .map(|(_, slot, signature)| SignatureWithSlot { signature, slot })
                .collect::<Vec<_>>()
        })
    }
}

impl Storage {
    pub async fn signature_exists(
        &self,
        program_id: Pubkey,
        signature: SignatureWithSlot,
    ) -> Result<bool, StorageError> {
        let db = self.db.clone();
        let slot = signature.slot;
        let signature = signature.signature;
        let key = (program_id, slot, signature);
        tokio::task::spawn_blocking(move || {
            Self::sync_has_key::<SignatureIdx>(db, key)
                .map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    fn sync_has_key<T>(db: Arc<DB>, key: T::KeyType) -> crate::Result<bool>
    where
        T: TypedColumn,
    {
        let cf = db.cf_handle(T::NAME).unwrap();
        let encoded_key = T::encode_key(key);
        if !db.key_may_exist_cf(cf, &encoded_key) {
            return Ok(false);
        }
        let res = db.get_cf(cf, &encoded_key)?;
        Ok(res.is_some())
    }

    fn sync_missing_keys_of<T>(db: Arc<DB>, keys: Vec<T::KeyType>) -> crate::Result<Vec<T::KeyType>>
    where
        T: TypedColumn,
        T::KeyType: Clone + std::hash::Hash + Eq,
    {
        let mut missing_keys = Vec::new();
        for key in keys {
            if !Self::sync_has_key::<T>(db.clone(), key.clone())? {
                missing_keys.push(key);
            }
        }
        Ok(missing_keys)
        //todo: Perform batched get, for now, needs optimisation, sorry for the commented out code
    }

    fn sync_drop_signatures_before<T>(
        db: Arc<DB>,
        from_key: T::KeyType,
        to_key: T::KeyType,
    ) -> crate::Result<()>
    where
        T: TypedColumn,
    {
        let from = T::encode_key(from_key);
        let to = T::encode_key(to_key);
        db.delete_range_cf(db.cf_handle(T::NAME).unwrap(), from, to)?;
        Ok(())
    }

    fn first_key_after<T>(db: Arc<DB>, key: T::KeyType) -> crate::Result<Option<T::KeyType>>
    where
        T: TypedColumn,
    {
        let mut iter = db.iterator_cf(
            db.cf_handle(T::NAME).unwrap(),
            rocksdb::IteratorMode::From(T::encode_key(key).as_slice(), rocksdb::Direction::Forward),
        );

        match iter.next() {
            Some(result) => {
                let (key, _) = result?;
                let decoded = T::decode_key(key.to_vec())?;
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }

    fn put<T>(backend: Arc<DB>, key: T::KeyType, value: &T::ValueType) -> crate::Result<()>
    where
        T: TypedColumn,
    {
        let serialized_value = serialize(value)?;

        backend.put_cf(
            backend.cf_handle(T::NAME).unwrap(),
            T::encode_key(key),
            serialized_value,
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::signature::Signature;

    use super::*;

    #[test]
    fn test_encode_decode() {
        let slot = 12345u64;
        let pubkey = Pubkey::new_unique();
        let sig = Signature::new_unique();

        let encoded = SignatureIdx::encode_key((pubkey, slot, sig));
        let decoded = SignatureIdx::decode_key(encoded).unwrap();
        assert_eq!(decoded, (pubkey, slot, sig));
    }
}
