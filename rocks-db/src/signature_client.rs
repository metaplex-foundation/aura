use std::sync::Arc;

use rocksdb::DB;
use serde::{Deserialize, Serialize};
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::errors::StorageError;
use crate::{column::TypedColumn, Result, Storage};
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

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let pubkey_size = std::mem::size_of::<Pubkey>();
        let slot_size = std::mem::size_of::<u64>();
        let signature_size = std::mem::size_of::<Signature>();
        if bytes.len() != pubkey_size + slot_size + signature_size {
            return Err(crate::StorageError::InvalidKeyLength);
        }
        let pubkey = Pubkey::try_from(&bytes[..pubkey_size])?;
        let slot = u64::from_be_bytes(bytes[pubkey_size..pubkey_size + slot_size].try_into()?);
        let sig = Signature::new(&bytes[pubkey_size + slot_size..]);
        Ok((pubkey, slot, sig))
    }
}

impl Storage {
    pub async fn persist_signature(
        &self,
        program_id: Pubkey,
        signature: interface::solana_rpc::SignatureWithSlot,
    ) -> Result<()> {
        let program_id = program_id;
        let slot = signature.slot;
        let signature = signature.signature;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            Self::put::<SignatureIdx>(db, (program_id, slot, signature), &SignatureIdx {})
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub async fn first_persisted_signature_for(
        &self,
        program_id: Pubkey,
    ) -> Result<Option<interface::solana_rpc::SignatureWithSlot>> {
        let program_id = program_id;
        let db = self.db.clone();
        let key = (program_id, 0, Signature::default());
        let res =
            tokio::task::spawn_blocking(move || Self::first_key_after::<SignatureIdx>(db, key))
                .await
                .map_err(|e| StorageError::Common(e.to_string()))?;
        let res = res?;

        Ok(res.map(
            |(_, slot, signature)| interface::solana_rpc::SignatureWithSlot { signature, slot },
        ))
    }

    pub async fn drop_signatures_before(
        &self,
        program_id: Pubkey,
        signature: interface::solana_rpc::SignatureWithSlot,
    ) -> Result<()> {
        let db = self.db.clone();
        let program_id = program_id;
        let slot = signature.slot;
        let signature = signature.signature;
        let from = (program_id, 0, Signature::default());
        let to = (program_id, slot, signature);
        tokio::task::spawn_blocking(move || {
            Self::sync_drop_signatures_before::<SignatureIdx>(db, from, to)
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub async fn signature_exists(
        &self,
        program_id: Pubkey,
        signature: interface::solana_rpc::SignatureWithSlot,
    ) -> Result<bool> {
        let db = self.db.clone();
        let program_id = program_id;
        let slot = signature.slot;
        let signature = signature.signature;
        let key = (program_id, slot, signature);
        tokio::task::spawn_blocking(move || Self::sync_has_key::<SignatureIdx>(db, key))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub async fn missing_signatures(
        &self,
        program_id: Pubkey,
        signatures: Vec<interface::solana_rpc::SignatureWithSlot>,
    ) -> Result<Vec<interface::solana_rpc::SignatureWithSlot>> {
        let db = self.db.clone();
        let program_id = program_id;
        let keys = signatures
            .into_iter()
            .map(|s| (program_id, s.slot, s.signature))
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || Self::sync_missing_keys_of::<SignatureIdx>(db, keys))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
            .map(|keys| {
                keys.into_iter()
                    .map(
                        |(_, slot, signature)| interface::solana_rpc::SignatureWithSlot {
                            signature,
                            slot,
                        },
                    )
                    .collect::<Vec<_>>()
            })
    }

    fn sync_has_key<T>(db: Arc<DB>, key: T::KeyType) -> Result<bool>
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

    fn sync_missing_keys_of<T>(db: Arc<DB>, keys: Vec<T::KeyType>) -> Result<Vec<T::KeyType>>
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
    ) -> Result<()>
    where
        T: TypedColumn,
    {
        let from = T::encode_key(from_key);
        let to = T::encode_key(to_key);
        db.delete_range_cf(db.cf_handle(T::NAME).unwrap(), from, to)?;
        Ok(())
    }

    fn first_key_after<T>(db: Arc<DB>, key: T::KeyType) -> Result<Option<T::KeyType>>
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

    fn put<T>(backend: Arc<DB>, key: T::KeyType, value: &T::ValueType) -> Result<()>
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
