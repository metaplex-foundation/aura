use std::{collections::HashMap, marker::PhantomData, sync::Arc, vec};

use bincode::{deserialize, serialize};
use log::error;
use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, MergeOperands, DB};
use serde::{de::DeserializeOwned, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::{Result, StorageError};
pub trait TypedColumn {
    type KeyType: Sync + Clone + Send;
    type ValueType: Sync + Serialize + DeserializeOwned + Send;

    const NAME: &'static str;

    fn key_size() -> usize {
        std::mem::size_of::<Self::KeyType>()
    }

    fn encode_key(index: Self::KeyType) -> Vec<u8>;

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType>;
}

#[derive(Debug)]
pub struct Column<C>
where
    C: TypedColumn,
    <C as TypedColumn>::ValueType: 'static,
    <C as TypedColumn>::ValueType: Clone,
    <C as TypedColumn>::KeyType: 'static,
{
    pub backend: Arc<DB>,
    pub column: PhantomData<C>,
}

impl<C> Column<C>
where
    C: TypedColumn,
    <C as TypedColumn>::ValueType: 'static,
    <C as TypedColumn>::ValueType: Clone,
    <C as TypedColumn>::KeyType: 'static,
{
    pub async fn put_async(&self, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let backend = self.backend.clone();
        tokio::task::spawn_blocking(move || Self::put_sync(backend, key, value))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub async fn put_cbor_encoded(&self, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let backend = self.backend.clone();
        tokio::task::spawn_blocking(move || Self::put_cbor_encoded_sync(backend, key, value))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub fn put(&self, key: C::KeyType, value: C::ValueType) -> Result<()> {
        Self::put_sync(self.backend.clone(), key, value)
    }

    fn put_cbor_encoded_sync(backend: Arc<DB>, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let serialized_value =
            serde_cbor::to_vec(&value).map_err(|e| StorageError::Common(e.to_string()))?;
        Self::put_sync_raw(backend, key, serialized_value, C::NAME)
    }

    fn put_sync(backend: Arc<DB>, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let serialized_value = serialize(&value)?;
        Self::put_sync_raw(backend, key, serialized_value, C::NAME)
    }

    fn put_sync_raw(
        backend: Arc<DB>,
        key: C::KeyType,
        serialized_value: Vec<u8>,
        col_name: &str,
    ) -> Result<()> {
        backend.put_cf(
            &backend.cf_handle(col_name).unwrap(),
            C::encode_key(key),
            serialized_value,
        )?;
        Ok(())
    }

    pub async fn merge(&self, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let backend = self.backend.clone();
        tokio::task::spawn_blocking(move || Self::merge_sync(backend, key, value))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub fn merge_sync(backend: Arc<DB>, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let serialized_value = serialize(&value)?;

        backend.merge_cf(
            &backend.cf_handle(C::NAME).unwrap(),
            C::encode_key(key),
            serialized_value,
        )?;

        Ok(())
    }

    fn merge_with_batch_generic<F>(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: &C::ValueType,
        serialize_fn: F,
    ) -> Result<()>
    where
        F: Fn(&C::ValueType) -> Result<Vec<u8>>,
    {
        let serialized_value = serialize_fn(value)?;
        batch.merge_cf(&self.handle(), C::encode_key(key), serialized_value);
        Ok(())
    }

    pub(crate) fn merge_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: &C::ValueType,
    ) -> Result<()> {
        self.merge_with_batch_generic(batch, key, value, |v| {
            serialize(v).map_err(|e| StorageError::Common(e.to_string()))
        })
    }

    pub(crate) fn merge_with_batch_cbor(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: &C::ValueType,
    ) -> Result<()> {
        self.merge_with_batch_generic(batch, key, value, |v| {
            serde_cbor::to_vec(v).map_err(|e| StorageError::Common(e.to_string()))
        })
    }

    pub(crate) fn put_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: &C::ValueType,
    ) -> Result<()> {
        let serialized_value = serialize(value)?;

        batch.put_cf(&self.handle(), C::encode_key(key), serialized_value);

        Ok(())
    }

    async fn merge_batch_generic<F>(
        &self,
        values: HashMap<C::KeyType, C::ValueType>,
        serialize_fn: F,
    ) -> Result<()>
    where
        F: Fn(&C::ValueType) -> Result<Vec<u8>> + Copy + Send + 'static,
    {
        let db = self.backend.clone();
        let values = values.clone();
        tokio::task::spawn_blocking(move || {
            Self::merge_batch_sync_generic(db, values, serialize_fn)
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    fn merge_batch_sync_generic<F>(
        backend: Arc<DB>,
        values: HashMap<C::KeyType, C::ValueType>,
        serialize_fn: F,
    ) -> Result<()>
    where
        F: Fn(&C::ValueType) -> Result<Vec<u8>>,
    {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (k, v) in values.iter() {
            let serialized_value = serialize_fn(v)?;
            batch.merge_cf(
                &backend.cf_handle(C::NAME).unwrap(),
                C::encode_key(k.clone()),
                serialized_value,
            )
        }
        backend.write(batch)?;
        Ok(())
    }

    pub async fn merge_batch(&self, values: HashMap<C::KeyType, C::ValueType>) -> Result<()> {
        self.merge_batch_generic(values, |v| {
            serialize(v).map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
    }

    pub async fn merge_batch_cbor(&self, values: HashMap<C::KeyType, C::ValueType>) -> Result<()> {
        self.merge_batch_generic(values, |v| {
            serde_cbor::to_vec(v).map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
    }

    async fn put_batch_generic<F>(
        &self,
        values: HashMap<C::KeyType, C::ValueType>,
        serialize_fn: F,
    ) -> Result<()>
    where
        F: Fn(&C::ValueType) -> Result<Vec<u8>> + Copy + Send + 'static,
    {
        let db = self.backend.clone();
        let values = values.clone();
        tokio::task::spawn_blocking(move || Self::put_batch_sync_generic(db, values, serialize_fn))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    fn put_batch_sync_generic<F>(
        backend: Arc<DB>,
        values: HashMap<C::KeyType, C::ValueType>,
        serialize_fn: F,
    ) -> Result<()>
    where
        F: Fn(&C::ValueType) -> Result<Vec<u8>>,
    {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (k, v) in values.iter() {
            let serialized_value = serialize_fn(v)?;
            batch.put_cf(
                &backend.cf_handle(C::NAME).unwrap(),
                C::encode_key(k.clone()),
                serialized_value,
            )
        }
        backend.write(batch)?;
        Ok(())
    }

    pub async fn put_batch(&self, values: HashMap<C::KeyType, C::ValueType>) -> Result<()> {
        self.put_batch_generic(values, |v| {
            serialize(v).map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
    }

    pub async fn put_batch_cbor(&self, values: HashMap<C::KeyType, C::ValueType>) -> Result<()> {
        self.put_batch_generic(values, |v| {
            serde_cbor::to_vec(v).map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
    }

    pub async fn get_cbor_encoded(&self, key: C::KeyType) -> Result<Option<C::ValueType>> {
        let mut result = Ok(None);

        let backend = self.backend.clone();
        let res = tokio::task::spawn_blocking(move || Self::get_raw(backend, key))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))??;

        if let Some(serialized_value) = res {
            let value = serde_cbor::from_slice(&serialized_value)
                .map_err(|e| StorageError::Common(e.to_string()))?;

            result = Ok(Some(value))
        }
        result
    }

    fn get_raw(backend: Arc<DB>, key: C::KeyType) -> Result<Option<Vec<u8>>> {
        let r = backend.get_cf(&backend.cf_handle(C::NAME).unwrap(), C::encode_key(key))?;
        Ok(r)
    }

    pub fn get(&self, key: C::KeyType) -> Result<Option<C::ValueType>> {
        let mut result = Ok(None);

        if let Some(serialized_value) = self.backend.get_cf(&self.handle(), C::encode_key(key))? {
            let value = deserialize(&serialized_value)?;

            result = Ok(Some(value))
        }
        result
    }

    async fn batch_get_generic<F>(
        &self,
        keys: Vec<C::KeyType>,
        deserialize_fn: F,
    ) -> Result<Vec<Option<C::ValueType>>>
    where
        F: Fn(&[u8]) -> Result<C::ValueType> + Copy + Send + 'static,
    {
        let db = self.backend.clone();
        let keys = keys.clone();
        tokio::task::spawn_blocking(move || Self::batch_get_sync_generic(db, keys, deserialize_fn))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    // Відповідна синхронна функція
    fn batch_get_sync_generic<F>(
        backend: Arc<DB>,
        keys: Vec<C::KeyType>,
        deserialize_fn: F,
    ) -> Result<Vec<Option<C::ValueType>>>
    where
        F: Fn(&[u8]) -> Result<C::ValueType>,
    {
        backend
            .batched_multi_get_cf(
                &backend.cf_handle(C::NAME).unwrap(),
                &keys.into_iter().map(C::encode_key).collect::<Vec<_>>(),
                false,
            )
            .into_iter()
            .map(|res| {
                res.map_err(StorageError::from).and_then(|opt| {
                    opt.map(|pinned| deserialize_fn(pinned.as_ref()))
                        .transpose()
                })
            })
            .collect()
    }

    pub async fn batch_get(&self, keys: Vec<C::KeyType>) -> Result<Vec<Option<C::ValueType>>> {
        self.batch_get_generic(keys, |bytes| {
            deserialize::<C::ValueType>(bytes).map_err(StorageError::from)
        })
        .await
    }

    pub async fn batch_get_cbor(&self, keys: Vec<C::KeyType>) -> Result<Vec<Option<C::ValueType>>> {
        self.batch_get_generic(keys, |bytes| {
            serde_cbor::from_slice(bytes).map_err(|e| StorageError::Common(e.to_string()))
        })
        .await
    }

    pub fn decode_key(&self, bytes: Vec<u8>) -> Result<C::KeyType> {
        C::decode_key(bytes)
    }

    pub fn encode_key(index: C::KeyType) -> Vec<u8> {
        C::encode_key(index)
    }

    pub fn iter(&self, key: C::KeyType) -> DBIteratorWithThreadMode<'_, DB> {
        let index_iterator = self.backend.iterator_cf(
            &self.handle(),
            rocksdb::IteratorMode::From(&C::encode_key(key), rocksdb::Direction::Forward),
        );

        index_iterator
    }
    // Method to get an iterator starting from the beginning of the column
    pub fn iter_start(&self) -> DBIteratorWithThreadMode<'_, DB> {
        self.backend
            .iterator_cf(&self.handle(), rocksdb::IteratorMode::Start)
    }

    // Method to get an iterator starting from the end of the column
    pub fn iter_end(&self) -> DBIteratorWithThreadMode<'_, DB> {
        self.backend
            .iterator_cf(&self.handle(), rocksdb::IteratorMode::End)
    }

    #[inline]
    pub(crate) fn handle(&self) -> Arc<BoundColumnFamily> {
        self.backend.cf_handle(C::NAME).unwrap()
    }

    pub fn delete(&self, key: C::KeyType) -> Result<()> {
        self.backend.delete_cf(&self.handle(), C::encode_key(key))?;
        Ok(())
    }
    pub(crate) fn delete_with_batch(&self, batch: &mut rocksdb::WriteBatch, key: C::KeyType) {
        batch.delete_cf(&self.handle(), C::encode_key(key));
    }

    fn delete_batch_sync(backend: Arc<DB>, keys: Vec<C::KeyType>) -> Result<()> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for key in keys {
            batch.delete_cf(&backend.cf_handle(C::NAME).unwrap(), C::encode_key(key))
        }
        backend.write(batch)?;
        Ok(())
    }

    pub async fn delete_batch(&self, keys: Vec<C::KeyType>) -> Result<()> {
        let db = self.backend.clone();
        let keys = keys.clone();
        tokio::task::spawn_blocking(move || Self::delete_batch_sync(db, keys))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    fn delete_range_sync(backend: Arc<DB>, from: C::KeyType, to: C::KeyType) -> Result<()> {
        backend.delete_range_cf(
            &backend.cf_handle(C::NAME).unwrap(),
            C::encode_key(from),
            C::encode_key(to),
        )?;
        Ok(())
    }

    pub async fn delete_range(&self, from: C::KeyType, to: C::KeyType) -> Result<()> {
        let db = self.backend.clone();
        tokio::task::spawn_blocking(move || Self::delete_range_sync(db, from, to))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }
}

pub mod columns {
    use serde::{Deserialize, Serialize};
    use solana_sdk::pubkey::Pubkey;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TokenAccount {
        pub pubkey: Pubkey,
        pub mint: Pubkey,
        pub delegate: Option<Pubkey>,
        pub owner: Pubkey,
        pub frozen: bool,
        pub delegated_amount: i64,
        pub slot_updated: i64,
        pub amount: i64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Mint {
        pub pubkey: Pubkey,
        pub slot_updated: i64,
        pub supply: i64,
        pub decimals: i32,
        pub mint_authority: Option<Pubkey>,
        pub freeze_authority: Option<Pubkey>,
    }
}

impl TypedColumn for columns::TokenAccount {
    const NAME: &'static str = "TOKEN_ACCOUNTS";

    type KeyType = Pubkey;
    type ValueType = Self;

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        pubkey.to_bytes().to_vec()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let key = Pubkey::try_from(&bytes[0..32])?;
        Ok(key)
    }
}

impl columns::TokenAccount {
    pub fn merge_accounts(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        let mut slot = 0;
        if let Some(existing_val) = existing_val {
            match deserialize::<columns::TokenAccount>(existing_val) {
                Ok(value) => {
                    slot = value.slot_updated;
                    result = existing_val.to_vec();
                }
                Err(e) => {
                    error!("RocksDB: TokenAccount deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<columns::TokenAccount>(op) {
                Ok(new_val) => {
                    if new_val.slot_updated > slot {
                        slot = new_val.slot_updated;
                        result = op.to_vec();
                    }
                }
                Err(e) => {
                    error!("RocksDB: TokenAccount deserialize new_val: {}", e)
                }
            }
        }

        Some(result)
    }
}
