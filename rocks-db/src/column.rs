use std::fmt::Debug;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use bincode::{deserialize, serialize};

use metrics_utils::red::RequestErrorDurationMetrics;
use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, DB};
use serde::{de::DeserializeOwned, Serialize};

use crate::{Result, StorageError, ToFlatbuffersConverter, BATCH_GET_ACTION, ROCKS_COMPONENT};
pub trait TypedColumn {
    type KeyType: Sync + Clone + Send + Debug;
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
    pub red_metrics: Arc<RequestErrorDurationMetrics>,
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

    pub fn merge_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: &C::ValueType,
    ) -> Result<()> {
        self.merge_with_batch_generic(batch, key, value, |v| {
            serialize(v).map_err(|e| StorageError::Common(e.to_string()))
        })
    }

    pub fn merge_with_batch_flatbuffers(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: &C::ValueType,
    ) -> Result<()>
    where
        C::ValueType: for<'a> ToFlatbuffersConverter<'a>,
    {
        self.merge_with_batch_generic(batch, key, value, |v| {
            Ok(ToFlatbuffersConverter::convert_to_fb_bytes(v))
        })
    }

    pub(crate) fn merge_with_batch_raw(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: C::KeyType,
        value: Vec<u8>,
    ) -> Result<()> {
        batch.merge_cf(&self.handle(), C::encode_key(key), value);
        Ok(())
    }

    pub fn merge_with_batch_cbor(
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

    pub async fn put_batch_flatbuffers(
        &self,
        values: HashMap<C::KeyType, C::ValueType>,
    ) -> Result<()>
    where
        C::ValueType: for<'a> ToFlatbuffersConverter<'a>,
    {
        self.put_batch_generic(values, |v| {
            Ok(ToFlatbuffersConverter::convert_to_fb_bytes(v))
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
        let start_time = chrono::Utc::now();
        let db = self.backend.clone();
        let keys = keys.clone();
        match tokio::task::spawn_blocking(move || {
            Self::batch_get_sync_generic(db, keys, deserialize_fn)
        })
        .await
        {
            Ok(res) => {
                self.red_metrics.observe_request(
                    ROCKS_COMPONENT,
                    BATCH_GET_ACTION,
                    C::NAME,
                    start_time,
                );
                res
            }
            Err(e) => {
                self.red_metrics
                    .observe_error(ROCKS_COMPONENT, BATCH_GET_ACTION, C::NAME);
                Err(StorageError::Common(e.to_string()))
            }
        }
    }

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

    #[allow(clippy::type_complexity)]
    fn to_pairs_generic(
        &self,
        it: &mut dyn Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
        num: usize,
    ) -> Vec<(C::KeyType, C::ValueType)> {
        it.filter_map(|r| r.ok())
            .filter_map(|(key_bytes, val_bytes)| {
                let k_op = C::decode_key(key_bytes.to_vec()).ok();
                let v_op = deserialize::<C::ValueType>(&val_bytes).ok();
                k_op.zip(v_op)
            })
            .take(num)
            .collect::<Vec<_>>()
    }

    pub fn pairs_iterator<'a>(
        &self,
        it: impl Iterator<Item = std::result::Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a,
    ) -> impl Iterator<Item = (C::KeyType, C::ValueType)> + 'a {
        it.filter_map(|r| r.ok())
            .filter_map(|(key_bytes, val_bytes)| {
                let k_op = C::decode_key(key_bytes.to_vec()).ok();
                let v_op = deserialize::<C::ValueType>(&val_bytes).ok();
                k_op.zip(v_op)
            })
    }

    /// Fetches maximum given amount of records from the beginning of the column family.
    /// ## Args:
    /// * `num` - desired amount of records to fetch
    pub fn get_from_start(&self, num: usize) -> Vec<(C::KeyType, C::ValueType)> {
        self.to_pairs_generic(&mut self.iter_start(), num)
    }

    /// Fetches maximum given amount of records from the position that is right
    /// after the given key.
    /// ## Args:
    /// * `key` - the key, records should be fetched after
    /// * `num` - desired amount of records to fetch
    pub fn get_after(&self, key: C::KeyType, num: usize) -> Vec<(C::KeyType, C::ValueType)> {
        self.to_pairs_generic(&mut self.iter(key).skip(1), num)
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
    pub fn iter_reverse(&self, key: C::KeyType) -> DBIteratorWithThreadMode<'_, DB> {
        let index_iterator = self.backend.iterator_cf(
            &self.handle(),
            rocksdb::IteratorMode::From(&C::encode_key(key), rocksdb::Direction::Reverse),
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

    pub async fn has_key(&self, key: C::KeyType) -> Result<bool> {
        let db = self.backend.clone();
        tokio::task::spawn_blocking(move || Self::sync_has_key(db, key))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub(crate) fn sync_has_key(db: Arc<DB>, key: C::KeyType) -> crate::Result<bool> {
        let cf = &db.cf_handle(C::NAME).unwrap();
        let encoded_key = C::encode_key(key);
        if !db.key_may_exist_cf(cf, &encoded_key) {
            return Ok(false);
        }
        let res = db.get_cf(cf, &encoded_key)?;
        Ok(res.is_some())
    }
}
