use std::{collections::HashMap, marker::PhantomData, sync::Arc, vec};

use bincode::{deserialize, serialize};
use log::error;
use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, MergeOperands, DB};
use serde::{de::DeserializeOwned, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::{Result, StorageError};
pub trait TypedColumn {
    type KeyType: Clone + Send;
    type ValueType: Serialize + DeserializeOwned + Send;

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
            .map_err(|e| StorageError::Common(e.to_string()))?;

        Ok(())
    }

    pub fn put(&self, key: C::KeyType, value: &C::ValueType) -> Result<()> {
        Self::put_sync(self.backend.clone(), key, *value)
    }

    fn put_sync(backend: Arc<DB>, key: C::KeyType, value: C::ValueType) -> Result<()> {
        let serialized_value = serialize(&value)?;

        backend.put_cf(
            &backend.cf_handle(C::NAME).unwrap(),
            C::encode_key(key),
            serialized_value,
        )?;
        Ok(())
    }

    pub fn merge(&self, key: C::KeyType, value: &C::ValueType) -> Result<()> {
        let serialized_value = serialize(value)?;

        self.backend
            .merge_cf(&self.handle(), C::encode_key(key), serialized_value)?;

        Ok(())
    }

    pub async fn put_batch(&self, values: HashMap<C::KeyType, C::ValueType>) -> Result<()> {
        let db = self.backend.clone();
        let values = values.clone();
        tokio::task::spawn_blocking(move || Self::put_batch_sync(db, values))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    fn put_batch_sync(backend: Arc<DB>, values: HashMap<C::KeyType, C::ValueType>) -> Result<()> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (k, v) in values.iter() {
            let serialized_value = serialize(v)?;
            batch.put_cf(
                &backend.cf_handle(C::NAME).unwrap(),
                C::encode_key(k.clone()),
                serialized_value,
            )
        }
        backend.write(batch)?;
        Ok(())
    }

    pub fn get(&self, key: C::KeyType) -> Result<Option<C::ValueType>> {
        let mut result = Ok(None);

        if let Some(serialized_value) = self.backend.get_cf(&self.handle(), C::encode_key(key))? {
            let value = deserialize(&serialized_value)?;

            result = Ok(Some(value))
        }
        result
    }

    fn batch_get_sync(
        backend: Arc<DB>,
        keys: Vec<C::KeyType>,
    ) -> Result<Vec<Option<C::ValueType>>> {
        backend
            .batched_multi_get_cf(
                &backend.cf_handle(C::NAME).unwrap(),
                &keys.into_iter().map(C::encode_key).collect::<Vec<_>>(),
                false,
            )
            .into_iter()
            .map(|res| {
                res.map_err(StorageError::from).and_then(|opt| {
                    opt.map(|pinned| {
                        deserialize::<C::ValueType>(pinned.as_ref()).map_err(StorageError::from)
                    })
                    .transpose()
                })
            })
            .collect()
    }

    pub async fn batch_get(&self, keys: Vec<C::KeyType>) -> Result<Vec<Option<C::ValueType>>> {
        let db = self.backend.clone();
        let keys = keys.clone();
        tokio::task::spawn_blocking(move || Self::batch_get_sync(db, keys))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub fn decode_key(&self, bytes: Vec<u8>) -> Result<C::KeyType> {
        C::decode_key(bytes)
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
    fn handle(&self) -> Arc<BoundColumnFamily> {
        self.backend.cf_handle(C::NAME).unwrap()
    }

    pub fn delete(&self, key: C::KeyType) -> Result<()> {
        self.backend.delete_cf(&self.handle(), C::encode_key(key))?;
        Ok(())
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
}

pub mod columns {
    use serde::{Deserialize, Serialize};
    use solana_sdk::pubkey::Pubkey;

    #[derive(Debug, Serialize, Deserialize)]
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
