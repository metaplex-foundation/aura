use std::{marker::PhantomData, sync::Arc, vec};

use bincode::{deserialize, serialize};
use log::error;
use rocksdb::{ColumnFamily, DBIteratorWithThreadMode, MergeOperands, DB};
use serde::{de::DeserializeOwned, Serialize};
use solana_sdk::pubkey::Pubkey;

use crate::{Result, StorageError};
pub trait TypedColumn {
    type KeyType;
    type ValueType: Serialize + DeserializeOwned;

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
{
    pub backend: Arc<DB>,
    pub column: PhantomData<C>,
}

impl<C> Column<C>
where
    C: TypedColumn,
{
    pub fn put(&self, key: C::KeyType, value: &C::ValueType) -> Result<()> {
        let serialized_value = serialize(value)?;

        self.backend
            .put_cf(self.handle(), C::encode_key(key), serialized_value)?;

        Ok(())
    }

    pub fn merge(&self, key: C::KeyType, value: &C::ValueType) -> Result<()> {
        let serialized_value = serialize(value)?;

        self.backend
            .merge_cf(self.handle(), C::encode_key(key), serialized_value)?;

        Ok(())
    }

    pub fn get(&self, key: C::KeyType) -> Result<Option<C::ValueType>> {
        let mut result = Ok(None);

        if let Some(serialized_value) = self.backend.get_cf(self.handle(), C::encode_key(key))? {
            let value = deserialize(&serialized_value)?;

            result = Ok(Some(value))
        }

        result
    }

    pub async fn batch_get(&self, keys: Vec<C::KeyType>) -> Result<Vec<Option<C::ValueType>>> {
        self.backend
            .batched_multi_get_cf(
                self.handle(),
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

    pub fn decode_key(&self, bytes: Vec<u8>) -> Result<C::KeyType> {
        C::decode_key(bytes)
    }

    pub fn iter(&self, key: C::KeyType) -> DBIteratorWithThreadMode<'_, DB> {
        let index_iterator = self.backend.iterator_cf(
            self.handle(),
            rocksdb::IteratorMode::From(&C::encode_key(key), rocksdb::Direction::Forward),
        );

        index_iterator
    }
    // Method to get an iterator starting from the beginning of the column
    pub fn iter_start(&self) -> DBIteratorWithThreadMode<'_, DB> {
        self.backend
            .iterator_cf(self.handle(), rocksdb::IteratorMode::Start)
    }

    // Method to get an iterator starting from the end of the column
    pub fn iter_end(&self) -> DBIteratorWithThreadMode<'_, DB> {
        self.backend
            .iterator_cf(self.handle(), rocksdb::IteratorMode::End)
    }

    #[inline]
    fn handle(&self) -> &ColumnFamily {
        self.backend.cf_handle(C::NAME).unwrap()
    }

    pub fn delete(&self, key: C::KeyType) -> Result<()> {
        self.backend.delete_cf(self.handle(), C::encode_key(key))?;
        Ok(())
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

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TokenAccountOwnerIdx {}

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TokenAccountMintIdx {}
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

impl TypedColumn for columns::TokenAccountOwnerIdx {
    const NAME: &'static str = "ACCOUNTS_TOKEN_OWNER_IDX";

    type KeyType = (Pubkey, Pubkey);
    type ValueType = Self;

    fn encode_key((owner, pubkey): (Pubkey, Pubkey)) -> Vec<u8> {
        let mut key = vec![0; 32 + 32]; // size_of Pubkey + size_of Pubkey
        key[0..32].clone_from_slice(&owner.as_ref()[0..32]);
        key[32..64].clone_from_slice(&pubkey.as_ref()[0..32]);

        key
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let owner = Pubkey::try_from(&bytes[0..32])?;
        let pubkey = Pubkey::try_from(&bytes[32..64])?;

        Ok((owner, pubkey))
    }
}

impl TypedColumn for columns::TokenAccountMintIdx {
    const NAME: &'static str = "ACCOUNTS_TOKEN_MINT_IDX";

    type KeyType = (Pubkey, Pubkey);
    type ValueType = Self;

    fn encode_key((owner, pubkey): (Pubkey, Pubkey)) -> Vec<u8> {
        let mut key = vec![0; 32 + 32]; // size_of Pubkey + size_of Pubkey
        key[0..32].clone_from_slice(&owner.as_ref()[0..32]);
        key[32..64].clone_from_slice(&pubkey.as_ref()[0..32]);

        key
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let owner = Pubkey::try_from(&bytes[0..32])?;
        let pubkey = Pubkey::try_from(&bytes[32..64])?;

        Ok((owner, pubkey))
    }
}
