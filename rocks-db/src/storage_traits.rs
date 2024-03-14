use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use mockall::automock;
use solana_sdk::pubkey::Pubkey;

pub use crate::Result;
use crate::Storage;
use entities::models::AssetIndex;

#[automock]
pub trait AssetUpdateIndexStorage {
    fn last_known_asset_updated_key(&self) -> Result<Option<(u64, u64, Pubkey)>>;
    #[allow(clippy::type_complexity)]
    fn fetch_asset_updated_keys(
        &self,
        from: Option<(u64, u64, Pubkey)>,
        up_to: Option<(u64, u64, Pubkey)>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<(u64, u64, Pubkey)>)>;
}

#[automock]
#[async_trait]
pub trait AssetIndexReader {
    async fn get_asset_indexes(&self, keys: &[Pubkey]) -> Result<HashMap<Pubkey, AssetIndex>>;
}

#[automock]
#[async_trait]
pub trait Dumper {
    async fn dump_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> core::result::Result<(), String>;
}

pub trait AssetIndexStorage: AssetIndexReader + AssetUpdateIndexStorage + Dumper {}

#[derive(Default)]
pub struct MockAssetIndexStorage {
    pub mock_update_index_storage: MockAssetUpdateIndexStorage,
    pub mock_asset_index_reader: MockAssetIndexReader,
    pub mock_dumper: MockDumper,
}

impl MockAssetIndexStorage {
    pub fn new() -> Self {
        MockAssetIndexStorage {
            mock_update_index_storage: MockAssetUpdateIndexStorage::new(),
            mock_asset_index_reader: MockAssetIndexReader::new(),
            mock_dumper: MockDumper::new(),
        }
    }
}

impl AssetUpdateIndexStorage for MockAssetIndexStorage {
    fn last_known_asset_updated_key(&self) -> Result<Option<(u64, u64, Pubkey)>> {
        self.mock_update_index_storage
            .last_known_asset_updated_key()
    }

    fn fetch_asset_updated_keys(
        &self,
        from: Option<(u64, u64, Pubkey)>,
        up_to: Option<(u64, u64, Pubkey)>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<(u64, u64, Pubkey)>)> {
        self.mock_update_index_storage
            .fetch_asset_updated_keys(from, up_to, limit, skip_keys)
    }
}

#[async_trait]
impl AssetIndexReader for MockAssetIndexStorage {
    async fn get_asset_indexes(&self, keys: &[Pubkey]) -> Result<HashMap<Pubkey, AssetIndex>> {
        self.mock_asset_index_reader.get_asset_indexes(keys).await
    }
}

#[async_trait]
impl Dumper for MockAssetIndexStorage {
    async fn dump_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
    ) -> core::result::Result<(), String> {
        self.mock_dumper.dump_db(base_path, batch_size, rx).await
    }
}

impl AssetIndexStorage for MockAssetIndexStorage {}

impl AssetIndexStorage for Storage {}

#[automock]
pub trait AssetSlotStorage {
    fn last_saved_slot(&self) -> Result<Option<u64>>;
}
