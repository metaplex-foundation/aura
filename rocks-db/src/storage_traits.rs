use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use mockall::automock;
use solana_sdk::pubkey::Pubkey;

pub use crate::Result;
use crate::Storage;
use entities::models::{AssetIndex, FungibleAssetIndex};

#[derive(Clone, Debug, PartialEq)]
pub struct AssetUpdatedKey {
    pub seq: u64,
    pub slot: u64,
    pub pubkey: Pubkey,
}

impl AssetUpdatedKey {
    pub fn new(seq: u64, slot: u64, pubkey: Pubkey) -> Self {
        AssetUpdatedKey { seq, slot, pubkey }
    }
}

#[automock]
pub trait AssetUpdateIndexStorage {
    fn last_known_nft_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>>;
    fn last_known_fungible_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>>;

    #[allow(clippy::type_complexity)]
    fn fetch_nft_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)>;

    fn fetch_fungible_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)>;
}

#[automock]
#[async_trait]
pub trait AssetIndexReader {
    async fn get_fungible_assets_indexes(
        &self,
        keys: &[Pubkey],
    ) -> Result<HashMap<Pubkey, FungibleAssetIndex>>;

    async fn get_nft_asset_indexes<'a>(
        &self,
        keys: &[Pubkey],
        collection_authorities: Option<&'a HashMap<Pubkey, Pubkey>>,
    ) -> Result<HashMap<Pubkey, AssetIndex>>;
}

#[automock]
#[async_trait]
pub trait Dumper {
    async fn dump_nft_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<(), String>;

    async fn dump_fungible_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
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
    fn last_known_nft_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
        self.mock_update_index_storage
            .last_known_nft_asset_updated_key()
    }

    fn last_known_fungible_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
        self.mock_update_index_storage
            .last_known_fungible_asset_updated_key()
    }

    fn fetch_nft_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)> {
        self.mock_update_index_storage
            .fetch_nft_asset_updated_keys(from, up_to, limit, skip_keys)
    }

    fn fetch_fungible_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)> {
        self.mock_update_index_storage
            .fetch_fungible_asset_updated_keys(from, up_to, limit, skip_keys)
    }
}

#[async_trait]
impl AssetIndexReader for MockAssetIndexStorage {
    async fn get_fungible_assets_indexes(
        &self,
        keys: &[Pubkey],
    ) -> Result<HashMap<Pubkey, FungibleAssetIndex>> {
        self.mock_asset_index_reader
            .get_fungible_assets_indexes(keys)
            .await
    }

    async fn get_nft_asset_indexes<'a>(
        &self,
        keys: &[Pubkey],
        collection_authorities: Option<&'a HashMap<Pubkey, Pubkey>>,
    ) -> Result<HashMap<Pubkey, AssetIndex>> {
        self.mock_asset_index_reader
            .get_nft_asset_indexes(keys, collection_authorities)
            .await
    }
}

#[async_trait]
impl Dumper for MockAssetIndexStorage {
    async fn dump_nft_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<(), String> {
        self.mock_dumper
            .dump_nft_db(base_path, batch_size, rx, synchronizer_metrics)
            .await
    }

    async fn dump_fungible_db(
        &self,
        base_path: &std::path::Path,
        batch_size: usize,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<(), String> {
        self.mock_dumper
            .dump_fungible_db(base_path, batch_size, rx, synchronizer_metrics)
            .await
    }
}

impl AssetIndexStorage for MockAssetIndexStorage {}

impl AssetIndexStorage for Storage {}

#[automock]
pub trait AssetSlotStorage {
    fn last_saved_slot(&self) -> Result<Option<u64>>;
}
