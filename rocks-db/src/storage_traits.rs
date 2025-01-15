use std::{collections::HashSet, fs::File};

use async_trait::async_trait;
use entities::models::{AssetIndex, FungibleAssetIndex};
use mockall::automock;
use solana_sdk::pubkey::Pubkey;

pub use crate::Result;
use crate::Storage;

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
    async fn get_fungible_assets_indexes(&self, keys: &[Pubkey])
        -> Result<Vec<FungibleAssetIndex>>;

    async fn get_nft_asset_indexes<'a>(&self, keys: &[Pubkey]) -> Result<Vec<AssetIndex>>;
}

#[automock]
pub trait Dumper {
    #[allow(clippy::too_many_arguments)]
    fn dump_nft_csv(
        &self,
        assets_file: File,
        creators_file: File,
        authority_file: File,
        metadata_file: File,
        buf_capacity: usize,
        asset_limit: Option<usize>,
        start_pubkey: Option<Pubkey>,
        end_pubkey: Option<Pubkey>,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<usize, String>;

    fn dump_fungible_csv(
        &self,
        fungible_tokens_file_and_path: (File, String),
        buf_capacity: usize,
        start_pubkey: Option<Pubkey>,
        end_pubkey: Option<Pubkey>,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<usize, String>;
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
        self.mock_update_index_storage.last_known_nft_asset_updated_key()
    }

    fn last_known_fungible_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
        self.mock_update_index_storage.last_known_fungible_asset_updated_key()
    }

    fn fetch_nft_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)> {
        self.mock_update_index_storage.fetch_nft_asset_updated_keys(from, up_to, limit, skip_keys)
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
    ) -> Result<Vec<FungibleAssetIndex>> {
        self.mock_asset_index_reader.get_fungible_assets_indexes(keys).await
    }

    async fn get_nft_asset_indexes<'a>(&self, keys: &[Pubkey]) -> Result<Vec<AssetIndex>> {
        self.mock_asset_index_reader.get_nft_asset_indexes(keys).await
    }
}

impl Dumper for MockAssetIndexStorage {
    fn dump_nft_csv(
        &self,
        assets_file: File,
        creators_file: File,
        authority_file: File,
        metadata_file: File,
        buf_capacity: usize,
        asset_limit: Option<usize>,
        start_pubkey: Option<Pubkey>,
        end_pubkey: Option<Pubkey>,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<usize, String> {
        self.mock_dumper.dump_nft_csv(
            assets_file,
            creators_file,
            authority_file,
            metadata_file,
            buf_capacity,
            asset_limit,
            start_pubkey,
            end_pubkey,
            rx,
            synchronizer_metrics,
        )
    }
    fn dump_fungible_csv(
        &self,
        fungible_tokens_file_and_path: (File, String),
        buf_capacity: usize,
        start_pubkey: Option<Pubkey>,
        end_pubkey: Option<Pubkey>,
        rx: &tokio::sync::broadcast::Receiver<()>,
        synchronizer_metrics: std::sync::Arc<metrics_utils::SynchronizerMetricsConfig>,
    ) -> core::result::Result<usize, String> {
        self.mock_dumper.dump_fungible_csv(
            fungible_tokens_file_and_path,
            buf_capacity,
            start_pubkey,
            end_pubkey,
            rx,
            synchronizer_metrics,
        )
    }
}

impl AssetIndexStorage for MockAssetIndexStorage {}

impl AssetIndexStorage for Storage {}

#[automock]
pub trait AssetSlotStorage {
    fn last_saved_slot(&self) -> Result<Option<u64>>;
}
