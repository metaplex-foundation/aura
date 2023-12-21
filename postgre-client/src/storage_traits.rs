use crate::model::{AssetSortedIndex, AssetSorting, SearchAssetsFilter};
use entities::models::AssetIndex;
use async_trait::async_trait;
use mockall::automock;

#[automock]
#[async_trait]
pub trait AssetIndexStorage {
    async fn fetch_last_synced_id(&self) -> Result<Option<Vec<u8>>, String>;
    async fn update_asset_indexes_batch(
        &self,
        asset_indexes: &[AssetIndex],
        last_key: &[u8],
    ) -> Result<(), String>;
}

#[automock]
#[async_trait]
pub trait AssetPubkeyFilteredFetcher {
    async fn get_asset_pubkeys_filtered(
        &self,
        filter: &SearchAssetsFilter,
        order: &AssetSorting,
        limit: u64,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
    ) -> Result<Vec<AssetSortedIndex>, String>;
}
