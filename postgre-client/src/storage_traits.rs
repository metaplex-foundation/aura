use std::sync::Arc;

use async_trait::async_trait;
use entities::{
    api_req_params::GetByMethodsOptions,
    enums::AssetType,
    models::{AssetIndex, FungibleAssetIndex},
};
use mockall::{automock, mock};

use crate::{
    error::IndexDbError,
    model::{AssetSortedIndex, AssetSorting, SearchAssetsFilter},
};

pub struct NFTSemaphores {
    pub assets: Arc<tokio::sync::Semaphore>,
    pub creators: Arc<tokio::sync::Semaphore>,
    pub authority: Arc<tokio::sync::Semaphore>,
    pub metadata: Arc<tokio::sync::Semaphore>,
}

impl NFTSemaphores {
    pub fn new() -> Self {
        Self {
            assets: Arc::new(tokio::sync::Semaphore::new(1)),
            creators: Arc::new(tokio::sync::Semaphore::new(1)),
            authority: Arc::new(tokio::sync::Semaphore::new(1)),
            metadata: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

impl Default for NFTSemaphores {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
pub trait AssetIndexStorage {
    async fn fetch_last_synced_id(
        &self,
        asset_type: AssetType,
    ) -> Result<Option<Vec<u8>>, IndexDbError>;
    async fn update_nft_asset_indexes_batch(
        &self,
        asset_indexes: &[AssetIndex],
    ) -> Result<(), IndexDbError>;
    async fn update_fungible_asset_indexes_batch(
        &self,
        asset_indexes: &[FungibleAssetIndex],
    ) -> Result<(), IndexDbError>;
    async fn update_last_synced_key(
        &self,
        last_key: &[u8],
        asset_type: AssetType,
    ) -> Result<(), IndexDbError>;

    async fn load_from_dump_nfts(
        &self,
        assets_file_name: &str,
        creators_file_name: &str,
        authority_file_name: &str,
        metadata_file_name: &str,
        semaphores: Arc<NFTSemaphores>,
    ) -> Result<(), IndexDbError>;
    async fn load_from_dump_fungibles(
        &self,
        fungible_tokens_path: &str,
        semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Result<(), IndexDbError>;
    async fn destructive_prep_to_batch_nft_load(&self) -> Result<(), IndexDbError>;
    async fn finalize_batch_nft_load(&self) -> Result<(), IndexDbError>;

    async fn destructive_prep_to_batch_fungible_load(&self) -> Result<(), IndexDbError>;
    async fn finalize_batch_fungible_load(&self) -> Result<(), IndexDbError>;
}

mock!(
    pub AssetIndexStorageMock {}
    #[async_trait]
    impl AssetIndexStorage for AssetIndexStorageMock {
        async fn fetch_last_synced_id(&self, asset_type: AssetType) -> Result<Option<Vec<u8>>, IndexDbError>;
        async fn update_nft_asset_indexes_batch(
            &self,
            asset_indexes: &[AssetIndex],
        ) -> Result<(), IndexDbError>;
        async fn update_fungible_asset_indexes_batch(
            &self,
            asset_indexes: &[FungibleAssetIndex],
        ) -> Result<(), IndexDbError>;
        async fn update_last_synced_key(&self, last_key: &[u8], assset_type: AssetType) -> Result<(), IndexDbError>;

        async fn load_from_dump_nfts(
            &self,
            assets_file_name: &str,
            creators_file_name: &str,
            authority_file_name: &str,
            metadata_file_name: &str,
            semaphores: Arc<NFTSemaphores>,
        ) -> Result<(), IndexDbError>;
        async fn load_from_dump_fungibles(
            &self,
            fungible_tokens_path: &str,
            semaphore: Arc<tokio::sync::Semaphore>,
        ) -> Result<(), IndexDbError>;
        async fn destructive_prep_to_batch_nft_load(&self) -> Result<(), IndexDbError>;
        async fn finalize_batch_nft_load(&self) -> Result<(), IndexDbError>;
        async fn destructive_prep_to_batch_fungible_load(&self) -> Result<(), IndexDbError>;
        async fn finalize_batch_fungible_load(&self) -> Result<(), IndexDbError>;
    }

    impl Clone for AssetIndexStorageMock {
        fn clone(&self) -> Self;
    }
);

#[automock]
#[async_trait]
pub trait AssetPubkeyFilteredFetcher {
    #[allow(clippy::too_many_arguments)]
    async fn get_asset_pubkeys_filtered(
        &self,
        filter: &SearchAssetsFilter,
        order: &AssetSorting,
        limit: u64,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
        options: &GetByMethodsOptions,
    ) -> Result<Vec<AssetSortedIndex>, IndexDbError>;
    async fn get_grand_total(
        &self,
        filter: &SearchAssetsFilter,
        options: &GetByMethodsOptions,
    ) -> Result<u32, IndexDbError>;
}

#[automock]
#[async_trait]
pub trait IntegrityVerificationKeysFetcher {
    async fn get_verification_required_owners_keys(&self) -> Result<Vec<String>, IndexDbError>;
    async fn get_verification_required_creators_keys(&self) -> Result<Vec<String>, IndexDbError>;
    async fn get_verification_required_authorities_keys(&self)
        -> Result<Vec<String>, IndexDbError>;
    async fn get_verification_required_groups_keys(&self) -> Result<Vec<String>, IndexDbError>;
    async fn get_verification_required_assets_keys(&self) -> Result<Vec<String>, IndexDbError>;
    async fn get_verification_required_assets_proof_keys(
        &self,
    ) -> Result<Vec<String>, IndexDbError>;
}
