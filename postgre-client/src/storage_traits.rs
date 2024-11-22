use crate::error::IndexDbError;
use crate::model::{AssetSortedIndex, AssetSorting, SearchAssetsFilter};
use crate::temp_index_client::TempClient;
use async_trait::async_trait;
use entities::api_req_params::GetByMethodsOptions;
use entities::enums::AssetType;
use entities::models::{AssetIndex, FungibleAssetIndex};
use mockall::{automock, mock};

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
    async fn load_from_dump(
        &self,
        base_path: &std::path::Path,
        last_key: &[u8],
        asset_type: AssetType,
    ) -> Result<(), IndexDbError>;
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
        async fn load_from_dump(
            &self,
            base_path: &std::path::Path,
            last_key: &[u8],
            asset_type: AssetType,
        ) -> Result<(), IndexDbError>;
        async fn update_last_synced_key(&self, last_key: &[u8], assset_type: AssetType) -> Result<(), IndexDbError>;
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

#[async_trait]
pub trait TempClientProvider {
    async fn create_temp_client(&self) -> Result<TempClient, IndexDbError>;
}

mockall::mock!(
pub TempClientProviderMock {}
impl Clone for TempClientProviderMock {
    fn clone(&self) -> Self;
}
#[async_trait]
impl TempClientProvider for TempClientProviderMock {
    async fn create_temp_client(&self) -> Result<TempClient, IndexDbError>;
}
);
