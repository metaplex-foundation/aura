use async_trait::async_trait;
use futures::stream::Stream;
use std::pin::Pin;

type Error = Box<dyn std::error::Error + Send + Sync>;
type AssetDetailsStream = Pin<Box<dyn Stream<Item = Result<CompleteAssetDetails, Error>> + Send>>;

#[async_trait]
pub trait BlockchainDataStorage {
    async fn get_asset_details_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, Error>;
}
