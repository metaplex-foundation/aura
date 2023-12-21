use async_trait::async_trait;
use entities::models::CompleteAssetDetails;
use futures::stream::Stream;
use std::pin::Pin;

type Error = Box<dyn std::error::Error + Send + Sync>;
pub type AssetDetailsStream =
    Pin<Box<dyn Stream<Item = Result<CompleteAssetDetails, Error>> + Send + Sync>>;

#[async_trait]
pub trait AssetDetailsStreamer: Send + Sync {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, Error>;
}
