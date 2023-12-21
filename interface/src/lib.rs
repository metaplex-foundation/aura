pub mod error;

use async_trait::async_trait;
use entities::models::CompleteAssetDetails;
use futures::stream::Stream;
use std::pin::Pin;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
pub type AssetDetailsStream =
    Pin<Box<dyn Stream<Item = Result<CompleteAssetDetails, AsyncError>> + Send + Sync>>;

#[mockall::automock]
#[async_trait]
pub trait AssetDetailsStreamer: Send + Sync {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, AsyncError>;
}
