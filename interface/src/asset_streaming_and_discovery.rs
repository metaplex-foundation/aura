use async_trait::async_trait;
use entities::models::CompleteAssetDetails;
use futures::stream::Stream;
use mockall::automock;
use std::pin::Pin;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
pub type AssetDetailsStream =
    Pin<Box<dyn Stream<Item = Result<CompleteAssetDetails, AsyncError>> + Send + Sync>>;

#[automock]
#[async_trait]
pub trait AssetDetailsStreamer: Send + Sync {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, AsyncError>;
}

#[automock]
pub trait PeerDiscovery: Send + Sync {
    fn get_gapfiller_peer_addr(&self) -> String;
}
