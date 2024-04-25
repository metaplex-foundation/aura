use async_trait::async_trait;
use entities::models::CompleteAssetDetails;
use futures::stream::Stream;
use mockall::automock;
use std::pin::Pin;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
type AssetResult = Result<CompleteAssetDetails, AsyncError>;
pub type AssetDetailsStream = Pin<Box<dyn Stream<Item = AssetResult> + Send + Sync>>;
pub type AssetDetailsStreamNonSync = Pin<Box<dyn Stream<Item = AssetResult> + Send>>;

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
#[async_trait]
pub trait AssetDetailsConsumer: Send {
    async fn get_consumable_stream_in_range(
        &mut self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStreamNonSync, AsyncError>;
}

#[automock]
pub trait PeerDiscovery: Send + Sync {
    fn get_gapfiller_peer_addr(&self) -> String;
}
