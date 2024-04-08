use async_trait::async_trait;
use entities::models::{CompleteAssetDetails, SerializedRawBlock};
use futures::stream::Stream;
use mockall::automock;
use std::pin::Pin;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
type AssetResult = Result<CompleteAssetDetails, AsyncError>;
pub type AssetDetailsStream = Pin<Box<dyn Stream<Item = AssetResult> + Send + Sync>>;
pub type AssetDetailsStreamNonSync = Pin<Box<dyn Stream<Item = AssetResult> + Send>>;
type RawBlocksResult = Result<SerializedRawBlock, AsyncError>;
pub type RawBlocksStream = Pin<Box<dyn Stream<Item = RawBlocksResult> + Send + Sync>>;
pub type RawBlocksStreamNonSync = Pin<Box<dyn Stream<Item = RawBlocksResult> + Send>>;

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
    async fn get_asset_details_consumable_stream_in_range(
        &mut self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStreamNonSync, AsyncError>;
}

#[automock]
pub trait PeerDiscovery: Send + Sync {
    fn get_gapfiller_peer_addr(&self) -> String;
}

#[automock]
#[async_trait]
pub trait RawBlocksStreamer: Send + Sync {
    async fn get_raw_blocks_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<RawBlocksStream, AsyncError>;
}

#[automock]
#[async_trait]
pub trait RawBlocksConsumer: Send {
    async fn get_raw_blocks_consumable_stream_in_range(
        &mut self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<RawBlocksStreamNonSync, AsyncError>;
}

#[automock]
pub trait RawBlockGetter: Send + Sync {
    fn get_raw_block(&self, slot: u64) -> Result<SerializedRawBlock, AsyncError>;
}
