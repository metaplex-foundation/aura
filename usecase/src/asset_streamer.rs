use std::sync::Arc;

use async_trait::async_trait;
use interface::{
    asset_streaming_and_discovery::{AssetDetailsStream, AssetDetailsStreamer, AsyncError},
    error::UsecaseError,
};

pub struct AssetStreamer {
    pub max_window_size: u64,
    pub data_layer: Arc<dyn AssetDetailsStreamer>,
}

impl AssetStreamer {
    pub fn new(max_window_size: u64, data_layer: Arc<dyn AssetDetailsStreamer>) -> Self {
        Self { max_window_size, data_layer }
    }
}

#[async_trait]
impl AssetDetailsStreamer for AssetStreamer {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, AsyncError> {
        // do the business level validations here
        if start_slot > end_slot {
            return Err(Box::new(UsecaseError::InvalidRange(start_slot, end_slot)));
        }
        if end_slot - start_slot >= self.max_window_size {
            return Err(Box::new(UsecaseError::InvalidRangeTooWide(
                start_slot,
                end_slot,
                self.max_window_size,
            )));
        }

        self.data_layer.get_asset_details_stream_in_range(start_slot, end_slot).await
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::{self, StreamExt};
    use interface::asset_streaming_and_discovery::MockAssetDetailsStreamer;
    use mockall::predicate::*;

    use super::*;

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_too_wide_range() {
        let data_layer = MockAssetDetailsStreamer::new();
        let asset_streamer = AssetStreamer::new(100, Arc::new(data_layer));

        let result = asset_streamer.get_asset_details_stream_in_range(0, 100).await;

        assert!(result.is_err());
        // Check for a specific error response
        match result {
            Ok(_) => panic!("Expected an error for invalid range, but got OK"),
            Err(status) => assert_eq!(
                status.downcast_ref::<UsecaseError>(),
                Some(&UsecaseError::InvalidRangeTooWide(0, 100, 100))
            ),
        }
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_invalid_range() {
        let data_layer = MockAssetDetailsStreamer::new();
        let asset_streamer = AssetStreamer::new(100, Arc::new(data_layer));

        let result = asset_streamer.get_asset_details_stream_in_range(100, 0).await;

        assert!(result.is_err());
        // Check for a specific error response
        match result {
            Ok(_) => panic!("Expected an error for invalid range, but got OK"),
            Err(status) => assert_eq!(
                status.downcast_ref::<UsecaseError>(),
                Some(&UsecaseError::InvalidRange(100, 0))
            ),
        }
    }

    #[tokio::test]
    async fn test_get_asset_details_stream_in_range_valid_range() {
        let mut data_layer = MockAssetDetailsStreamer::new();

        // Expect the method to be called and return an empty stream
        data_layer
            .expect_get_asset_details_stream_in_range()
            .with(eq(0), eq(10))
            .times(1)
            .returning(
                |_start_slot, _end_slot| Ok(Box::pin(stream::empty()) as AssetDetailsStream),
            );
        let asset_streamer = AssetStreamer::new(100, Arc::new(data_layer));

        let result = asset_streamer.get_asset_details_stream_in_range(0, 10).await;

        assert!(result.is_ok());

        // Check that the stream is actually empty
        let stream = result.unwrap();
        let collected_items: Vec<_> = stream.collect().await;
        assert!(collected_items.is_empty());
    }
}
