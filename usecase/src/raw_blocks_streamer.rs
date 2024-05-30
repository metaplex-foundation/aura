use async_trait::async_trait;
use interface::asset_streaming_and_discovery::{AsyncError, RawBlocksStream, RawBlocksStreamer};
use interface::error::UsecaseError;
use std::sync::Arc;

pub struct BlocksStreamer {
    pub max_window_size: u64,
    pub data_layer: Arc<dyn RawBlocksStreamer>,
}

impl BlocksStreamer {
    pub fn new(max_window_size: u64, data_layer: Arc<dyn RawBlocksStreamer>) -> Self {
        Self {
            max_window_size,
            data_layer,
        }
    }
}

#[async_trait]
impl RawBlocksStreamer for BlocksStreamer {
    async fn get_raw_blocks_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<RawBlocksStream, AsyncError> {
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

        self.data_layer
            .get_raw_blocks_stream_in_range(start_slot, end_slot)
            .await
    }
}
