use std::{pin::Pin, sync::Arc};

use crate::gapfiller::{gap_filler_service_server::GapFillerService, AssetDetails, RangeRequest};
use futures::{stream::Stream, StreamExt};
use interface::AssetDetailsStreamer;
use tonic::{async_trait, Request, Response, Status};

pub struct PeerGapFillerServiceImpl {
    asset_details_streamer: Arc<dyn AssetDetailsStreamer>, // Dependency injection of the streaming service
}

#[async_trait]
impl GapFillerService for PeerGapFillerServiceImpl {
    type GetAssetsUpdatedWithinStream =
        Pin<Box<dyn Stream<Item = Result<AssetDetails, Status>> + Send + Sync>>;

    async fn get_assets_updated_within(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<Self::GetAssetsUpdatedWithinStream>, Status> {
        let range = request.into_inner();

        // Convert RangeRequest to your domain-specific request format if needed
        let start_slot = range.start_slot;
        let end_slot = range.end_slot;

        // Get the stream of asset details from the business logic layer
        let asset_stream_result = self
            .asset_details_streamer
            .get_asset_details_stream_in_range(start_slot, end_slot)
            .await;

        let asset_stream = match asset_stream_result {
            Ok(stream) => stream,
            Err(e) => {
                // Convert your business logic error to tonic::Status
                return Err(Status::internal(format!("Internal error: {}", e)));
            }
        };

        let response_stream = asset_stream.map(|result| {
            result
                .map(|asset_details| asset_details.into())
                .map_err(|e| Status::internal(format!("Streaming error: {}", e)))
        });
        Ok(Response::new(Box::pin(response_stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use mockall::predicate::*;
    use interface::AssetDetailsStream;
    
    #[tokio::test]
    async fn test_get_assets_updated_within_empty_range() {
        let mut mock_streamer = interface::MockAssetDetailsStreamer::new();
        
        // Expect the method to be called and return an empty stream
        mock_streamer.expect_get_asset_details_stream_in_range()
            .with(eq(0), eq(10))
            .times(1)
            .returning(
                |_start_slot, _end_slot| Ok(Box::pin(stream::empty()) as AssetDetailsStream),
            );
        let service = super::PeerGapFillerServiceImpl {
            asset_details_streamer: Arc::new(mock_streamer),
        };

        let response = service
            .get_assets_updated_within(Request::new(RangeRequest {
                start_slot: 0,
                end_slot: 10,
            }))
            .await;

        assert!(response.is_ok());
        let mut stream = response.unwrap().into_inner();

        // Check that the stream is empty
        assert!(stream.next().await.is_none());
    }
}
