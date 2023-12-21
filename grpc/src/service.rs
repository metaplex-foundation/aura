use std::{pin::Pin, sync::Arc};

use crate::gapfiller::{gap_filler_service_server::GapFillerService, AssetDetails, RangeRequest};
use futures::{stream::Stream, StreamExt};
use interface::{error::UsecaseError, AssetDetailsStreamer};
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
                if let Some(usecase_error) = e.downcast_ref::<UsecaseError>() {
                    return Err(usecase_error_to_status(usecase_error));
                }
                // If it's not a UsecaseError, or if you don't have specific handling for it
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

fn usecase_error_to_status(err: &UsecaseError) -> Status {
    match err {
        UsecaseError::InvalidRange(start, end) => Status::invalid_argument(format!(
            "Invalid range: start {} is greater than end {}",
            start, end
        )),
        // Add more cases here for other UsecaseError variants if needed
        // _ => Status::internal(format!("Internal error: {:?}", err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use interface::error::UsecaseError;
    use interface::AssetDetailsStream;
    use interface::MockAssetDetailsStreamer;
    use mockall::predicate::*;

    #[tokio::test]
    async fn test_get_assets_updated_within_empty_range() {
        let mut mock_streamer = MockAssetDetailsStreamer::new();

        // Expect the method to be called and return an empty stream
        mock_streamer
            .expect_get_asset_details_stream_in_range()
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

    #[tokio::test]
    async fn test_get_assets_updated_within_invalid_range() {
        let mut mock = MockAssetDetailsStreamer::new();

        // Set expectation for an invalid range
        mock.expect_get_asset_details_stream_in_range()
            .with(eq(10), eq(0)) // Invalid range
            .times(1)
            .returning(|_start_slot, _end_slot| {
                Err(Box::new(UsecaseError::InvalidRange(10, 0)) as interface::AsyncError)
            });

        let service = PeerGapFillerServiceImpl {
            asset_details_streamer: Arc::new(mock),
        };

        let response = service
            .get_assets_updated_within(Request::new(RangeRequest {
                start_slot: 10,
                end_slot: 0,
            }))
            .await;

        // Check for a specific error response
        match response {
            Ok(_) => panic!("Expected an error for invalid range, but got OK"),
            Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
        }
    }

    // #[tokio::test]
    // async fn test_get_assets_updated_within_with_assets() {
    //     let mut mock = MockAssetDetailsStreamer::new();

    //     // Expect the method to be called and return a stream with asset details
    //     mock.expect_get_asset_details_stream_in_range()
    //         .with(eq(0), eq(10))
    //         .times(1)
    //         .returning(|_start_slot, _end_slot| {
    //             let (tx, rx) = tokio::sync::mpsc::channel(10);

    //             // Simulate sending some asset details
    //             tokio::spawn(async move {
    //                 let asset_details = CompleteAssetDetails { /* ... */ };
    //                 let _ = tx.send(Ok(asset_details)).await;
    //             });

    //             Ok(Box::pin(ReceiverStream::new(rx)) as AssetDetailsStream)
    //         });

    //     let service = PeerGapFillerServiceImpl {
    //         asset_details_streamer: Arc::new(mock),
    //     };

    //     let response = service
    //         .get_assets_updated_within(Request::new(RangeRequest {
    //             start_slot: 0,
    //             end_slot: 10,
    //         }))
    //         .await
    //         .unwrap();

    //     // Check that the stream contains items
    //     assert!(response.into_inner().next().await.is_some());
    // }
}
