use crate::error::GrpcError;
use crate::gapfiller::gap_filler_service_client::GapFillerServiceClient;
use crate::gapfiller::RangeRequest;
use async_trait::async_trait;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::{
    AssetDetailsConsumer, AssetDetailsStreamNonSync, AsyncError, PeerDiscovery,
};
use std::str::FromStr;
use tonic::transport::{Channel, Uri};
use tonic::{Code, Status};

pub struct Client {
    inner: GapFillerServiceClient<Channel>,
}

impl Client {
    pub async fn connect(peer_discovery: impl PeerDiscovery) -> Result<Self, GrpcError> {
        let url = Uri::from_str(peer_discovery.get_gapfiller_peer_addr().as_str())
            .map_err(|e| GrpcError::UriCreate(e.to_string()))?;
        let channel = Channel::builder(url).connect().await?;

        Ok(Self {
            inner: GapFillerServiceClient::new(channel),
        })
    }
}

#[async_trait]
impl AssetDetailsConsumer for Client {
    async fn get_consumable_stream_in_range(
        &mut self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStreamNonSync, AsyncError> {
        Ok(Box::pin(
            self.inner
                .get_assets_updated_within(RangeRequest {
                    start_slot,
                    end_slot,
                })
                .await
                .map_err(|e| Box::new(e) as AsyncError)?
                .into_inner()
                .map(|stream| {
                    stream
                        .and_then(|asset_details| {
                            asset_details
                                .try_into()
                                .map_err(|e: GrpcError| Status::new(Code::Internal, e.to_string()))
                        })
                        .map_err(|e| Box::new(e) as AsyncError)
                }),
        ))
    }
}