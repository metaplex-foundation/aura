use crate::gapfiller::gap_filler_service_client::GapFillerServiceClient;
use crate::gapfiller::RangeRequest;
use async_trait::async_trait;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::{
    AssetDetailsConsumer, AssetDetailsStreamNonSync, AsyncError, PeerDiscovery,
};
use tonic::transport::{Channel, Error};

pub struct Client {
    inner: GapFillerServiceClient<Channel>,
}

impl Client {
    pub async fn new(peer_discovery: impl PeerDiscovery) -> Result<Self, Error> {
        let channel = Channel::from_static(peer_discovery.get_gapfiller_peer_addr())
            .connect()
            .await?;

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
                        .map(|asset_details| asset_details.into())
                        .map_err(|e| Box::new(e) as AsyncError)
                }),
        ))
    }
}
