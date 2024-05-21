use crate::error::GrpcError;
use crate::gapfiller::gap_filler_service_client::GapFillerServiceClient;
use crate::gapfiller::{GetRawBlockRequest, RangeRequest};
use async_trait::async_trait;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::{
    AssetDetailsConsumer, AssetDetailsStreamNonSync, AsyncError, PeerDiscovery, RawBlocksConsumer,
    RawBlocksStreamNonSync,
};
use interface::error::StorageError;
use interface::signature_persistence::BlockProducer;
use rocks_db::Storage;
use std::str::FromStr;
use std::sync::Arc;
use tonic::transport::{Channel, Uri};
use tonic::{Code, Status};

#[derive(Clone)]
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
    async fn get_asset_details_consumable_stream_in_range(
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

#[async_trait]
impl RawBlocksConsumer for Client {
    async fn get_raw_blocks_consumable_stream_in_range(
        &mut self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<RawBlocksStreamNonSync, AsyncError> {
        Ok(Box::pin(
            self.inner
                .get_raw_blocks_within(RangeRequest {
                    start_slot,
                    end_slot,
                })
                .await
                .map_err(|e| Box::new(e) as AsyncError)?
                .into_inner()
                .map(|stream| {
                    stream
                        .and_then(|raw_block| {
                            raw_block
                                .try_into()
                                .map_err(|e: GrpcError| Status::internal(e.to_string()))
                        })
                        .map_err(|e| Box::new(e) as AsyncError)
                }),
        ))
    }
}

#[async_trait]
impl BlockProducer for Client {
    async fn get_block(
        &self,
        slot: u64,
        backup_provider: Option<Arc<impl BlockProducer>>,
    ) -> Result<solana_transaction_status::UiConfirmedBlock, StorageError> {
        if let Ok(block) = self
            .inner
            .clone()
            .get_raw_block(GetRawBlockRequest { slot })
            .await
            .map_err(|e| StorageError::Common(e.to_string()))
            .and_then(|response| {
                serde_cbor::from_slice::<entities::models::RawBlock>(
                    response.into_inner().block.as_slice(),
                )
                .map_err(|e| StorageError::Common(e.to_string()))
                .map(|raw_block| raw_block.block)
            })
        {
            return Ok(block);
        }
        if let Some(backup_provider) = backup_provider {
            let none_bp: Option<Arc<Storage>> = None;
            let block = backup_provider.get_block(slot, none_bp).await?;
            tracing::info!("Got block from backup provider for slot: {}", slot);
            return Ok(block);
        }
        Err(StorageError::NotFound)
    }
}
