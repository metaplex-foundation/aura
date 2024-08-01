use crate::api::synchronization_state_consistency::CATCH_UP_SEQUENCES_TIMEOUT_SEC;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::{AssetDetailsConsumer, RawBlocksConsumer};
use rocks_db::Storage;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};

/// Method returns the number of successfully processed assets
pub async fn process_raw_blocks_stream(
    rx: Receiver<()>,
    storage: Arc<Storage>,
    start_slot: u64,
    end_slot: u64,
    mut raw_blocks_consumer: impl RawBlocksConsumer,
) -> u64 {
    let mut raw_blocks_streamer = match raw_blocks_consumer
        .get_raw_blocks_consumable_stream_in_range(start_slot, end_slot)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            error!("Error consume raw blocks stream in range: {}", e);
            return 0;
        }
    };

    let mut rx = rx;
    let mut processed_slots = 0;

    while rx.is_empty() {
        match raw_blocks_streamer.next().await {
            Ok(block) => {
                if let Some(e) = storage
                    .raw_blocks_cbor
                    .put_cbor_encoded(block.slot, block)
                    .await
                    .err()
                {
                    error!("Error processing raw block: {}", e)
                } else {
                    processed_slots += 1;
                }
            }
            Err(e) => {
                error!("Error processing raw block stream item: {}", e);
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC)) => {},
            _ = rx.recv() => {
                info!("Received stop signal, stopping Gapfiller processing raw blocks...");
                return processed_slots;
            }
        }
    }

    processed_slots
}

/// Method returns the number of successfully processed slots
pub async fn process_asset_details_stream(
    rx: Receiver<()>,
    storage: Arc<Storage>,
    start_slot: u64,
    end_slot: u64,
    mut asset_details_consumer: impl AssetDetailsConsumer,
) -> u64 {
    let mut asset_details_stream = match asset_details_consumer
        .get_asset_details_consumable_stream_in_range(start_slot, end_slot)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            error!("Error consume asset details stream in range: {}", e);
            return 0;
        }
    };

    let mut rx = rx;
    let mut processed_assets = 0;

    while rx.is_empty() {
        match asset_details_stream.next().await {
            Ok(details) => {
                if let Some(e) = storage.insert_gaped_data(details).await.err() {
                    error!("Error processing gaped data: {}", e)
                } else {
                    processed_assets += 1;
                }
            }
            Err(e) => {
                error!("Error processing asset details stream item: {}", e);
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(CATCH_UP_SEQUENCES_TIMEOUT_SEC)) => {},
            _ = rx.recv() => {
                info!("Received stop signal, stopping Gapfiller processing assets...");
                return processed_assets;
            }
        }
    }

    processed_assets
}
