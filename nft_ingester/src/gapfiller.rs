use entities::models::RawBlock;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::{AssetDetailsConsumer, RawBlocksConsumer};
use log::error;
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub async fn process_raw_blocks_stream(
    keep_running: Arc<AtomicBool>,
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

    let mut processed_slots = 0;
    while let Some(result) = raw_blocks_streamer.next().await {
        if !keep_running.load(Ordering::SeqCst) {
            break;
        }
        match result {
            Ok(serialized_block) => {
                // Deserialize block in order to check if all data transferred correctly
                let block =
                    match serde_cbor::from_slice::<RawBlock>(serialized_block.block.as_slice()) {
                        Ok(block) => block,
                        Err(e) => {
                            error!("Error deserialize block: {}", e);
                            continue;
                        }
                    };
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
    }

    processed_slots
}

pub async fn process_asset_details_stream(
    keep_running: Arc<AtomicBool>,
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

    let mut processed_slots = 0;
    while let Some(result) = asset_details_stream.next().await {
        if !keep_running.load(Ordering::SeqCst) {
            break;
        }
        match result {
            Ok(details) => {
                if let Some(e) = storage.insert_gaped_data(details).await.err() {
                    error!("Error processing gaped data: {}", e)
                } else {
                    processed_slots += 1;
                }
            }
            Err(e) => {
                error!("Error processing asset details stream item: {}", e);
            }
        }
    }

    processed_slots
}
