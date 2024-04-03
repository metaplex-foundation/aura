use crate::error::IngesterError;
use entities::models::CompleteAssetDetails;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::AssetDetailsConsumer;
use log::error;
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub async fn process_asset_details_stream(
    keep_running: Arc<AtomicBool>,
    storage: Arc<Storage>,
    start_slot: u64,
    end_slot: u64,
    mut consumer: impl AssetDetailsConsumer,
) -> u64 {
    let mut stream = match consumer
        .get_consumable_stream_in_range(start_slot, end_slot)
        .await
    {
        Ok(stream) => stream,
        Err(e) => {
            error!("Error consume asset details stream in range: {}", e);
            return 0;
        }
    };

    let mut processed_slots = 0;
    while let Some(result) = stream.next().await {
        if !keep_running.load(Ordering::SeqCst) {
            break;
        }
        match result {
            Ok(details) => {
                if let Some(e) = insert_gaped_data(storage.clone(), details).await.err() {
                    error!("Error processing gaped data: {}", e)
                }
                processed_slots += 1;
            }
            Err(e) => {
                error!("Error processing stream item: {}", e);
            }
        }
    }

    processed_slots
}

pub async fn insert_gaped_data(
    rocks_storage: Arc<Storage>,
    data: CompleteAssetDetails,
) -> Result<(), IngesterError> {
    rocks_storage.insert_gaped_data(data).await?;
    Ok(())
}
