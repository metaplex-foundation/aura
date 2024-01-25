use crate::error::IngesterError;
use entities::models::CompleteAssetDetails;
use futures::StreamExt;
use interface::asset_streaming_and_discovery::AssetDetailsStream;
use log::error;
use rocks_db::Storage;
use std::sync::Arc;

pub async fn process_asset_details_stream(storage: Arc<Storage>, mut stream: AssetDetailsStream) {
    while let Some(result) = stream.next().await {
        match result {
            Ok(details) => {
                if let Some(e) = insert_gaped_data(storage.clone(), details).await.err() {
                    error!("Error processing gaped data: {}", e)
                }
            }
            Err(e) => {
                error!("Error processing stream item: {}", e);
            }
        }
    }
}

pub async fn insert_gaped_data(
    rocks_storage: Arc<Storage>,
    data: CompleteAssetDetails,
) -> Result<(), IngesterError> {
    rocks_storage.insert_gaped_data(data).await?;
    Ok(())
}
