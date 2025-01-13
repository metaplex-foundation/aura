use std::sync::Arc;

use entities::enums::AssetType;
use postgre_client::storage_traits::AssetIndexStorage;
use rocks_db::Storage;

use crate::error::IngesterError;

pub async fn clean_syncronized_idxs(
    index_storage: Arc<impl AssetIndexStorage>,
    primary_rocks_storage: Arc<Storage>,
    asset_type: AssetType,
) -> Result<(), IngesterError> {
    let optional_last_synced_key = index_storage.fetch_last_synced_id(asset_type).await;

    if let Ok(Some(last_synced_key)) = optional_last_synced_key {
        primary_rocks_storage.clean_syncronized_idxs(asset_type, last_synced_key)?;
    };

    Ok(())
}
