use std::sync::Arc;

use entities::enums::AssetType;
use rocks_db::{
    key_encoders::encode_u64x2_pubkey, storage_traits::AssetUpdateIndexStorage, Storage,
};

use crate::error::IngesterError;

pub fn clean_syncronized_idxs(
    primary_rocks_storage: Arc<Storage>,
    asset_type: AssetType,
) -> Result<(), IngesterError> {
    let optional_last_synced_key = match asset_type {
        AssetType::NonFungible => primary_rocks_storage.last_known_nft_asset_updated_key(),
        AssetType::Fungible => primary_rocks_storage.last_known_fungible_asset_updated_key(),
    };

    if let Ok(Some(last_synced_key)) = optional_last_synced_key {
        let last_synced_key = encode_u64x2_pubkey(
            last_synced_key.seq,
            last_synced_key.slot,
            last_synced_key.pubkey,
        );
        primary_rocks_storage.clean_syncronized_idxs(asset_type, last_synced_key)?;
    };

    Ok(())
}
