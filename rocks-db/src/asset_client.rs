use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::Ordering;

use crate::asset::{AssetSelectedMaps, AssetsUpdateIdx, SlotAssetIdx};
use crate::errors::StorageError;
use crate::key_encoders::encode_u64x2_pubkey;
use crate::{Result, Storage};
use std::collections::HashMap;

impl Storage {
    fn get_next_asset_update_seq(&self) -> Result<u64> {
        if self.assets_update_last_seq.load(Ordering::SeqCst) == 0 {
            // If assets_update_next_seq is zero, fetch the last key from assets_update_idx
            let mut iter = self.assets_update_idx.iter_end(); // Assuming iter_end method fetches the last item

            if let Some(pair) = iter.next() {
                let (last_key, _) = pair?;
                // Assuming the key is structured as (u64, ...)

                let seq = u64::from_be_bytes(last_key[..std::mem::size_of::<u64>()].try_into()?);
                self.assets_update_last_seq.store(seq, Ordering::SeqCst);
            }
        }
        // Increment and return the sequence number
        let seq = self.assets_update_last_seq.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(seq)
    }

    // TODO: Add a backfiller to fill the slot_asset_idx based on the assets_update_idx

    pub fn asset_updated(&self, slot: u64, pubkey: Pubkey) -> Result<()> {
        let seq = self.get_next_asset_update_seq()?;
        let value = encode_u64x2_pubkey(seq, slot, pubkey);
        self.assets_update_idx.put(value, AssetsUpdateIdx {})?;
        self.slot_asset_idx.put((slot, pubkey), SlotAssetIdx {})
    }
}

#[macro_export]
macro_rules! to_map {
    ($res:expr) => {{
        $res.map_err(|e| StorageError::Common(e.to_string()))?
            .into_iter()
            .filter_map(|asset| asset.map(|a| (a.pubkey, a)))
            .collect::<HashMap<_, _>>()
    }};
}

impl Storage {
    pub async fn get_asset_selected_maps_async(
        &self,
        asset_ids: Vec<Pubkey>,
    ) -> Result<AssetSelectedMaps> {
        let assets_dynamic_fut = self.asset_dynamic_data.batch_get(asset_ids.clone());
        let assets_static_fut = self.asset_static_data.batch_get(asset_ids.clone());
        let assets_authority_fut = self.asset_authority_data.batch_get(asset_ids.clone());
        let assets_collection_fut = self.asset_collection_data.batch_get(asset_ids.clone());
        let assets_owner_fut = self.asset_owner_data.batch_get(asset_ids.clone());
        let assets_leaf_fut = self.asset_leaf_data.batch_get(asset_ids.clone());

        let assets_dynamic = to_map!(assets_dynamic_fut.await);
        let urls: HashMap<_, _> = assets_dynamic
            .iter()
            .map(|(key, asset)| (key.to_string(), asset.url.value.clone()))
            .collect();
        let offchain_data_fut = self
            .asset_offchain_data
            .batch_get(urls.clone().into_values().collect::<Vec<_>>());

        let (
            assets_static,
            assets_authority,
            assets_collection,
            assets_owner,
            assets_leaf,
            offchain_data,
        ) = tokio::join!(
            assets_static_fut,
            assets_authority_fut,
            assets_collection_fut,
            assets_owner_fut,
            assets_leaf_fut,
            offchain_data_fut
        );
        let offchain_data = offchain_data
            .map_err(|e| StorageError::Common(e.to_string()))?
            .into_iter()
            .filter_map(|asset| asset.map(|a| (a.url.clone(), a)))
            .collect::<HashMap<_, _>>();
        Ok(AssetSelectedMaps {
            assets_static: to_map!(assets_static),
            assets_dynamic,
            assets_authority: to_map!(assets_authority),
            assets_collection: to_map!(assets_collection),
            assets_owner: to_map!(assets_owner),
            assets_leaf: to_map!(assets_leaf),
            offchain_data,
            urls,
        })
    }
}
