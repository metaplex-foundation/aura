use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocksdb::DB;
use solana_sdk::pubkey::Pubkey;

use crate::asset::{AssetCollection, AssetLeaf, AssetSelectedMaps, AssetsUpdateIdx, SlotAssetIdx};
use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::encode_u64x2_pubkey;
use crate::offchain_data::OffChainData;
use crate::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Result, Storage};
use bincode::deserialize;
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
        self.assets_update_idx.put(value, &AssetsUpdateIdx {})?;
        self.slot_asset_idx.put((slot, pubkey), &SlotAssetIdx {})
    }
}

#[macro_export]
macro_rules! fetch_data {
    ($db:expr, $type:ident, $asset_ids:expr) => {{
        $db.batched_multi_get_cf(
            $db.cf_handle($type::NAME).unwrap(),
            $asset_ids
                .into_iter()
                .cloned()
                .map($type::encode_key)
                .collect::<Vec<_>>(),
            false,
        )
        .into_iter()
        .map(|res| {
            res.map_err(StorageError::from).and_then(|opt| {
                opt.map(|pinned| deserialize::<$type>(pinned.as_ref()).map_err(StorageError::from))
                    .transpose()
            })
        })
        .collect::<Result<Vec<Option<$type>>>>()
        .map_err(|e| StorageError::Common(e.to_string()))?
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
        let db = self.db.clone();
        let asset_ids = asset_ids.clone();
        tokio::task::spawn_blocking(move || Self::get_asset_selected_maps(db, asset_ids))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
    }

    fn get_asset_selected_maps(db: Arc<DB>, asset_ids: Vec<Pubkey>) -> Result<AssetSelectedMaps> {
        let assets_static = fetch_data!(db, AssetStaticDetails, &asset_ids);
        let assets_dynamic = fetch_data!(db, AssetDynamicDetails, &asset_ids);
        let assets_authority = fetch_data!(db, AssetAuthority, &asset_ids);
        let assets_collection = fetch_data!(db, AssetCollection, &asset_ids);
        let assets_owner = fetch_data!(db, AssetOwner, &asset_ids);
        let assets_leaf = fetch_data!(db, AssetLeaf, &asset_ids);

        let urls: HashMap<_, _> = assets_dynamic
            .iter()
            .map(|(key, asset)| (key.to_string(), asset.url.value.clone()))
            .collect();

        let offchain_data = db
            .batched_multi_get_cf(
                db.cf_handle(OffChainData::NAME).unwrap(),
                urls.clone()
                    .into_values()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(OffChainData::encode_key)
                    .collect::<Vec<_>>(),
                false,
            )
            .into_iter()
            .map(|res| {
                res.map_err(StorageError::from).and_then(|opt| {
                    opt.map(|pinned| {
                        deserialize::<OffChainData>(pinned.as_ref()).map_err(StorageError::from)
                    })
                    .transpose()
                })
            })
            .collect::<Result<Vec<Option<OffChainData>>>>()
            .map_err(|e| StorageError::Common(e.to_string()))?
            .into_iter()
            .filter_map(|asset| asset.map(|a| (a.url.clone(), a)))
            .collect::<HashMap<_, _>>();

        Ok(AssetSelectedMaps {
            assets_static,
            assets_dynamic,
            assets_authority,
            assets_collection,
            assets_owner,
            assets_leaf,
            offchain_data,
            urls,
        })
    }
}
