use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use entities::enums::SpecificationVersions;
use solana_sdk::pubkey::Pubkey;

use crate::asset::{AssetsUpdateIdx, SlotAssetIdx};
use crate::column::TypedColumn;
use crate::key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey};
use crate::storage_traits::{AssetIndexReader, AssetSlotStorage, AssetUpdateIndexStorage};
use crate::{Result, Storage};
use entities::models::AssetIndex;

impl AssetUpdateIndexStorage for Storage {
    fn last_known_asset_updated_key(&self) -> Result<Option<(u64, u64, Pubkey)>> {
        let mut iter = self.assets_update_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let key = AssetsUpdateIdx::decode_key(last_key.to_vec())?;
            let decoded_key = decode_u64x2_pubkey(key).unwrap();
            Ok(Some(decoded_key))
        } else {
            Ok(None)
        }
    }

    fn fetch_asset_updated_keys(
        &self,
        from: Option<(u64, u64, Pubkey)>,
        up_to: Option<(u64, u64, Pubkey)>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<(u64, u64, Pubkey)>)> {
        let mut unique_pubkeys = HashSet::new();
        let mut last_key = from;

        if limit == 0 {
            return Ok((unique_pubkeys, last_key));
        }

        let iterator = match last_key {
            Some(key) => {
                let encoded = encode_u64x2_pubkey(key.0, key.1, key.2);
                let mut iter = self.assets_update_idx.iter(encoded);
                iter.next(); // Skip the first key, as it is the `from`
                iter
            }
            None => self.assets_update_idx.iter_start(),
        };

        for pair in iterator {
            let (idx_key, _) = pair?;
            let key = AssetsUpdateIdx::decode_key(idx_key.to_vec())?;
            // Stop if the current key is greater than `up_to`
            if let Some(ref up_to_key) = up_to {
                let up_to = encode_u64x2_pubkey(up_to_key.0, up_to_key.1, up_to_key.2);
                if key > up_to {
                    break;
                }
            }
            let decoded_key = decode_u64x2_pubkey(key.clone()).unwrap();
            last_key = Some(decoded_key);
            // Skip keys that are in the skip_keys set
            if skip_keys
                .as_ref()
                .map_or(false, |sk| sk.contains(&decoded_key.2))
            {
                continue;
            }

            unique_pubkeys.insert(decoded_key.2);

            if unique_pubkeys.len() >= limit {
                break;
            }
        }

        Ok((unique_pubkeys, last_key))
    }
}

#[async_trait]
impl AssetIndexReader for Storage {
    async fn get_asset_indexes(&self, keys: &[Pubkey]) -> Result<HashMap<Pubkey, AssetIndex>> {
        let mut asset_indexes = HashMap::new();

        let asset_static_details = self.asset_static_data.batch_get(keys.to_vec()).await?;
        let asset_dynamic_details = self.asset_dynamic_data.batch_get(keys.to_vec()).await?;
        let asset_authority_details = self.asset_authority_data.batch_get(keys.to_vec()).await?;
        let asset_owner_details = self.asset_owner_data.batch_get(keys.to_vec()).await?;
        let asset_collection_details = self.asset_collection_data.batch_get(keys.to_vec()).await?;

        for static_info in asset_static_details.iter().flatten() {
            let asset_index = AssetIndex {
                pubkey: static_info.pubkey,
                specification_version: SpecificationVersions::V1,
                specification_asset_class: static_info.specification_asset_class,
                royalty_target_type: static_info.royalty_target_type,
                slot_created: static_info.created_at,
                ..Default::default()
            };

            asset_indexes.insert(asset_index.pubkey, asset_index);
        }

        for dynamic_info in asset_dynamic_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&dynamic_info.pubkey) {
                existed_index.is_compressible = dynamic_info.is_compressible.value;
                existed_index.is_compressed = dynamic_info.is_compressed.value;
                existed_index.is_frozen = dynamic_info.is_frozen.value;
                existed_index.supply = dynamic_info.supply.clone().map(|s| s.value as i64);
                existed_index.is_burnt = dynamic_info.is_burnt.value;
                existed_index.creators = dynamic_info.creators.clone().value;
                existed_index.royalty_amount = dynamic_info.royalty_amount.value as i64;
                existed_index.slot_updated = dynamic_info.get_slot_updated() as i64;
                existed_index.metadata_url = Some(dynamic_info.url.value.clone());
            } else {
                let asset_index = AssetIndex {
                    pubkey: dynamic_info.pubkey,
                    is_compressible: dynamic_info.is_compressible.value,
                    is_compressed: dynamic_info.is_compressed.value,
                    is_frozen: dynamic_info.is_frozen.value,
                    supply: dynamic_info.supply.clone().map(|s| s.value as i64),
                    is_burnt: dynamic_info.is_burnt.value,
                    creators: dynamic_info.creators.clone().value,
                    royalty_amount: dynamic_info.royalty_amount.value as i64,
                    slot_updated: dynamic_info.get_slot_updated() as i64,
                    metadata_url: Some(dynamic_info.url.value.clone()),
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_authority_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.authority = Some(data.authority);
                if data.slot_updated as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.slot_updated as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    authority: Some(data.authority),
                    slot_updated: data.slot_updated as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_owner_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.owner = Some(data.owner.value);
                existed_index.delegate = data.delegate.clone().map(|delegate| delegate.value);
                existed_index.owner_type = Some(data.owner_type.value);
                if data.get_slot_updated() as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.get_slot_updated() as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    owner: Some(data.owner.value),
                    delegate: data.delegate.clone().map(|delegate| delegate.value),
                    owner_type: Some(data.owner_type.value),
                    slot_updated: data.get_slot_updated() as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_collection_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.collection = Some(data.collection);
                existed_index.is_collection_verified = Some(data.is_collection_verified);
                if data.slot_updated as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.slot_updated as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    collection: Some(data.collection),
                    is_collection_verified: Some(data.is_collection_verified),
                    slot_updated: data.slot_updated as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        Ok(asset_indexes)
    }
}

impl AssetSlotStorage for Storage {
    fn last_saved_slot(&self) -> Result<Option<u64>> {
        let mut iter = self.slot_asset_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let (slot, _) = SlotAssetIdx::decode_key(last_key.to_vec())?;
            return Ok(Some(slot));
        }

        Ok(None)
    }
}
