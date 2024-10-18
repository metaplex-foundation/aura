use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use entities::enums::{SpecificationAssetClass, SpecificationVersions, TokenMetadataEdition};
use serde_json::json;
use solana_sdk::pubkey::Pubkey;

use crate::asset::{AssetCollection, AssetLeaf, AssetsUpdateIdx, SlotAssetIdx, SlotAssetIdxKey};
use crate::cl_items::{ClItem, ClItemKey, ClLeaf, ClLeafKey};
use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey};
use crate::storage_traits::{
    AssetIndexReader, AssetSlotStorage, AssetUpdateIndexStorage, AssetUpdatedKey,
};
use crate::{
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Result, Storage,
    BATCH_ITERATION_ACTION, ITERATOR_TOP_ACTION, ROCKS_COMPONENT,
};
use entities::models::{AssetIndex, CompleteAssetDetails, UpdateVersion, Updated, UrlWithStatus};

impl AssetUpdateIndexStorage for Storage {
    fn last_known_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
        _ = self.db.try_catch_up_with_primary();
        let start_time = chrono::Utc::now();
        let mut iter = self.assets_update_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let key = AssetsUpdateIdx::decode_key(last_key.to_vec())?;
            let decoded_key = decode_u64x2_pubkey(key).unwrap();
            self.red_metrics.observe_request(
                ROCKS_COMPONENT,
                ITERATOR_TOP_ACTION,
                AssetsUpdateIdx::NAME,
                start_time,
            );
            Ok(Some(decoded_key))
        } else {
            Ok(None)
        }
    }

    fn fetch_asset_updated_keys(
        &self,
        from: Option<AssetUpdatedKey>,
        up_to: Option<AssetUpdatedKey>,
        limit: usize,
        skip_keys: Option<HashSet<Pubkey>>,
    ) -> Result<(HashSet<Pubkey>, Option<AssetUpdatedKey>)> {
        let mut unique_pubkeys = HashSet::new();
        let mut last_key = from;

        if limit == 0 {
            return Ok((unique_pubkeys, last_key));
        }
        let start_time = chrono::Utc::now();

        let iterator = match last_key.clone() {
            Some(key) => {
                let encoded = encode_u64x2_pubkey(key.seq, key.slot, key.pubkey);
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
                let up_to = encode_u64x2_pubkey(up_to_key.seq, up_to_key.slot, up_to_key.pubkey);
                if key > up_to {
                    break;
                }
            }
            let decoded_key = decode_u64x2_pubkey(key.clone()).unwrap();
            last_key = Some(decoded_key.clone());
            // Skip keys that are in the skip_keys set
            if skip_keys
                .as_ref()
                .map_or(false, |sk| sk.contains(&decoded_key.pubkey))
            {
                continue;
            }

            unique_pubkeys.insert(decoded_key.pubkey);

            if unique_pubkeys.len() >= limit {
                break;
            }
        }
        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            BATCH_ITERATION_ACTION,
            AssetsUpdateIdx::NAME,
            start_time,
        );
        Ok((unique_pubkeys, last_key))
    }
}

#[async_trait]
impl AssetIndexReader for Storage {
    async fn get_asset_indexes<'a>(
        &self,
        keys: &[Pubkey],
        collection_authorities: Option<&'a HashMap<Pubkey, Pubkey>>,
    ) -> Result<HashMap<Pubkey, AssetIndex>> {
        let mut asset_indexes = HashMap::new();
        let start_time = chrono::Utc::now();
        let assets_static_fut = self.asset_static_data.batch_get(keys.to_vec());
        let assets_dynamic_fut = self.asset_dynamic_data.batch_get(keys.to_vec());
        let assets_authority_fut = self.asset_authority_data.batch_get(keys.to_vec());
        let assets_owner_fut = self.asset_owner_data.batch_get(keys.to_vec());
        let assets_collection_fut = self.asset_collection_data.batch_get(keys.to_vec());
        // these data will be used only during live synchronization
        // during full sync will be passed mint keys meaning response will be always empty
        let token_accounts_fut = self.token_accounts.batch_get(keys.to_vec());

        let (
            asset_static_details,
            asset_dynamic_details,
            asset_authority_details,
            asset_owner_details,
            asset_collection_details,
            token_accounts_details,
        ) = tokio::join!(
            assets_static_fut,
            assets_dynamic_fut,
            assets_authority_fut,
            assets_owner_fut,
            assets_collection_fut,
            token_accounts_fut
        );

        let asset_static_details = asset_static_details?;
        let asset_dynamic_details = asset_dynamic_details?;
        let asset_authority_details = asset_authority_details?;
        let asset_owner_details = asset_owner_details?;
        let asset_collection_details = asset_collection_details?;
        let token_accounts_details = token_accounts_details?;

        let mpl_core_map = {
            // during dump creation hashmap with collection authorities will be passed
            // and during regular synchronization we should make additional select from DB
            if collection_authorities.is_some() {
                HashMap::new()
            } else {
                let assets_collection_pks = asset_collection_details
                    .iter()
                    .flat_map(|c| c.as_ref().map(|c| c.collection.value))
                    .collect::<Vec<_>>();

                self.asset_collection_data
                    .batch_get(assets_collection_pks)
                    .await?
                    .into_iter()
                    .flatten()
                    .filter_map(|asset| asset.authority.value.map(|v| (asset.pubkey, v)))
                    .collect::<HashMap<_, _>>()
            }
        };

        // mpl_core_map.is_empty() && collection_authorities.is_some() check covers case when DB is empty
        let mpl_core_collections = if mpl_core_map.is_empty() && collection_authorities.is_some() {
            collection_authorities.unwrap()
        } else {
            &mpl_core_map
        };

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
                existed_index.pubkey = dynamic_info.pubkey;
                existed_index.is_compressible = dynamic_info.is_compressible.value;
                existed_index.is_compressed = dynamic_info.is_compressed.value;
                existed_index.is_frozen = dynamic_info.is_frozen.value;
                existed_index.supply = dynamic_info.supply.clone().map(|s| s.value as i64);
                existed_index.is_burnt = dynamic_info.is_burnt.value;
                existed_index.creators = dynamic_info.creators.clone().value;
                existed_index.royalty_amount = dynamic_info.royalty_amount.value as i64;
                existed_index.slot_updated = dynamic_info.get_slot_updated() as i64;
                existed_index.metadata_url = self.url_with_status_for(dynamic_info);
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
                    metadata_url: self.url_with_status_for(dynamic_info),
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_authority_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.pubkey = data.pubkey;
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
                existed_index.pubkey = data.pubkey;
                existed_index.owner = data.owner.value;
                existed_index.delegate = data.delegate.value;
                existed_index.owner_type = Some(data.owner_type.value);
                if data.get_slot_updated() as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.get_slot_updated() as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    owner: data.owner.value,
                    delegate: data.delegate.value,
                    owner_type: Some(data.owner_type.value),
                    slot_updated: data.get_slot_updated() as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        for data in asset_collection_details.iter().flatten() {
            if let Some(existed_index) = asset_indexes.get_mut(&data.pubkey) {
                existed_index.pubkey = data.pubkey;
                existed_index.collection = Some(data.collection.value);
                existed_index.is_collection_verified = Some(data.is_collection_verified.value);
                existed_index.update_authority =
                    mpl_core_collections.get(&data.collection.value).copied();
                if data.get_slot_updated() as i64 > existed_index.slot_updated {
                    existed_index.slot_updated = data.get_slot_updated() as i64;
                }
            } else {
                let asset_index = AssetIndex {
                    pubkey: data.pubkey,
                    collection: Some(data.collection.value),
                    is_collection_verified: Some(data.is_collection_verified.value),
                    update_authority: mpl_core_collections.get(&data.collection.value).copied(),
                    slot_updated: data.get_slot_updated() as i64,
                    ..Default::default()
                };

                asset_indexes.insert(asset_index.pubkey, asset_index);
            }
        }

        // compare to other data this data is saved in HashMap by token account keys, not mints
        for token_acc in token_accounts_details.iter().flatten() {
            let asset_index = AssetIndex {
                pubkey: token_acc.pubkey,
                specification_asset_class: SpecificationAssetClass::FungibleToken,
                owner: Some(token_acc.owner),
                fungible_asset_mint: Some(token_acc.mint),
                fungible_asset_balance: Some(token_acc.amount as u64),
                slot_updated: token_acc.slot_updated,
                ..Default::default()
            };

            asset_indexes.insert(token_acc.pubkey, asset_index);
        }

        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            BATCH_ITERATION_ACTION,
            "collect_asset_indexes",
            start_time,
        );
        Ok(asset_indexes)
    }
}

impl AssetSlotStorage for Storage {
    fn last_saved_slot(&self) -> Result<Option<u64>> {
        let mut iter = self.slot_asset_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let SlotAssetIdxKey { slot, .. } = SlotAssetIdx::decode_key(last_key.to_vec())?;
            return Ok(Some(slot));
        }

        Ok(None)
    }
}

impl Storage {
    pub async fn insert_gaped_data(&self, data: CompleteAssetDetails) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        self.asset_static_data.merge_with_batch(
            &mut batch,
            data.pubkey,
            &AssetStaticDetails {
                pubkey: data.pubkey,
                specification_asset_class: data.specification_asset_class,
                royalty_target_type: data.royalty_target_type,
                created_at: data.slot_created as i64,
                edition_address: data.edition_address,
            },
        )?;

        self.asset_dynamic_data.merge_with_batch(
            &mut batch,
            data.pubkey,
            &AssetDynamicDetails {
                pubkey: data.pubkey,
                is_compressible: data.is_compressible,
                is_compressed: data.is_compressed,
                is_frozen: data.is_frozen,
                supply: data.supply,
                seq: data.seq,
                is_burnt: data.is_burnt,
                was_decompressed: data.was_decompressed,
                onchain_data: data.onchain_data.map(|chain_data| {
                    Updated::new(
                        chain_data.slot_updated,
                        chain_data.update_version,
                        json!(chain_data.value).to_string(),
                    )
                }),
                creators: data.creators,
                royalty_amount: data.royalty_amount,
                url: data.url,
                chain_mutability: data.chain_mutability,
                lamports: data.lamports,
                executable: data.executable,
                metadata_owner: data.metadata_owner,
                raw_name: data.raw_name,
                mpl_core_plugins: data.mpl_core_plugins,
                mpl_core_unknown_plugins: data.mpl_core_unknown_plugins,
                rent_epoch: data.rent_epoch,
                num_minted: data.num_minted,
                current_size: data.current_size,
                plugins_json_version: data.plugins_json_version,
                mpl_core_external_plugins: data.mpl_core_external_plugins,
                mpl_core_unknown_external_plugins: data.mpl_core_unknown_external_plugins,
                mint_extensions: data.mint_extensions,
            },
        )?;

        let write_version = if let Some(write_v) = data.authority.update_version {
            match write_v {
                UpdateVersion::WriteVersion(v) => Some(v),
                _ => None,
            }
        } else {
            None
        };

        self.asset_authority_data.merge_with_batch(
            &mut batch,
            data.pubkey,
            &AssetAuthority {
                pubkey: data.pubkey,
                authority: data.authority.value,
                slot_updated: data.authority.slot_updated,
                write_version,
            },
        )?;

        if let Some(collection) = data.collection {
            self.asset_collection_data.merge_with_batch(
                &mut batch,
                data.pubkey,
                &AssetCollection {
                    pubkey: data.pubkey,
                    collection: collection.collection,
                    is_collection_verified: collection.is_collection_verified,
                    authority: collection.authority,
                },
            )?;
        }

        if let Some(leaf) = data.asset_leaf {
            self.asset_leaf_data.merge_with_batch(
                &mut batch,
                data.pubkey,
                &AssetLeaf {
                    pubkey: data.pubkey,
                    tree_id: leaf.value.tree_id,
                    leaf: leaf.value.leaf.clone(),
                    nonce: leaf.value.nonce,
                    data_hash: leaf.value.data_hash,
                    creator_hash: leaf.value.creator_hash,
                    leaf_seq: leaf.value.leaf_seq,
                    slot_updated: leaf.slot_updated,
                },
            )?
        }

        self.asset_owner_data.merge_with_batch(
            &mut batch,
            data.pubkey,
            &AssetOwner {
                pubkey: data.pubkey,
                owner: data.owner,
                delegate: data.delegate,
                owner_type: data.owner_type,
                owner_delegate_seq: data.owner_delegate_seq,
            },
        )?;

        if let Some(leaf) = data.cl_leaf {
            self.cl_leafs.put_with_batch(
                &mut batch,
                ClLeafKey::new(leaf.cli_leaf_idx, leaf.cli_tree_key),
                &ClLeaf {
                    cli_leaf_idx: leaf.cli_leaf_idx,
                    cli_tree_key: leaf.cli_tree_key,
                    cli_node_idx: leaf.cli_node_idx,
                },
            )?
        }
        for item in data.cl_items {
            self.cl_items.merge_with_batch(
                &mut batch,
                ClItemKey::new(item.cli_node_idx, item.cli_tree_key),
                &ClItem {
                    cli_node_idx: item.cli_node_idx,
                    cli_tree_key: item.cli_tree_key,
                    cli_leaf_idx: item.cli_leaf_idx,
                    cli_seq: item.cli_seq,
                    cli_level: item.cli_level,
                    cli_hash: item.cli_hash.clone(),
                    slot_updated: item.slot_updated,
                },
            )?;
        }
        if let Some(edition) = data.edition {
            self.token_metadata_edition_cbor.merge_with_batch_cbor(
                &mut batch,
                edition.key,
                &TokenMetadataEdition::EditionV1(edition),
            )?;
        }
        if let Some(master_edition) = data.master_edition {
            self.token_metadata_edition_cbor.merge_with_batch_cbor(
                &mut batch,
                master_edition.key,
                &TokenMetadataEdition::MasterEdition(master_edition),
            )?;
        }
        if let Some(offchain_data) = data.offchain_data {
            self.asset_offchain_data.merge_with_batch_cbor(
                &mut batch,
                offchain_data.url.clone(),
                &offchain_data,
            )?;
        }
        if let Some(spl_mint) = data.spl_mint {
            self.spl_mints
                .merge_with_batch(&mut batch, spl_mint.pubkey, &spl_mint)?;
        }
        self.write_batch(batch).await?;
        Ok(())
    }

    pub(crate) async fn write_batch(&self, batch: rocksdb::WriteBatch) -> Result<()> {
        let backend = self.db.clone();
        tokio::task::spawn_blocking(move || backend.write(batch))
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?
            .map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }

    fn url_with_status_for(&self, dynamic_info: &AssetDynamicDetails) -> Option<UrlWithStatus> {
        if dynamic_info.url.value.trim().is_empty() {
            None
        } else {
            // doing this check because there may be saved empty strings for some urls
            // because of bug in previous code
            let is_downloaded = self
                .asset_offchain_data
                .get(dynamic_info.url.value.clone())
                .ok()
                .flatten()
                .map(|a| !a.metadata.is_empty())
                .unwrap_or(false);

            Some(UrlWithStatus::new(&dynamic_info.url.value, is_downloaded))
        }
    }
}
