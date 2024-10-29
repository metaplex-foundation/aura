use std::collections::{HashMap, HashSet};

use crate::asset::{
    AssetCollection, AssetCompleteDetails, AssetLeaf, AssetsUpdateIdx, SlotAssetIdx,
    SlotAssetIdxKey,
};
use crate::asset_generated::asset as fb;
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
use async_trait::async_trait;
use entities::enums::{SpecificationAssetClass, TokenMetadataEdition};
use entities::models::{AssetIndex, CompleteAssetDetails, UpdateVersion, Updated};
use serde_json::json;
use solana_sdk::pubkey::Pubkey;

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

impl Storage {
    pub async fn get_asset_indexes_with_collections_and_urls(
        &self,
        asset_ids: Vec<Pubkey>,
    ) -> Result<(Vec<AssetIndex>, HashSet<Pubkey>, HashMap<Pubkey, String>)> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let d = db.batched_multi_get_cf(
                &db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                asset_ids,
                false, //sorting the input and using true here slows down the method by 5% for batches or 15% for an indiviual asset
            );
            let mut asset_indexes = Vec::new();
            let mut assets_collection_pks = HashSet::new();
            let mut urls = HashMap::new();

            for asset in d {
                let asset = asset?;
                if let Some(asset) = asset {
                    let asset = fb::root_as_asset_complete_details(asset.as_ref())
                        .map_err(|e| StorageError::Common(e.to_string()))?;
                    let key =
                        Pubkey::new_from_array(asset.pubkey().unwrap().bytes().try_into().unwrap());
                    asset
                        .collection()
                        .and_then(|c| c.collection())
                        .and_then(|c| c.value())
                        .map(|c| {
                            assets_collection_pks.insert(Pubkey::try_from(c.bytes()).unwrap())
                        });
                    asset
                        .dynamic_details()
                        .and_then(|d| d.url())
                        .and_then(|u| u.value())
                        .map(|u| urls.insert(key, u.to_string()));
                    asset_indexes.push(asset.into());
                }
            }
            Ok::<(_, _, _), StorageError>((asset_indexes, assets_collection_pks, urls))
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }

    pub async fn get_assets_with_collections_and_urls(
        &self,
        asset_ids: Vec<Pubkey>,
    ) -> Result<(
        HashMap<Pubkey, AssetCompleteDetails>,
        HashSet<Pubkey>,
        HashMap<Pubkey, String>,
    )> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let d = db.batched_multi_get_cf(
                &db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                asset_ids,
                false, //sorting the input and using true here slows down the method by 5% for batches or 15% for an indiviual asset
            );
            let mut assets_data = HashMap::new();
            let mut assets_collection_pks = HashSet::new();
            let mut urls = HashMap::new();

            for asset in d {
                let asset = asset?;
                if let Some(asset) = asset {
                    let asset = fb::root_as_asset_complete_details(asset.as_ref())
                        .map_err(|e| StorageError::Common(e.to_string()))?;
                    let key =
                        Pubkey::new_from_array(asset.pubkey().unwrap().bytes().try_into().unwrap());
                    asset
                        .collection()
                        .and_then(|c| c.collection())
                        .and_then(|c| c.value())
                        .map(|c| {
                            assets_collection_pks.insert(Pubkey::try_from(c.bytes()).unwrap())
                        });
                    asset
                        .dynamic_details()
                        .and_then(|d| d.url())
                        .and_then(|u| u.value())
                        .map(|u| urls.insert(key, u.to_string()));
                    assets_data.insert(key, asset.into());
                }
            }
            Ok::<(_, _, _), StorageError>((assets_data, assets_collection_pks, urls))
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }
}

#[async_trait]
impl AssetIndexReader for Storage {
    async fn get_asset_indexes<'a>(&self, keys: &[Pubkey]) -> Result<Vec<AssetIndex>> {
        let start_time: chrono::DateTime<chrono::Utc> = chrono::Utc::now();

        let asset_index_collection_url_fut =
            self.get_asset_indexes_with_collections_and_urls(keys.to_vec());
        // these data will be used only during live synchronization
        // during full sync will be passed mint keys meaning response will be always empty
        let token_accounts_fut = self.token_accounts.batch_get(keys.to_vec());

        let (mut asset_indexes, assets_collection_pks, urls) =
            asset_index_collection_url_fut.await?;

        let offchain_data_downloaded_map_fut = self
            .asset_offchain_data
            .batch_get(urls.values().map(|u| u.to_string()).collect());

        let mut mpl_core_collections = HashMap::new();
        let core_collections_iterator = self.db.batched_multi_get_cf(
            &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            assets_collection_pks,
            false,
        );
        for asset in core_collections_iterator {
            let asset = asset?;
            if let Some(asset) = asset {
                let asset = fb::root_as_asset_complete_details(asset.as_ref())
                    .map_err(|e| StorageError::Common(e.to_string()))?;
                if let Some(auth) = asset
                    .collection()
                    .and_then(|collection| collection.authority())
                    .and_then(|auth| auth.value())
                {
                    let key =
                        Pubkey::new_from_array(asset.pubkey().unwrap().bytes().try_into().unwrap());
                    let auth_value = Pubkey::new_from_array(auth.bytes().try_into().unwrap());
                    mpl_core_collections.insert(key, auth_value);
                };
            }
        }
        let offchain_data_downloaded_map: HashMap<String, bool> = offchain_data_downloaded_map_fut
            .await?
            .into_iter()
            .flatten()
            .map(|offchain_data| {
                (
                    offchain_data.url.clone(),
                    !offchain_data.metadata.is_empty(),
                )
            })
            .collect::<HashMap<_, _>>();

        asset_indexes.iter_mut().for_each(|ref mut asset_index| {
            if let Some(coll) = asset_index.collection {
                asset_index.update_authority = mpl_core_collections.get(&coll).copied();
            }
            if let Some(ref mut mut_val) = asset_index.metadata_url {
                mut_val.is_downloaded = offchain_data_downloaded_map
                    .get(&mut_val.metadata_url)
                    .copied()
                    .unwrap_or_default();
            }
        });
        let token_accounts_details = token_accounts_fut.await?;

        // compare to other data this data is saved in HashMap by token account keys, not mints
        asset_indexes.extend(
            token_accounts_details
                .iter()
                .flatten()
                .map(|token_acc| AssetIndex {
                    pubkey: token_acc.pubkey,
                    specification_asset_class: SpecificationAssetClass::FungibleToken,
                    owner: Some(token_acc.owner),
                    fungible_asset_mint: Some(token_acc.mint),
                    fungible_asset_balance: Some(token_acc.amount as u64),
                    slot_updated: token_acc.slot_updated,
                    ..Default::default()
                }),
        );

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
        let write_version = if let Some(write_v) = data.authority.update_version {
            match write_v {
                UpdateVersion::WriteVersion(v) => Some(v),
                _ => None,
            }
        } else {
            None
        };
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let acd = AssetCompleteDetails {
            pubkey: data.pubkey,
            static_details: Some(AssetStaticDetails {
                pubkey: data.pubkey,
                specification_asset_class: data.specification_asset_class,
                royalty_target_type: data.royalty_target_type,
                created_at: data.slot_created as i64,
                edition_address: data.edition_address,
            }),
            dynamic_details: Some(AssetDynamicDetails {
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
            }),
            authority: Some(AssetAuthority {
                pubkey: data.pubkey,
                authority: data.authority.value,
                slot_updated: data.authority.slot_updated,
                write_version,
            }),
            owner: Some(AssetOwner {
                pubkey: data.pubkey,
                owner: data.owner,
                delegate: data.delegate,
                owner_type: data.owner_type,
                owner_delegate_seq: data.owner_delegate_seq,
            }),
            collection: data.collection.map(|collection| AssetCollection {
                pubkey: data.pubkey,
                collection: collection.collection,
                is_collection_verified: collection.is_collection_verified,
                authority: collection.authority,
            }),
        }
        .convert_to_fb(&mut builder);

        let mut batch = rocksdb::WriteBatch::default();
        builder.finish_minimal(acd);
        batch.merge_cf(
            &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            AssetCompleteDetails::encode_key(data.pubkey),
            builder.finished_data(),
        );

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
}
