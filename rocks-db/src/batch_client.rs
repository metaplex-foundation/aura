use std::collections::{HashMap, HashSet};

use crate::asset::{
    AssetCollection, AssetCompleteDetails, AssetLeaf, AssetsUpdateIdx, FungibleAssetsUpdateIdx,
    MplCoreCollectionAuthority, SlotAssetIdx, SlotAssetIdxKey, SourcedAssetLeaf,
};
use crate::cl_items::{ClItem, ClItemKey, ClLeaf, ClLeafKey, SourcedClItem};
use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::generated::asset_generated::asset as fb;
use crate::key_encoders::{decode_u64x2_pubkey, encode_u64x2_pubkey};
use crate::offchain_data::OffChainData;
use crate::storage_traits::{
    AssetIndexReader, AssetSlotStorage, AssetUpdateIndexStorage, AssetUpdatedKey,
};
use crate::{
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Result, Storage,
    ToFlatbuffersConverter, BATCH_GET_ACTION, BATCH_ITERATION_ACTION, ITERATOR_TOP_ACTION,
    ROCKS_COMPONENT,
};
use async_trait::async_trait;
use entities::enums::{SpecificationAssetClass, TokenMetadataEdition};
use entities::models::{
    AssetCompleteDetailsGrpc, AssetIndex, FungibleAssetIndex, UpdateVersion, Updated,
};
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use tracing::info;
impl AssetUpdateIndexStorage for Storage {
    fn last_known_fungible_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
        _ = self.db.try_catch_up_with_primary();
        let start_time = chrono::Utc::now();
        let mut iter = self.fungible_assets_update_idx.iter_end();
        if let Some(pair) = iter.next() {
            let (last_key, _) = pair?;
            let key = FungibleAssetsUpdateIdx::decode_key(last_key.to_vec())?;
            let decoded_key = decode_u64x2_pubkey(key).unwrap();
            self.red_metrics.observe_request(
                ROCKS_COMPONENT,
                ITERATOR_TOP_ACTION,
                FungibleAssetsUpdateIdx::NAME,
                start_time,
            );
            Ok(Some(decoded_key))
        } else {
            Ok(None)
        }
    }

    fn last_known_nft_asset_updated_key(&self) -> Result<Option<AssetUpdatedKey>> {
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

    fn fetch_fungible_asset_updated_keys(
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
                let mut iter = self.fungible_assets_update_idx.iter(encoded);
                iter.next(); // Skip the first key, as it is the `from`
                iter
            }
            None => self.fungible_assets_update_idx.iter_start(),
        };

        let up_to = up_to
            .map(|up_to_key| encode_u64x2_pubkey(up_to_key.seq, up_to_key.slot, up_to_key.pubkey));

        for pair in iterator {
            let (idx_key, _) = pair?;
            let key = FungibleAssetsUpdateIdx::decode_key(idx_key.to_vec())?;
            // Stop if the current key is greater than `up_to`
            if up_to.is_some() && &key > up_to.as_ref().unwrap() {
                break;
            }
            let decoded_key = decode_u64x2_pubkey(key.clone()).unwrap();
            if let Some(ref last_key) = last_key {
                if decoded_key.seq != last_key.seq + 1 && decoded_key.seq != last_key.seq {
                    // we're allowing the same sequence as it's possible to get one on a start of a cycle
                    info!("Breaking the fungibles sync loop at seq {} as the sequence is not consecutive to the previously handled {}", decoded_key.seq, last_key.seq);
                    break;
                }
            }
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
            FungibleAssetsUpdateIdx::NAME,
            start_time,
        );
        Ok((unique_pubkeys, last_key))
    }

    fn fetch_nft_asset_updated_keys(
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

        let up_to = up_to
            .map(|up_to_key| encode_u64x2_pubkey(up_to_key.seq, up_to_key.slot, up_to_key.pubkey));

        for pair in iterator {
            let (idx_key, _) = pair?;
            let key = AssetsUpdateIdx::decode_key(idx_key.to_vec())?;
            // Stop if the current key is greater than `up_to`
            if up_to.is_some() && &key > up_to.as_ref().unwrap() {
                break;
            }
            let decoded_key = decode_u64x2_pubkey(key.clone()).unwrap();
            if let Some(ref last_key) = last_key {
                if decoded_key.seq != last_key.seq + 1 && decoded_key.seq != last_key.seq {
                    info!("Breaking the NFT sync loop at seq {} as the sequence is not consecutive to the previously handled {}", decoded_key.seq, last_key.seq);
                    break;
                }
            }
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
                    if asset.static_details().is_none() {
                        continue;
                    }
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
                        .filter(|s| !s.is_empty())
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
        let red_metrics = self.red_metrics.clone();
        tokio::task::spawn_blocking(move || {
            let start_time = chrono::Utc::now();
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
            red_metrics.observe_request(
                ROCKS_COMPONENT,
                BATCH_GET_ACTION,
                AssetCompleteDetails::NAME,
                start_time,
            );
            Ok::<(_, _, _), StorageError>((assets_data, assets_collection_pks, urls))
        })
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?
    }
}

#[async_trait]
impl AssetIndexReader for Storage {
    async fn get_fungible_assets_indexes(
        &self,
        keys: &[Pubkey],
    ) -> Result<Vec<FungibleAssetIndex>> {
        let mut fungible_assets_indexes: Vec<FungibleAssetIndex> = Vec::new();
        let start_time = chrono::Utc::now();

        let token_accounts_details = self.token_accounts.batch_get(keys.to_vec()).await?;

        for token_acc in token_accounts_details.iter().flatten() {
            let fungible_asset_index = FungibleAssetIndex {
                pubkey: token_acc.pubkey,
                owner: Some(token_acc.owner),
                slot_updated: token_acc.slot_updated,
                fungible_asset_mint: Some(token_acc.mint),
                fungible_asset_balance: Some(token_acc.amount as u64),
            };

            fungible_assets_indexes.push(fungible_asset_index);
        }

        self.red_metrics.observe_request(
            ROCKS_COMPONENT,
            BATCH_ITERATION_ACTION,
            "collect_fungible_assets_indexes",
            start_time,
        );

        Ok(fungible_assets_indexes)
    }

    async fn get_nft_asset_indexes<'a>(&self, keys: &[Pubkey]) -> Result<Vec<AssetIndex>> {
        let start_time = chrono::Utc::now();

        let asset_index_collection_url_fut =
            self.get_asset_indexes_with_collections_and_urls(keys.to_vec());
        let spl_mints_fut = self.spl_mints.batch_get(keys.to_vec());

        let (mut asset_indexes, assets_collection_pks, urls) =
            asset_index_collection_url_fut.await?;

        let spl_mints = spl_mints_fut.await?;
        let is_nft_map = spl_mints
            .into_iter()
            .flatten()
            .map(|spl_mint| (spl_mint.pubkey, spl_mint.is_nft()))
            .collect::<HashMap<_, _>>();
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
            .filter(|off_chain_data| {
                off_chain_data.url.is_some() && off_chain_data.metadata.is_some()
            })
            .map(|off_chain_data| {
                (
                    off_chain_data.url.unwrap().clone(),
                    !off_chain_data.metadata.unwrap().is_empty(),
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
            // We can not trust this field and have to double check it
            if (asset_index.specification_asset_class == SpecificationAssetClass::FungibleToken
                || asset_index.specification_asset_class == SpecificationAssetClass::FungibleAsset)
                && is_nft_map
                    .get(&asset_index.pubkey)
                    .map(|v| *v)
                    .unwrap_or_default()
            {
                asset_index.specification_asset_class = SpecificationAssetClass::Nft;
            }
        });

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
    pub async fn insert_gaped_data(&self, data: AssetCompleteDetailsGrpc) -> Result<()> {
        let write_version = if let Some(write_v) = data.authority.update_version {
            match write_v {
                UpdateVersion::WriteVersion(v) => Some(v),
                _ => None,
            }
        } else {
            None
        };
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
                pubkey: data.owner_record_pubkey,
                owner: data.owner,
                delegate: data.delegate,
                owner_type: data.owner_type,
                owner_delegate_seq: data.owner_delegate_seq,
                is_current_owner: data.is_current_owner,
            }),
            collection: data.collection.map(|collection| AssetCollection {
                pubkey: data.pubkey,
                collection: collection.collection,
                is_collection_verified: collection.is_collection_verified,
                authority: collection.authority,
            }),
        };
        let mut batch = rocksdb::WriteBatch::default();
        self.merge_compete_details_with_batch(&mut batch, &acd)?;

        if let Some(leaf) = data.asset_leaf {
            self.asset_leaf_data.merge_with_batch_raw(
                &mut batch,
                data.pubkey,
                bincode::serialize(&SourcedAssetLeaf {
                    leaf: AssetLeaf {
                        pubkey: data.pubkey,
                        tree_id: leaf.value.tree_id,
                        leaf: leaf.value.leaf.clone(),
                        nonce: leaf.value.nonce,
                        data_hash: leaf.value.data_hash,
                        creator_hash: leaf.value.creator_hash,
                        leaf_seq: leaf.value.leaf_seq,
                        slot_updated: leaf.slot_updated,
                    },
                    // todo: probably that's a finalized source, needs to be checked
                    is_from_finalized_source: false,
                })
                .map_err(|e| StorageError::Common(e.to_string()))?,
            )?;
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
            self.cl_items.merge_with_batch_raw(
                &mut batch,
                ClItemKey::new(item.cli_node_idx, item.cli_tree_key),
                bincode::serialize(&SourcedClItem {
                    // todo: probably that's a finalized source, needs to be checked
                    is_from_finalized_source: false,
                    item: ClItem {
                        cli_node_idx: item.cli_node_idx,
                        cli_tree_key: item.cli_tree_key,
                        cli_leaf_idx: item.cli_leaf_idx,
                        cli_seq: item.cli_seq,
                        cli_level: item.cli_level,
                        cli_hash: item.cli_hash.clone(),
                        slot_updated: item.slot_updated,
                    },
                })
                .map_err(|e| StorageError::Common(e.to_string()))?,
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
        if let Some(off_chain_data) = data.offchain_data {
            let url = off_chain_data.url.clone();
            let off_chain_data = OffChainData::from(off_chain_data);
            self.asset_offchain_data.merge_with_batch_flatbuffers(
                &mut batch,
                url,
                &off_chain_data,
            )?;
        }
        if let Some(spl_mint) = data.spl_mint {
            self.spl_mints
                .merge_with_batch(&mut batch, spl_mint.pubkey, &spl_mint)?;
        }
        self.write_batch(batch).await?;
        Ok(())
    }

    pub(crate) fn merge_compete_details_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        data: &AssetCompleteDetails,
    ) -> Result<()> {
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(2500);
        let acd = data.convert_to_fb(&mut builder);
        builder.finish_minimal(acd);
        batch.merge_cf(
            &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            data.pubkey,
            builder.finished_data(),
        );
        // Store the MPL Core Collection authority for easy access
        if data
            .static_details
            .as_ref()
            .filter(|sd| sd.specification_asset_class == SpecificationAssetClass::MplCoreCollection)
            .is_some()
            && data.collection.is_some()
        {
            self.mpl_core_collection_authorities.merge_with_batch(
                batch,
                data.pubkey,
                &MplCoreCollectionAuthority {
                    // total BS
                    authority: data.collection.as_ref().unwrap().authority.clone(),
                },
            )?; //this will never error in fact
        }
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
