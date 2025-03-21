use std::{
    collections::{HashMap, HashSet},
    sync::atomic::Ordering,
};

use bincode::serialize;
use entities::{
    api_req_params::{DisplayOptions, PaginationQuery},
    enums::{AssetType, SpecificationAssetClass, TokenMetadataEdition},
    models::{EditionData, PubkeyWithSlot},
};
use futures::future::Either;
use mpl_token_metadata::ID as METADATA_PROGRAM_ID;
use solana_sdk::pubkey::Pubkey;

use crate::{
    asset::{
        AssetCollection, AssetCompleteDetails, AssetSelectedMaps, AssetsUpdateIdx,
        FungibleAssetsUpdateIdx, SlotAssetIdx, SlotAssetIdxKey,
    },
    column::{Column, TypedColumn},
    columns::{
        asset::{AssetEditionInfo, MasterAssetEditionsInfo, TokenMetadataEditionParentIndex},
        editions::EditionIndexKey,
    },
    errors::StorageError,
    generated::asset_generated::asset as fb,
    key_encoders::encode_u64x2_pubkey,
    Result, Storage, BATCH_GET_ACTION, ROCKS_COMPONENT,
};

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
    fn get_next_fungible_asset_update_seq(&self) -> Result<u64> {
        if self.fungible_assets_update_last_seq.load(Ordering::Relaxed) == 0 {
            // If fungible_assets_update_next_seq is zero, fetch the last key from fungible_assets_update_idx
            let mut iter = self.fungible_assets_update_idx.iter_end(); // Assuming iter_end method fetches the last item

            if let Some(pair) = iter.next() {
                let (last_key, _) = pair?;
                // Assuming the key is structured as (u64, ...)

                let seq = u64::from_be_bytes(last_key[..std::mem::size_of::<u64>()].try_into()?);
                self.fungible_assets_update_last_seq.store(seq, Ordering::Relaxed);
            }
        }
        // Increment and return the sequence number
        let seq = self.fungible_assets_update_last_seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(seq)
    }

    fn get_next_asset_update_seq(&self) -> Result<u64> {
        if self.assets_update_last_seq.load(Ordering::Relaxed) == 0 {
            // If assets_update_next_seq is zero, fetch the last key from assets_update_idx
            let mut iter = self.assets_update_idx.iter_end(); // Assuming iter_end method fetches the last item

            if let Some(pair) = iter.next() {
                let (last_key, _) = pair?;
                // Assuming the key is structured as (u64, ...)

                let seq = u64::from_be_bytes(last_key[..std::mem::size_of::<u64>()].try_into()?);
                self.assets_update_last_seq.store(seq, Ordering::Relaxed);
            }
        }
        // Increment and return the sequence number
        let seq = self.assets_update_last_seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(seq)
    }

    // TODO: Add a backfiller to fill the slot_asset_idx based on the assets_update_idx

    pub fn asset_updated_batch(&self, items: Vec<PubkeyWithSlot>) -> Result<()> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        items.iter().try_for_each(|item| {
            self.asset_updated_with_batch(&mut batch, item.slot, item.pubkey)
        })?;
        self.db.write(batch)?;
        Ok(())
    }

    pub fn asset_updated(&self, slot: u64, pubkey: Pubkey) -> Result<()> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        self.asset_updated_with_batch(&mut batch, slot, pubkey)?;
        self.db.write(batch)?;
        Ok(())
    }

    pub fn fungible_asset_updated_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        slot: u64,
        pubkey: Pubkey,
    ) -> Result<()> {
        let seq = self.get_next_fungible_asset_update_seq()?;
        let value = encode_u64x2_pubkey(seq, slot, pubkey);
        let serialized_value = serialize(&FungibleAssetsUpdateIdx {})?;
        batch.put_cf(
            &self.fungible_assets_update_idx.handle(),
            Column::<FungibleAssetsUpdateIdx>::encode_key(value),
            serialized_value,
        );
        Ok(())
    }

    pub fn asset_updated_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        slot: u64,
        pubkey: Pubkey,
    ) -> Result<()> {
        let seq = self.get_next_asset_update_seq()?;
        let value = encode_u64x2_pubkey(seq, slot, pubkey);
        let serialized_value = serialize(&AssetsUpdateIdx {})?;
        batch.put_cf(
            &self.assets_update_idx.handle(),
            Column::<AssetsUpdateIdx>::encode_key(value),
            serialized_value,
        );
        let serialized_value = serialize(&SlotAssetIdx {})?;
        batch.put_cf(
            &self.slot_asset_idx.handle(),
            Column::<SlotAssetIdx>::encode_key(SlotAssetIdxKey::new(slot, pubkey)),
            serialized_value,
        );
        Ok(())
    }

    pub fn clean_syncronized_idxs(
        &self,
        asset_type: AssetType,
        last_synced_key: Vec<u8>,
    ) -> Result<()> {
        let cf = match asset_type {
            AssetType::Fungible => self.fungible_assets_update_idx.handle(),
            AssetType::NonFungible => self.assets_update_idx.handle(),
        };

        let from = vec![];
        self.db.delete_range_cf(&cf, from, last_synced_key)?;

        Ok(())
    }

    pub async fn get_asset_selected_maps_async(
        &self,
        asset_ids: Vec<Pubkey>,
        owner_address: &Option<Pubkey>,
        options: &DisplayOptions,
    ) -> Result<AssetSelectedMaps> {
        let token_accounts_fut = if let Some(owner_address) = owner_address {
            Either::Left(self.get_raw_token_accounts(
                Some(*owner_address),
                None,
                None,
                None,
                None,
                None,
                true,
            ))
        } else {
            Either::Right(async { Ok(Vec::new()) })
        };

        let inscriptions_fut = if options.show_inscription {
            Either::Left(self.inscriptions.batch_get(asset_ids.clone()))
        } else {
            Either::Right(async { Ok(Vec::new()) })
        };

        let (assets_leaf, assets_with_collectios_and_urls, token_accounts, spl_mints, inscriptions) = tokio::join!(
            self.asset_leaf_data.batch_get(asset_ids.clone()),
            self.get_assets_with_collections_and_urls(asset_ids.clone()),
            token_accounts_fut,
            self.spl_mints.batch_get(asset_ids.clone()),
            inscriptions_fut,
        );

        let (mut assets_data, assets_collection_pks, mut urls) = assets_with_collectios_and_urls?;

        let offchain_data_fut =
            self.asset_offchain_data.batch_get(urls.clone().into_values().collect::<Vec<_>>());
        let asset_collection_data_fut = if assets_collection_pks.is_empty() {
            Either::Left(async { Ok(Vec::new()) })
        } else {
            let assets_collection_pks = assets_collection_pks.into_iter().collect::<Vec<_>>();
            let start_time = chrono::Utc::now();
            let red_metrics = self.red_metrics.clone();
            let db = self.db.clone();
            Either::Right(async move {
                tokio::task::spawn_blocking(move || {
                    let collection_d = db.batched_multi_get_cf(
                        &db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                        assets_collection_pks,
                        false,
                    );
                    red_metrics.observe_request(
                        ROCKS_COMPONENT,
                        BATCH_GET_ACTION,
                        "get_asset_collection",
                        start_time,
                    );
                    // since we cannot return referenced data from this closure,
                    // we need to convert the db slice to an owned value (Vec in this case).
                    collection_d
                        .into_iter()
                        .map(|res_opt| res_opt.map(|opt| opt.map(|slice| slice.as_ref().to_vec())))
                        .collect()
                })
                .await
                .map_err(|e| StorageError::Common(e.to_string()))
            })
        };

        let (offchain_data, asset_collection_data) =
            tokio::join!(offchain_data_fut, asset_collection_data_fut);

        let mut mpl_core_collections = HashMap::new();
        for asset in asset_collection_data? {
            let asset = asset?;
            if let Some(asset) = asset {
                let asset = fb::root_as_asset_complete_details(asset.as_ref())
                    .map_err(|e| StorageError::Common(e.to_string()))?;
                let key =
                    Pubkey::new_from_array(asset.pubkey().unwrap().bytes().try_into().unwrap());
                if options.show_collection_metadata {
                    asset
                        .dynamic_details()
                        .and_then(|d| d.url())
                        .and_then(|u| u.value())
                        .map(|u| urls.insert(key, u.to_string()));
                    assets_data.insert(key, asset.into());
                }
                if let Some(collection) = asset.collection() {
                    mpl_core_collections.insert(key, AssetCollection::from(collection));
                }
            }
        }

        let offchain_data = offchain_data
            .map_err(|e| StorageError::Common(e.to_string()))?
            .into_iter()
            .filter_map(|asset| {
                asset
                    .filter(|a| {
                        if let Some(metadata) = a.metadata.as_ref() {
                            !metadata.is_empty() && a.url.is_some()
                        } else {
                            false
                        }
                    })
                    .map(|a| (a.url.clone().unwrap(), a))
            })
            .collect::<HashMap<_, _>>();

        let inscriptions = inscriptions
            .map_err(|e| StorageError::Common(e.to_string()))?
            .into_iter()
            .filter_map(|asset| asset.map(|a| (a.root, a)))
            .collect::<HashMap<_, _>>();
        let inscriptions_data = to_map!(
            self.inscription_data
                .batch_get(
                    inscriptions
                        .values()
                        .map(|inscription| inscription.inscription_data_account)
                        .collect(),
                )
                .await
        );
        let token_accounts = token_accounts.map_err(|e| StorageError::Common(e.to_string()))?;
        let spl_mints = to_map!(spl_mints);

        // As we can not rely on the asset class from the database, we need to check the mint
        assets_data
            .iter_mut()
            .filter(|(_, asset)| {
                asset.static_details.as_ref().is_some_and(|sd| {
                    sd.specification_asset_class == SpecificationAssetClass::FungibleAsset
                        || sd.specification_asset_class == SpecificationAssetClass::FungibleToken
                })
            })
            .for_each(|(_, ref mut asset)| {
                if spl_mints.get(&asset.pubkey).map(|spl_mint| spl_mint.is_nft()).unwrap_or(false) {
                    if let Some(sd) = asset.static_details.as_mut() {
                        sd.specification_asset_class = SpecificationAssetClass::Nft
                    }
                }
            });
        Ok(AssetSelectedMaps {
            editions: self
                .get_editions(
                    assets_data
                        .values()
                        .filter_map(|a: &AssetCompleteDetails| {
                            a.static_details.as_ref().map(|s| s.edition_address)
                        })
                        .flatten()
                        .collect::<Vec<_>>(),
                )
                .await?,
            mpl_core_collections,
            asset_complete_details: assets_data,
            assets_leaf: to_map!(assets_leaf),
            offchain_data,
            urls,
            inscriptions,
            inscriptions_data,
            spl_mints,
            token_accounts: token_accounts
                .into_iter()
                .flat_map(|ta| ta.map(|ta| (ta.mint, ta)))
                .collect(),
        })
    }

    pub async fn get_master_edition_child_assets(
        &self,
        master_edition: Pubkey,
        pagination: PaginationQuery,
    ) -> Result<Vec<TokenMetadataEditionParentIndex>> {
        let limit = pagination.limit.unwrap_or(100) as usize;
        let mut reverse = false;

        let iter = if pagination.after.is_some() {
            let mut iter = self.token_metadata_edition_parent_index.iter(EditionIndexKey {
                pub_key: master_edition,
                edition: pagination.after.unwrap().parse::<u64>().map_err(|_| {
                    StorageError::Common("failed to parse edition after key".to_string())
                })?,
            });
            // iterator is on the item we were searching for
            iter.next();
            iter
        } else if pagination.before.is_some() {
            let mut iter = self.token_metadata_edition_parent_index.iter_reverse(EditionIndexKey {
                pub_key: master_edition,
                edition: pagination.before.unwrap().parse::<u64>().map_err(|_| {
                    StorageError::Common("failed to parse edition before key".to_string())
                })?,
            });
            reverse = true;

            // iterator is on the item we were searching for
            iter.next();
            iter
        } else {
            let page = pagination.page.unwrap_or(1) - 1;
            let mut iter = self.db.prefix_iterator_cf(
                &self.db.cf_handle(TokenMetadataEditionParentIndex::NAME).unwrap(),
                master_edition.as_ref(),
            );

            if page > 0 {
                let skip = page as usize * limit;
                // iter.skip(n) changes the iterator type, making it incompatible with RocksDB.
                for _ in 0..skip {
                    iter.next();
                }
            }
            iter
        };

        let mut asset_keys: Vec<TokenMetadataEditionParentIndex> = Vec::with_capacity(limit);
        for item in iter {
            let (_, value) = item?;
            if let Ok(decoded_value) = TokenMetadataEditionParentIndex::decode(&value) {
                asset_keys.push(decoded_value);
            }
            if asset_keys.len() >= limit {
                break;
            }
        }

        if reverse {
            asset_keys.reverse()
        }

        Ok(asset_keys)
    }

    pub async fn get_master_edition_child_assets_info(
        &self,
        master_edition: Pubkey,
        pagination: PaginationQuery,
    ) -> Result<MasterAssetEditionsInfo> {
        let (master_edition_metadata, asset_edition_child_assets) = tokio::join!(
            self.token_metadata_edition_cbor.get_async(master_edition),
            self.get_master_edition_child_assets(master_edition, pagination),
        );

        let master_edition_metadata = match master_edition_metadata? {
            Some(TokenMetadataEdition::MasterEdition(metadata)) => metadata,
            _ => return Err(StorageError::Common("Expected MasterEdition".to_string())),
        };

        let asset_edition_info_list: Vec<AssetEditionInfo> = asset_edition_child_assets?
            .iter()
            .map(|asset_edition| AssetEditionInfo {
                mint: asset_edition.asset_key,
                edition_address: Storage::find_edition_address(&asset_edition.asset_key),
                edition: asset_edition.edition,
            })
            .collect();

        Ok(MasterAssetEditionsInfo {
            master_edition_address: Storage::find_edition_address(&master_edition),
            supply: master_edition_metadata.supply,
            max_supply: master_edition_metadata.max_supply,
            editions: asset_edition_info_list,
        })
    }

    pub fn find_edition_address(mint: &Pubkey) -> Pubkey {
        let (edition_address, _) = Pubkey::find_program_address(
            &[b"metadata", METADATA_PROGRAM_ID.as_ref(), mint.as_ref(), b"edition"],
            &METADATA_PROGRAM_ID,
        );
        edition_address
    }

    // todo: review this method as it has 2 more awaits
    async fn get_editions(
        &self,
        edition_keys: Vec<Pubkey>,
    ) -> Result<HashMap<Pubkey, EditionData>> {
        let first_batch = self.token_metadata_edition_cbor.batch_get(edition_keys).await?;
        let mut edition_data_list = Vec::new();
        let mut parent_keys = HashSet::new();

        for token_metadata_edition in &first_batch {
            match token_metadata_edition {
                Some(TokenMetadataEdition::EditionV1(edition)) => {
                    parent_keys.insert(edition.parent);
                },
                Some(TokenMetadataEdition::MasterEdition(master)) => {
                    edition_data_list.push(EditionData {
                        key: master.key,
                        supply: master.supply,
                        max_supply: master.max_supply,
                        edition_number: None,
                    });
                },
                None => {},
            }
        }

        if !parent_keys.is_empty() {
            let master_edition_map = self
                .token_metadata_edition_cbor
                .batch_get(parent_keys.into_iter().collect())
                .await?
                .into_iter()
                .filter_map(|e| {
                    if let Some(TokenMetadataEdition::MasterEdition(master)) = e {
                        Some((master.key, master))
                    } else {
                        None
                    }
                })
                .collect::<HashMap<_, _>>();

            // Overwrite supply and max_supply with data from the master edition asset.
            for token_metadata_edition in first_batch.iter().flatten() {
                if let TokenMetadataEdition::EditionV1(edition) = token_metadata_edition {
                    if let Some(master) = master_edition_map.get(&edition.parent) {
                        edition_data_list.push(EditionData {
                            key: edition.key,
                            supply: master.supply,
                            max_supply: master.max_supply,
                            edition_number: Some(edition.edition),
                        });
                    }
                }
            }
        }

        Ok(edition_data_list
            .into_iter()
            .map(|edition| (edition.key, edition))
            .collect::<HashMap<_, _>>())
    }

    pub fn get_complete_asset_details(
        &self,
        pubkey: Pubkey,
    ) -> Result<Option<AssetCompleteDetails>> {
        let data = self
            .db
            .get_pinned_cf(&self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(), pubkey)?;
        match data {
            Some(data) => {
                let asset = fb::root_as_asset_complete_details(&data)
                    .map_err(|e| StorageError::Common(e.to_string()))?;
                Ok(Some(AssetCompleteDetails::from(asset)))
            },
            _ => Ok(None),
        }
    }
    pub async fn get_complete_assets_details(
        &self,
        assets_pk: Vec<Pubkey>,
    ) -> Result<Vec<AssetCompleteDetails>> {
        let assets_results = &self.db.batched_multi_get_cf(
            &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            assets_pk,
            false,
        );

        let assets: Vec<AssetCompleteDetails> = assets_results
            .iter()
            .filter_map(|data| match data {
                Ok(Some(data)) => fb::root_as_asset_complete_details(data)
                    .map_err(|e| StorageError::Common(e.to_string()))
                    .ok()
                    .map(AssetCompleteDetails::from),
                _ => None,
            })
            .collect::<Vec<AssetCompleteDetails>>();

        Ok(assets)
    }

    #[cfg(test)]
    pub fn put_complete_asset_details_batch(
        &self,
        assets: HashMap<Pubkey, AssetCompleteDetails>,
    ) -> Result<()> {
        use crate::ToFlatbuffersConverter;

        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (pubkey, asset) in assets {
            batch.put_cf(&self.asset_data.handle(), pubkey, asset.convert_to_fb_bytes());
        }
        self.db.write(batch).map_err(StorageError::RocksDb)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_find_edition_address() {
        let mint = Pubkey::from_str("Ey2Qb8kLctbchQsMnhZs5DjY32To2QtPuXNwWvk4NosL").unwrap();
        let expected_edition =
            Pubkey::from_str("8SHfqzJYABeGfiG1apwiEYt6TvfGQiL1pdwEjvTKsyiZ").unwrap();
        assert_eq!(Storage::find_edition_address(&mint), expected_edition);
    }
}
