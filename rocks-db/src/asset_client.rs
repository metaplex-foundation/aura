use bincode::serialize;
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::Ordering;

use crate::asset::{
    AssetCollection, AssetCompleteDetails, AssetSelectedMaps, AssetsUpdateIdx, SlotAssetIdx,
    SlotAssetIdxKey,FungibleAssetsUpdateIdx,
};
use crate::asset_generated::asset as fb;
use crate::column::{Column, TypedColumn};
use crate::errors::StorageError;
use crate::key_encoders::encode_u64x2_pubkey;
use crate::{Result, Storage, BATCH_GET_ACTION, ROCKS_COMPONENT};
use entities::api_req_params::Options;
use entities::enums::{AssetType, SpecificationAssetClass, TokenMetadataEdition};
use entities::models::{EditionData, PubkeyWithSlot};
use futures_util::FutureExt;
use std::collections::HashMap;

impl Storage {
    fn get_next_fungible_asset_update_seq(&self) -> Result<u64> {
        if self.fungible_assets_update_last_seq.load(Ordering::Relaxed) == 0 {
            // If fungible_assets_update_next_seq is zero, fetch the last key from fungible_assets_update_idx
            let mut iter = self.fungible_assets_update_idx.iter_end(); // Assuming iter_end method fetches the last item

            if let Some(pair) = iter.next() {
                let (last_key, _) = pair?;
                // Assuming the key is structured as (u64, ...)

                let seq = u64::from_be_bytes(last_key[..std::mem::size_of::<u64>()].try_into()?);
                self.fungible_assets_update_last_seq
                    .store(seq, Ordering::Relaxed);
            }
        }
        // Increment and return the sequence number
        let seq = self
            .fungible_assets_update_last_seq
            .fetch_add(1, Ordering::Relaxed)
            + 1;
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
        owner_address: &Option<Pubkey>,
        options: &Options,
    ) -> Result<AssetSelectedMaps> {
        let assets_with_collections_and_urls_fut =
            self.get_assets_with_collections_and_urls(asset_ids.clone());
        let assets_leaf_fut = self.asset_leaf_data.batch_get(asset_ids.clone());
        let token_accounts_fut = if let Some(owner_address) = owner_address {
            self.get_raw_token_accounts(Some(*owner_address), None, None, None, None, None, true)
                .boxed()
        } else {
            async { Ok(Vec::new()) }.boxed()
        };
        let spl_mints_fut = self.spl_mints.batch_get(asset_ids.clone());

        let inscriptions_fut = if options.show_inscription {
            self.inscriptions.batch_get(asset_ids.clone()).boxed()
        } else {
            async { Ok(Vec::new()) }.boxed()
        };
        let (mut assets_data, assets_collection_pks, mut urls) =
            assets_with_collections_and_urls_fut.await?;
        let mut mpl_core_collections = HashMap::new();
        // todo: consider async/future here, but not likely as the very next call depends on urls from this one
        if !assets_collection_pks.is_empty() {
            let assets_collection_pks = assets_collection_pks.into_iter().collect::<Vec<_>>();
            let start_time = chrono::Utc::now();
            let collection_d = self.db.batched_multi_get_cf(
                &self.asset_data.handle(),
                assets_collection_pks.clone(),
                false,
            );
            for asset in collection_d {
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
            self.red_metrics.observe_request(
                ROCKS_COMPONENT,
                BATCH_GET_ACTION,
                "get_asset_collection",
                start_time,
            );
        }

        let offchain_data_fut = self
            .asset_offchain_data
            .batch_get(urls.clone().into_values().collect::<Vec<_>>());

        let (assets_leaf, offchain_data, token_accounts, spl_mints) = tokio::join!(
            assets_leaf_fut,
            offchain_data_fut,
            token_accounts_fut,
            spl_mints_fut
        );
        let offchain_data = offchain_data
            .map_err(|e| StorageError::Common(e.to_string()))?
            .into_iter()
            .filter_map(|asset| {
                asset
                    .filter(|a| !a.metadata.is_empty())
                    .map(|a| (a.url.clone(), a))
            })
            .collect::<HashMap<_, _>>();

        let (inscriptions, inscriptions_data) = if options.show_inscription {
            let inscriptions = inscriptions_fut
                .await
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
            (inscriptions, inscriptions_data)
        } else {
            (HashMap::new(), HashMap::new())
        };
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
                if spl_mints
                    .get(&asset.pubkey)
                    .map(|spl_mint| spl_mint.is_nft())
                    .unwrap_or(false)
                {
                    asset
                        .static_details
                        .as_mut()
                        .map(|sd| sd.specification_asset_class = SpecificationAssetClass::Nft);
                }
            });
        Ok(AssetSelectedMaps {
            editions: self
                .get_editions(
                    assets_data
                        .values()
                        .filter_map(|a: &crate::asset::AssetCompleteDetails| {
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

    // todo: review this method as it has 2 more awaits
    async fn get_editions(
        &self,
        edition_keys: Vec<Pubkey>,
    ) -> Result<HashMap<Pubkey, EditionData>> {
        let first_batch = self
            .token_metadata_edition_cbor
            .batch_get_cbor(edition_keys)
            .await?;
        let mut edition_data_list = Vec::new();
        let mut parent_keys = Vec::new();

        for token_metadata_edition in &first_batch {
            match token_metadata_edition {
                Some(TokenMetadataEdition::EditionV1(edition)) => {
                    parent_keys.push(edition.parent);
                }
                Some(TokenMetadataEdition::MasterEdition(master)) => {
                    edition_data_list.push(EditionData {
                        key: master.key,
                        supply: master.supply,
                        max_supply: master.max_supply,
                        edition_number: None,
                    });
                }
                None => {}
            }
        }

        if !parent_keys.is_empty() {
            let master_edition_map = self
                .token_metadata_edition_cbor
                .batch_get_cbor(parent_keys)
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
        let data = self.db.get_pinned_cf(
            &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
            pubkey,
        )?;
        match data {
            Some(data) => {
                let asset = fb::root_as_asset_complete_details(&data)
                    .map_err(|e| StorageError::Common(e.to_string()))?;
                Ok(Some(AssetCompleteDetails::from(asset)))
            }
            _ => Ok(None),
        }
    }

    #[cfg(test)]
    pub fn put_complete_asset_details_batch(
        &self,
        assets: HashMap<Pubkey, AssetCompleteDetails>,
    ) -> Result<()> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for (pubkey, asset) in assets {
            batch.put_cf(
                &self.asset_data.handle(),
                pubkey,
                asset.convert_to_fb_bytes(),
            );
        }
        self.db.write(batch).map_err(StorageError::RocksDb)
    }
}
