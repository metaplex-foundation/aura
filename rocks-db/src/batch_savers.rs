use std::sync::Arc;

use entities::{
    enums::TokenMetadataEdition,
    models::{
        InscriptionDataInfo, InscriptionInfo, Mint, TokenAccount, TokenAccountMintOwnerIdxKey,
        TokenAccountOwnerIdxKey,
    },
};
use metrics_utils::IngesterMetricsConfig;
use num_traits::Zero;
use solana_sdk::pubkey::Pubkey;
use tokio::time::Instant;
use tracing::error;
use usecase::save_metrics::result_to_metrics;

use crate::{
    asset::{AssetCollection, AssetCompleteDetails, MetadataMintMap},
    column::TypedColumn,
    columns::inscriptions::InscriptionData,
    generated::asset_generated::asset as fb,
    token_accounts::{TokenAccountMintOwnerIdx, TokenAccountOwnerIdx},
    AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Result, Storage,
};

pub struct BatchSaveStorage {
    storage: Arc<Storage>,
    batch: rocksdb::WriteBatchWithTransaction<false>,
    batch_size: usize,
    metrics: Arc<IngesterMetricsConfig>,
}

#[derive(Default, Debug)]
pub struct MetadataModels {
    pub asset_static: Option<AssetStaticDetails>,
    pub asset_dynamic: Option<AssetDynamicDetails>,
    pub asset_authority: Option<AssetAuthority>,
    pub asset_owner: Option<AssetOwner>,
    pub asset_collection: Option<AssetCollection>,
    pub metadata_mint: Option<MetadataMintMap>,
}

impl From<&MetadataModels> for AssetCompleteDetails {
    fn from(value: &MetadataModels) -> Self {
        Self {
            pubkey: value
                .asset_static
                .as_ref()
                .map(|s| s.pubkey)
                .or_else(|| value.asset_dynamic.as_ref().map(|d| d.pubkey))
                .or_else(|| value.asset_authority.as_ref().map(|a| a.pubkey))
                // this might be wrong for token accounts, where the owner is the token account pubkey, rather than the NFT mint pubkey, but in 2 cases where it's used - it's ok, as static data is passed along with the owner, so that one is used.
                .or_else(|| value.asset_owner.as_ref().map(|o| o.pubkey))
                .or_else(|| value.asset_collection.as_ref().map(|c| c.pubkey))
                .unwrap_or_default(),
            static_details: value.asset_static.clone(),
            dynamic_details: value.asset_dynamic.clone(),
            authority: value.asset_authority.clone(),
            owner: value.asset_owner.clone(),
            collection: value.asset_collection.clone(),
        }
    }
}

#[macro_export]
macro_rules! store_assets {
    ($self:expr, $asset:expr, $db_field:ident, $metric_name:expr) => {{
        let res = $self.storage.$db_field.merge_with_batch(&mut $self.batch, $asset.pubkey, $asset);

        result_to_metrics($self.metrics.clone(), &res, $metric_name);
        res
    }};
}

impl BatchSaveStorage {
    pub fn new(
        storage: Arc<Storage>,
        batch_size: usize,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Self {
        Self { storage, batch_size, batch: Default::default(), metrics }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.metrics.set_buffer("accounts_batch_size", self.batch.len() as i64);
        let begin_processing = Instant::now();
        let res = self.storage.db.write(std::mem::take(&mut self.batch)).map_err(Into::into);

        result_to_metrics(self.metrics.clone(), &res, "accounts_batch_flush");
        self.metrics
            .set_latency("accounts_batch_flush", begin_processing.elapsed().as_millis() as f64);
        res
    }
    pub fn batch_filled(&self) -> bool {
        self.batch.len() >= self.batch_size
    }

    pub fn store_complete(&mut self, data: &AssetCompleteDetails) -> Result<()> {
        self.storage.merge_compete_details_with_batch(&mut self.batch, data)?;
        let res = Ok(());
        result_to_metrics(self.metrics.clone(), &res, "accounts_complete_data_merge_with_batch");
        res
    }

    pub fn store_dynamic(&mut self, asset_dynamic: &AssetDynamicDetails) -> Result<()> {
        let asset = &AssetCompleteDetails {
            pubkey: asset_dynamic.pubkey,
            dynamic_details: Some(asset_dynamic.clone()),
            ..Default::default()
        };
        self.store_complete(asset)
    }

    pub fn store_static(&mut self, asset_dynamic: &AssetStaticDetails) -> Result<()> {
        let asset = &AssetCompleteDetails {
            pubkey: asset_dynamic.pubkey,
            static_details: Some(asset_dynamic.clone()),
            ..Default::default()
        };
        self.store_complete(asset)
    }

    fn store_metadata_mint(&mut self, metadata_mint_map: &MetadataMintMap) -> Result<()> {
        store_assets!(
            self,
            metadata_mint_map,
            metadata_mint_map,
            "metadata_mint_map_merge_with_batch"
        )
    }
    pub fn store_spl_mint(&mut self, mint: &Mint) -> Result<()> {
        let res =
            self.storage.spl_mints.merge_with_batch(&mut self.batch, mint.pubkey, &mint.into());

        result_to_metrics(self.metrics.clone(), &res, "spl_mints_merge_with_batch");
        res
    }

    pub fn store_edition(&mut self, key: Pubkey, edition: &TokenMetadataEdition) -> Result<()> {
        self.storage.token_metadata_edition_cbor.merge_with_batch(&mut self.batch, key, edition)?;
        Ok(())
    }
    pub fn store_inscription(&mut self, inscription: &InscriptionInfo) -> Result<()> {
        self.storage.inscriptions.merge_with_batch(
            &mut self.batch,
            inscription.inscription.root,
            &inscription.into(),
        )?;
        Ok(())
    }
    pub fn store_inscription_data(
        &mut self,
        key: Pubkey,
        inscription_data: &InscriptionDataInfo,
    ) -> Result<()> {
        self.storage.inscription_data.merge_with_batch(
            &mut self.batch,
            key,
            &InscriptionData {
                pubkey: key,
                data: inscription_data.inscription_data.clone(),
                write_version: inscription_data.write_version,
            },
        )?;
        Ok(())
    }
    pub fn fungible_asset_updated_with_batch(&mut self, slot: u64, pubkey: Pubkey) -> Result<()> {
        self.storage.fungible_asset_updated_with_batch(&mut self.batch, slot, pubkey)?;
        Ok(())
    }

    pub fn asset_updated_with_batch(&mut self, slot: u64, pubkey: Pubkey) -> Result<()> {
        self.storage.asset_updated_with_batch(&mut self.batch, slot, pubkey)?;
        Ok(())
    }
    pub fn save_token_account_with_idxs(
        &mut self,
        key: Pubkey,
        token_account: &TokenAccount,
    ) -> Result<()> {
        self.storage.token_accounts.merge_with_batch(&mut self.batch, key, token_account)?;
        self.storage.token_account_owner_idx.merge_with_batch(
            &mut self.batch,
            TokenAccountOwnerIdxKey {
                owner: token_account.owner,
                token_account: token_account.pubkey,
            },
            &TokenAccountOwnerIdx {
                is_zero_balance: token_account.amount.is_zero(),
                write_version: token_account.write_version,
            },
        )?;
        self.storage.token_account_mint_owner_idx.merge_with_batch(
            &mut self.batch,
            TokenAccountMintOwnerIdxKey {
                mint: token_account.mint,
                owner: token_account.owner,
                token_account: token_account.pubkey,
            },
            &TokenAccountMintOwnerIdx {
                is_zero_balance: token_account.amount.is_zero(),
                write_version: token_account.write_version,
            },
        )
    }

    pub fn get_authority(&self, address: Pubkey) -> Option<Pubkey> {
        if let Ok(Some(data)) = self
            .storage
            .db
            .get_pinned_cf(&self.storage.db.cf_handle(AssetCompleteDetails::NAME).unwrap(), address)
        {
            let asset = fb::root_as_asset_complete_details(&data);
            return asset
                .ok()
                .and_then(|a| a.authority())
                .and_then(|auth| auth.authority())
                .map(|k| Pubkey::try_from(k.bytes()).unwrap());
        }
        None
    }

    pub fn get_mint_map(&self, key: Pubkey) -> Result<Option<MetadataMintMap>> {
        self.storage.metadata_mint_map.get(key)
    }

    pub fn store_metadata_models(
        &mut self,
        asset: &AssetCompleteDetails,
        metadata_mint: Option<MetadataMintMap>,
    ) -> Result<()> {
        if let Some(metadata_mint) = &metadata_mint {
            self.store_metadata_mint(metadata_mint)?;
        }
        if asset.any_field_is_set() {
            self.store_complete(asset)?;
            let slot_updated = asset.get_slot_updated();
            if slot_updated == 0 {
                return Ok(());
            }
            if let Err(e) = self.asset_updated_with_batch(slot_updated, asset.pubkey) {
                error!("Error while updating assets update idx: {}", e);
            }
        }
        Ok(())
    }
}
