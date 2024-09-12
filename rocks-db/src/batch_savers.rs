use crate::asset::{AssetCollection, MetadataMintMap};
use crate::token_accounts::{TokenAccountMintOwnerIdx, TokenAccountOwnerIdx};
use crate::Result;
use crate::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use entities::enums::TokenMetadataEdition;
use entities::models::{
    InscriptionDataInfo, InscriptionInfo, TokenAccount, TokenAccountMintOwnerIdxKey,
    TokenAccountOwnerIdxKey,
};
use metrics_utils::IngesterMetricsConfig;
use num_traits::Zero;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::error;
use usecase::save_metrics::result_to_metrics;

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

#[macro_export]
macro_rules! store_assets {
    ($self:expr, $asset:expr, $db_field:ident, $metric_name:expr) => {{
        let res = $self
            .storage
            .$db_field
            .merge_with_batch(&mut $self.batch, $asset.pubkey, $asset);

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
        Self {
            storage,
            batch_size,
            batch: Default::default(),
            metrics,
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.metrics
            .set_buffer("accounts_batch_size", self.batch.len() as i64);
        let begin_processing = Instant::now();
        let res = self
            .storage
            .db
            .write(std::mem::take(&mut self.batch))
            .map_err(Into::into);

        result_to_metrics(self.metrics.clone(), &res, "accounts_batch_flush");
        self.metrics.set_latency(
            "accounts_batch_flush",
            begin_processing.elapsed().as_millis() as f64,
        );
        res
    }
    pub fn batch_filled(&self) -> bool {
        self.batch.len() >= self.batch_size
    }

    fn store_static(&mut self, asset_static: &AssetStaticDetails) -> Result<()> {
        store_assets!(
            self,
            asset_static,
            asset_static_data,
            "accounts_static_merge_with_batch"
        )
    }
    pub fn store_owner(&mut self, asset_owner: &AssetOwner) -> Result<()> {
        store_assets!(
            self,
            asset_owner,
            asset_owner_data,
            "accounts_owner_merge_with_batch"
        )
    }
    pub fn store_dynamic(&mut self, asset_dynamic: &AssetDynamicDetails) -> Result<()> {
        store_assets!(
            self,
            asset_dynamic,
            asset_dynamic_data,
            "accounts_dynamic_merge_with_batch"
        )
    }
    fn store_authority(&mut self, asset_authority: &AssetAuthority) -> Result<()> {
        store_assets!(
            self,
            asset_authority,
            asset_authority_data,
            "accounts_authority_merge_with_batch"
        )
    }
    fn store_collection(&mut self, asset_collection: &AssetCollection) -> Result<()> {
        store_assets!(
            self,
            asset_collection,
            asset_collection_data,
            "accounts_collection_merge_with_batch"
        )
    }
    fn store_metadata_mint(&mut self, metadata_mint_map: &MetadataMintMap) -> Result<()> {
        store_assets!(
            self,
            metadata_mint_map,
            metadata_mint_map,
            "metadata_mint_map_merge_with_batch"
        )
    }
    pub fn store_edition(&mut self, key: Pubkey, edition: &TokenMetadataEdition) -> Result<()> {
        self.storage
            .token_metadata_edition_cbor
            .merge_with_batch_cbor(&mut self.batch, key, edition)?;
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
            &crate::inscriptions::InscriptionData {
                pubkey: key,
                data: inscription_data.inscription_data.clone(),
                write_version: inscription_data.write_version,
            },
        )?;
        Ok(())
    }
    pub fn asset_updated_with_batch(&mut self, slot: u64, pubkey: Pubkey) -> Result<()> {
        self.storage
            .asset_updated_with_batch(&mut self.batch, slot, pubkey)?;
        Ok(())
    }
    pub fn save_token_account_with_idxs(
        &mut self,
        key: Pubkey,
        token_account: &TokenAccount,
    ) -> Result<()> {
        self.storage
            .token_accounts
            .merge_with_batch(&mut self.batch, key, token_account)?;
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

    pub fn get_authority(&self, address: Pubkey) -> Pubkey {
        self.storage
            .asset_authority_data
            .get(address)
            .unwrap_or(None)
            .map(|authority| authority.authority)
            .unwrap_or_default()
    }
    pub fn get_mint_map(&self, key: Pubkey) -> Result<Option<MetadataMintMap>> {
        self.storage.metadata_mint_map.get(key)
    }

    pub fn store_metadata_models(&mut self, metadata_models: &MetadataModels) -> Result<()> {
        let mut slot_updated = 0;
        let mut key = None;
        if let Some(asset_static) = &metadata_models.asset_static {
            self.store_static(asset_static)?;
            key = Some(asset_static.pubkey);
        }
        if let Some(asset_dynamic) = &metadata_models.asset_dynamic {
            self.store_dynamic(asset_dynamic)?;
            key = Some(asset_dynamic.pubkey);
            if asset_dynamic.get_slot_updated() > slot_updated {
                slot_updated = asset_dynamic.get_slot_updated()
            }
        }
        if let Some(asset_authority) = &metadata_models.asset_authority {
            self.store_authority(asset_authority)?;
            key = Some(asset_authority.pubkey);
        }
        if let Some(asset_collection) = &metadata_models.asset_collection {
            self.store_collection(asset_collection)?;
            key = Some(asset_collection.pubkey);
            if asset_collection.get_slot_updated() > slot_updated {
                slot_updated = asset_collection.get_slot_updated()
            }
        }
        if let Some(metadata_mint) = &metadata_models.metadata_mint {
            self.store_metadata_mint(metadata_mint)?;
        }
        if let Some(asset_owner) = &metadata_models.asset_owner {
            self.store_owner(asset_owner)?;
            key = Some(asset_owner.pubkey);
            if asset_owner.get_slot_updated() > slot_updated {
                slot_updated = asset_owner.get_slot_updated()
            }
        }

        if let Some(key) = key {
            if slot_updated == 0 {
                return Ok(());
            }
            if let Err(e) = self.asset_updated_with_batch(slot_updated, key) {
                error!("Error while updating assets update idx: {}", e);
            }
        }
        Ok(())
    }
}
