use crate::asset::{AssetCollection, MetadataMintMap};
use crate::Result;
use crate::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use metrics_utils::IngesterMetricsConfig;
use std::sync::Arc;
use tracing::error;
use usecase::save_metrics::result_to_metrics;

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
    ($self:expr, $asset:expr, $db_batch:expr, $metrics:expr, $db_field:ident, $metric_name:expr) => {{
        let res = $self
            .$db_field
            .merge_with_batch($db_batch, $asset.pubkey, $asset);
        result_to_metrics($metrics, &res, $metric_name);
        res
    }};
}

impl Storage {
    fn store_static(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        asset_static: &AssetStaticDetails,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_static,
            db_batch,
            metrics,
            asset_static_data,
            "accounts_saving_static"
        )
    }
    fn store_owner(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        asset_owner: &AssetOwner,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_owner,
            db_batch,
            metrics,
            asset_owner_data,
            "accounts_merge_with_batch_owner"
        )
    }
    pub fn store_dynamic(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        asset_dynamic: &AssetDynamicDetails,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_dynamic,
            db_batch,
            metrics,
            asset_dynamic_data,
            "accounts_merge_with_batch_dynamic"
        )
    }
    fn store_authority(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        asset_authority: &AssetAuthority,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_authority,
            db_batch,
            metrics,
            asset_authority_data,
            "accounts_merge_with_batch_authority"
        )
    }
    fn store_collection(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        asset_collection: &AssetCollection,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_collection,
            db_batch,
            metrics,
            asset_collection_data,
            "accounts_merge_with_batch_collection"
        )
    }
    fn store_metadata_mint(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        metadata_mint_map: &MetadataMintMap,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            metadata_mint_map,
            db_batch,
            metrics,
            metadata_mint_map,
            "metadata_mint_map_merge_with_batch"
        )
    }
    pub fn store_metadata_models(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        metadata_models: &MetadataModels,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        let mut slot_updated = 0;
        let mut key = None;
        if let Some(asset_static) = &metadata_models.asset_static {
            self.store_static(db_batch, asset_static, metrics.clone())?;
            key = Some(asset_static.pubkey);
        }
        if let Some(asset_dynamic) = &metadata_models.asset_dynamic {
            self.store_dynamic(db_batch, asset_dynamic, metrics.clone())?;
            key = Some(asset_dynamic.pubkey);
            if asset_dynamic.get_slot_updated() > slot_updated {
                slot_updated = asset_dynamic.get_slot_updated()
            }
        }
        if let Some(asset_authority) = &metadata_models.asset_authority {
            self.store_authority(db_batch, asset_authority, metrics.clone())?;
            key = Some(asset_authority.pubkey);
        }
        if let Some(asset_collection) = &metadata_models.asset_collection {
            self.store_collection(db_batch, asset_collection, metrics.clone())?;
            key = Some(asset_collection.pubkey);
            if asset_collection.get_slot_updated() > slot_updated {
                slot_updated = asset_collection.get_slot_updated()
            }
        }
        if let Some(metadata_mint) = &metadata_models.metadata_mint {
            self.store_metadata_mint(db_batch, metadata_mint, metrics.clone())?;
        }
        if let Some(asset_owner) = &metadata_models.asset_owner {
            self.store_owner(db_batch, asset_owner, metrics.clone())?;
            key = Some(asset_owner.pubkey);
            if asset_owner.get_slot_updated() > slot_updated {
                slot_updated = asset_owner.get_slot_updated()
            }
        }

        if let Some(key) = key {
            if slot_updated == 0 {
                return Ok(());
            }
            if let Err(e) = self.asset_updated_with_batch(db_batch, slot_updated, key) {
                error!("Error while updating assets update idx: {}", e);
            }
        }
        Ok(())
    }
}
