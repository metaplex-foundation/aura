use crate::asset::{AssetCollection, MetadataMintMap};
use crate::Result;
use crate::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails, Storage};
use entities::models::PubkeyWithSlot;
use metrics_utils::IngesterMetricsConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;
use usecase::save_metrics::result_to_metrics;

#[derive(Default, Debug)]
pub struct MetadataModels {
    pub asset_static: Vec<AssetStaticDetails>,
    pub asset_dynamic: Vec<AssetDynamicDetails>,
    pub asset_authority: Vec<AssetAuthority>,
    pub asset_owner: Vec<AssetOwner>,
    pub asset_collection: Vec<AssetCollection>,
    pub metadata_mint: Vec<MetadataMintMap>,
}

#[macro_export]
macro_rules! store_assets {
    ($self:expr, $assets:expr, $metrics:expr, $db_field:ident, $metric_name:expr) => {{
        let save_values =
            $assets
                .into_iter()
                .fold(HashMap::new(), |mut acc: HashMap<_, _>, asset| {
                    acc.insert(asset.pubkey, asset);
                    acc
                });

        let res = $self.$db_field.merge_batch(save_values).await;
        result_to_metrics($metrics, &res, $metric_name);
        res
    }};
}

impl Storage {
    async fn store_static(
        &self,
        asset_static: Vec<AssetStaticDetails>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_static,
            metrics,
            asset_static_data,
            "accounts_saving_static"
        )
    }
    async fn store_owner(
        &self,
        asset_owner: Vec<AssetOwner>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_owner,
            metrics,
            asset_owner_data,
            "accounts_saving_owner"
        )
    }
    async fn store_dynamic(
        &self,
        asset_dynamic: Vec<AssetDynamicDetails>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_dynamic,
            metrics,
            asset_dynamic_data,
            "accounts_saving_dynamic"
        )
    }
    async fn store_authority(
        &self,
        asset_authority: Vec<AssetAuthority>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_authority,
            metrics,
            asset_authority_data,
            "accounts_saving_authority"
        )
    }
    async fn store_collection(
        &self,
        asset_collection: Vec<AssetCollection>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            asset_collection,
            metrics,
            asset_collection_data,
            "accounts_saving_collection"
        )
    }
    async fn store_metadata_mint(
        &self,
        metadata_mint_map: Vec<MetadataMintMap>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Result<()> {
        store_assets!(
            self,
            metadata_mint_map,
            metrics,
            metadata_mint_map,
            "metadata_mint_map"
        )
    }
    pub async fn store_metadata_models(
        &self,
        metadata_models: &MetadataModels,
        metrics: Arc<IngesterMetricsConfig>,
    ) {
        let _ = tokio::join!(
            self.store_static(metadata_models.asset_static.clone(), metrics.clone()),
            self.store_dynamic(metadata_models.asset_dynamic.clone(), metrics.clone()),
            self.store_authority(metadata_models.asset_authority.clone(), metrics.clone()),
            self.store_collection(metadata_models.asset_collection.clone(), metrics.clone()),
            self.store_metadata_mint(metadata_models.metadata_mint.clone(), metrics.clone()),
            self.store_owner(metadata_models.asset_owner.clone(), metrics.clone())
        );

        if let Err(e) = self.asset_updated_batch(
            metadata_models
                .asset_dynamic
                .iter()
                .map(|asset| PubkeyWithSlot {
                    slot: asset.get_slot_updated(),
                    pubkey: asset.pubkey,
                })
                .collect(),
        ) {
            error!("Error while updating assets update idx: {}", e);
        }
    }
}
