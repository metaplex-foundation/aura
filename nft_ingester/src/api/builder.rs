use std::sync::Arc;

use jsonrpc_core::types::params::Params;
use jsonrpc_core::{futures::prelude::*, IoHandler};

use crate::api::*;

pub struct RpcApiBuilder;

impl RpcApiBuilder {
    pub fn build(api: DasApi) -> Result<IoHandler, DasApiError> {
        let mut module = IoHandler::default();
        let api = Arc::new(api);

        let cloned_api = api.clone();
        module.add_method("health", move |_rpc_params: Params| {
            let api = cloned_api.clone();
            async move { api.check_health().await.map_err(Into::into) }
        });

        let cloned_api = api.clone();
        module.add_method("get_asset_proof", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_asset_proof(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetProof", "get_asset_proof");

        let cloned_api = api.clone();
        module.add_method("get_asset", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move { api.get_asset(rpc_params.parse()?).await.map_err(Into::into) }
        });
        module.add_alias("getAsset", "get_asset");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_owner", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_assets_by_owner(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByOwner", "get_assets_by_owner");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_creator", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_assets_by_creator(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByCreator", "get_assets_by_creator");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_authority", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_assets_by_authority(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByAuthority", "get_assets_by_authority");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_group", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_assets_by_group(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByGroup", "get_assets_by_group");

        let cloned_api = api.clone();
        module.add_method("get_asset_batch", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_asset_batch(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetBatch", "get_asset_batch");

        let cloned_api = api.clone();
        module.add_method("get_asset_proof_batch", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_asset_proof_batch(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetProofBatch", "get_asset_proof_batch");

        let cloned_api = api.clone();
        module.add_method("get_grouping", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_grouping(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getGrouping", "get_grouping");

        Ok(module)
    }
}
