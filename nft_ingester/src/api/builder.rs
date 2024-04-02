use std::sync::Arc;

use entities::api_req_params::{
    GetAssetBatchV0, GetAssetV0, GetAssetsByAuthorityV0, GetAssetsByCreatorV0, GetAssetsByGroupV0,
    GetAssetsByOwnerV0, SearchAssetsV0,
};
use jsonrpc_core::types::params::Params;
use jsonrpc_core::IoHandler;
use usecase::proofs::MaybeProofChecker;

use crate::api::*;

use self::middleware::JsonDownloaderMiddleware;

pub struct RpcApiBuilder;

impl RpcApiBuilder {
    pub fn build(
        api: DasApi<MaybeProofChecker, JsonDownloaderMiddleware>,
    ) -> Result<IoHandler, DasApiError> {
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
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetV0>() {
                    return api.get_asset(params.into()).await.map_err(Into::into);
                };
                api.get_asset(rpc_params.parse()?).await.map_err(Into::into)
            }
        });
        module.add_alias("getAsset", "get_asset");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_owner", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByOwnerV0>() {
                    return api
                        .get_assets_by_owner(params.into())
                        .await
                        .map_err(Into::into);
                };
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
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByCreatorV0>() {
                    return api
                        .get_assets_by_creator(params.into())
                        .await
                        .map_err(Into::into);
                };
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
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByAuthorityV0>() {
                    return api
                        .get_assets_by_authority(params.into())
                        .await
                        .map_err(Into::into);
                };
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
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByGroupV0>() {
                    return api
                        .get_assets_by_group(params.into())
                        .await
                        .map_err(Into::into);
                };
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
                if let Ok(params) = rpc_params.clone().parse::<GetAssetBatchV0>() {
                    return api.get_asset_batch(params.into()).await.map_err(Into::into);
                };
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

        let cloned_api = api.clone();
        module.add_method("search_assets", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<SearchAssetsV0>() {
                    return api.search_assets(params.into()).await.map_err(Into::into);
                };
                api.search_assets(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("searchAssets", "search_assets");

        let cloned_api = api.clone();
        module.add_method("get_signatures_for_asset", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_asset_signatures(rpc_params.parse()?, true)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getSignaturesForAsset", "get_signatures_for_asset");

        let cloned_api = api.clone();
        module.add_method("get_signatures_for_asset_v2", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_asset_signatures(rpc_params.parse()?, false)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getSignaturesForAssetV2", "get_signatures_for_asset_v2");

        module.add_method("get_token_accounts", move |rpc_params: Params| {
            let api = api.clone();
            async move {
                api.get_token_accounts(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getTokenAccounts", "get_token_accounts");

        Ok(module)
    }
}
