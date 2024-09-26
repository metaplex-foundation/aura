use std::sync::Arc;

use crate::api::error::DasApiError;
use entities::api_req_params::{
    GetAssetBatchV0, GetAssetV0, GetAssetsByAuthorityV0, GetAssetsByCreatorV0, GetAssetsByGroupV0,
    GetAssetsByOwnerV0, SearchAssetsV0,
};
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::types::params::Params;
use jsonrpc_core::MetaIoHandler;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use usecase::proofs::MaybeProofChecker;

use crate::api::account_balance::AccountBalanceGetterImpl;
use crate::api::meta_middleware::RpcMetaMiddleware;
use crate::api::*;
use crate::json_worker::JsonWorker;

pub struct RpcApiBuilder;

impl RpcApiBuilder {
    pub(crate) fn build(
        api: DasApi<MaybeProofChecker, JsonWorker, JsonWorker, AccountBalanceGetterImpl>,
        consistency_checkers: Vec<Arc<dyn ConsistencyChecker>>,
        tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    ) -> Result<MetaIoHandler<RpcMetaMiddleware, RpcMetaMiddleware>, DasApiError> {
        let mut module = MetaIoHandler::<RpcMetaMiddleware, RpcMetaMiddleware>::new(
            Default::default(),
            RpcMetaMiddleware::new(consistency_checkers),
        );
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
        let cloned_tasks = tasks.clone();
        module.add_method("get_asset", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetV0>() {
                    return api
                        .get_asset(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.get_asset(rpc_params.parse()?, tasks)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAsset", "get_asset");

        let cloned_api = api.clone();
        let cloned_tasks = tasks.clone();
        module.add_method("get_assets_by_owner", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByOwnerV0>() {
                    return api
                        .get_assets_by_owner(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.get_assets_by_owner(rpc_params.parse()?, tasks)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByOwner", "get_assets_by_owner");

        let cloned_api = api.clone();
        let cloned_tasks = tasks.clone();
        module.add_method("get_assets_by_creator", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByCreatorV0>() {
                    return api
                        .get_assets_by_creator(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.get_assets_by_creator(rpc_params.parse()?, tasks)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByCreator", "get_assets_by_creator");

        let cloned_api = api.clone();
        let cloned_tasks = tasks.clone();
        module.add_method("get_assets_by_authority", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByAuthorityV0>() {
                    return api
                        .get_assets_by_authority(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.get_assets_by_authority(rpc_params.parse()?, tasks)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByAuthority", "get_assets_by_authority");

        let cloned_api = api.clone();
        let cloned_tasks = tasks.clone();
        module.add_method("get_assets_by_group", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetsByGroupV0>() {
                    return api
                        .get_assets_by_group(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.get_assets_by_group(rpc_params.parse()?, tasks)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetsByGroup", "get_assets_by_group");

        let cloned_api = api.clone();
        let cloned_tasks = tasks.clone();
        module.add_method("get_asset_batch", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<GetAssetBatchV0>() {
                    return api
                        .get_asset_batch(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.get_asset_batch(rpc_params.parse()?, tasks)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getAssetBatch", "get_asset_batch");
        module.add_alias("getAssets", "get_asset_batch");
        module.add_alias("get_assets", "get_asset_batch");        

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
        module.add_alias("getAssetProofs", "get_asset_proof_batch");
        module.add_alias("get_asset_proofs", "get_asset_proof_batch");

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
        let cloned_tasks = tasks.clone();
        module.add_method("search_assets", move |rpc_params: Params| {
            let api = cloned_api.clone();
            let tasks = cloned_tasks.clone();
            async move {
                if let Ok(params) = rpc_params.clone().parse::<SearchAssetsV0>() {
                    return api
                        .search_assets(params.into(), tasks)
                        .await
                        .map_err(Into::into);
                };
                api.search_assets(rpc_params.parse()?, tasks)
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
        module.add_alias("getAssetSignatures", "get_signatures_for_asset");
        module.add_alias("get_asset_signatures", "get_signatures_for_asset");

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
        module.add_alias("getAssetSignaturesV2", "get_signatures_for_asset_v2");
        module.add_alias("get_asset_signatures_v2", "get_signatures_for_asset_v2");

        let cloned_api = api.clone();
        module.add_method("get_token_accounts", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                api.get_token_accounts(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getTokenAccounts", "get_token_accounts");

        module.add_method("get_core_fees", move |rpc_params: Params| {
            let api = api.clone();
            async move {
                api.get_core_fees(rpc_params.parse()?)
                    .await
                    .map_err(Into::into)
            }
        });
        module.add_alias("getCoreFees", "get_core_fees");

        Ok(module)
    }
}
