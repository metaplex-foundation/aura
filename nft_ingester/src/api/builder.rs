use std::sync::Arc;

use entities::api_req_params::{
    GetAsset, GetAssetBatch, GetAssetBatchV0, GetAssetV0, GetAssetsByAuthority,
    GetAssetsByAuthorityV0, GetAssetsByCreator, GetAssetsByCreatorV0, GetAssetsByGroup,
    GetAssetsByGroupV0, GetAssetsByOwner, GetAssetsByOwnerV0, GetNftEditions, SearchAssets,
    SearchAssetsV0,
};
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::{types::params::Params, MetaIoHandler};
use rocks_db::Storage;
use usecase::proofs::MaybeProofChecker;

use crate::{
    api::{
        account_balance::AccountBalanceGetterImpl, error::DasApiError,
        meta_middleware::RpcMetaMiddleware, *,
    },
    json_worker::JsonWorker,
    raydium_price_fetcher::RaydiumTokenPriceFetcher,
};

pub struct RpcApiBuilder;

impl RpcApiBuilder {
    pub(crate) fn build(
        api: DasApi<
            MaybeProofChecker,
            JsonWorker,
            JsonWorker,
            AccountBalanceGetterImpl,
            RaydiumTokenPriceFetcher,
            Storage,
        >,
        consistency_checkers: Vec<Arc<dyn ConsistencyChecker>>,
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
        module.add_alias("getAuraHealth", "health");

        let cloned_api = api.clone();
        module.add_method("get_asset_proof", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move { api.get_asset_proof(rpc_params.parse()?).await.map_err(Into::into) }
        });
        module.add_alias("getAssetProof", "get_asset_proof");

        let cloned_api = api.clone();
        module.add_method("get_asset", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetAsset>() {
                    Ok(payload) => api.get_asset(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_asset(rpc_params.parse::<GetAssetV0>()?.into())
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getAsset", "get_asset");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_owner", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetAssetsByOwner>() {
                    Ok(payload) => api.get_assets_by_owner(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_assets_by_owner(rpc_params.parse::<GetAssetsByOwnerV0>()?.into())
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getAssetsByOwner", "get_assets_by_owner");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_creator", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetAssetsByCreator>() {
                    Ok(payload) => api.get_assets_by_creator(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_assets_by_creator(rpc_params.parse::<GetAssetsByCreatorV0>()?.into())
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getAssetsByCreator", "get_assets_by_creator");

        let cloned_api = api.clone();
        module.add_method("get_nft_editions", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetNftEditions>() {
                    Ok(payload) => api.get_nft_editions(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_nft_editions(rpc_params.parse::<GetNftEditions>()?)
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getNftEditions", "get_nft_editions");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_authority", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetAssetsByAuthority>() {
                    Ok(payload) => api.get_assets_by_authority(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_assets_by_authority(
                            rpc_params.parse::<GetAssetsByAuthorityV0>()?.into(),
                        )
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getAssetsByAuthority", "get_assets_by_authority");

        let cloned_api = api.clone();
        module.add_method("get_assets_by_group", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetAssetsByGroup>() {
                    Ok(payload) => api.get_assets_by_group(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_assets_by_group(rpc_params.parse::<GetAssetsByGroupV0>()?.into())
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getAssetsByGroup", "get_assets_by_group");

        let cloned_api = api.clone();
        module.add_method("get_asset_batch", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<GetAssetBatch>() {
                    Ok(payload) => api.get_asset_batch(payload).await.map_err(Into::into),
                    Err(_) => api
                        .get_asset_batch(rpc_params.parse::<GetAssetBatchV0>()?.into())
                        .await
                        .map_err(Into::into),
                }
            }
        });
        module.add_alias("getAssetBatch", "get_asset_batch");
        module.add_alias("getAssets", "get_asset_batch");
        module.add_alias("get_assets", "get_asset_batch");

        let cloned_api = api.clone();
        module.add_method("get_asset_proof_batch", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move { api.get_asset_proof_batch(rpc_params.parse()?).await.map_err(Into::into) }
        });
        module.add_alias("getAssetProofBatch", "get_asset_proof_batch");
        module.add_alias("getAssetProofs", "get_asset_proof_batch");
        module.add_alias("get_asset_proofs", "get_asset_proof_batch");

        let cloned_api = api.clone();
        module.add_method("get_grouping", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move { api.get_grouping(rpc_params.parse()?).await.map_err(Into::into) }
        });
        module.add_alias("getGrouping", "get_grouping");

        let cloned_api = api.clone();
        module.add_method("search_assets", move |rpc_params: Params| {
            let api = cloned_api.clone();
            async move {
                match rpc_params.clone().parse::<SearchAssets>() {
                    Ok(payload) => api.search_assets(payload).await.map_err(Into::into),
                    Err(_) => api
                        .search_assets(rpc_params.parse::<SearchAssetsV0>()?.into())
                        .await
                        .map_err(Into::into),
                }
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
            async move { api.get_token_accounts(rpc_params.parse()?).await.map_err(Into::into) }
        });
        module.add_alias("getTokenAccounts", "get_token_accounts");

        module.add_method("get_core_fees", move |rpc_params: Params| {
            let api = api.clone();
            async move { api.get_core_fees(rpc_params.parse()?).await.map_err(Into::into) }
        });
        module.add_alias("getCoreFees", "get_core_fees");

        Ok(module)
    }
}
