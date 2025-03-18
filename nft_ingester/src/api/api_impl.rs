use std::{sync::Arc, time::Instant};

use dapi::{
    get_asset, get_asset_batch, get_asset_signatures::get_asset_signatures,
    get_core_fees::get_core_fees, get_proof_for_assets, get_token_accounts::get_token_accounts,
    search_assets,
};
use entities::{
    api_req_params::{
        DisplayOptions, GetAsset, GetAssetBatch, GetAssetProof, GetAssetProofBatch,
        GetAssetSignatures, GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup,
        GetAssetsByOwner, GetCoreFees, GetGrouping, GetNftEditions, GetTokenAccounts, Pagination,
        PaginationQuery, SearchAssets,
    },
    enums::TokenType,
};
use interface::{
    account_balance::AccountBalanceGetter,
    error::UsecaseError,
    json::{JsonDownloader, JsonPersister},
    price_fetcher::TokenPriceFetcher,
    processing_possibility::ProcessingPossibilityChecker,
    proofs::ProofChecker,
};
use metrics_utils::ApiMetricsConfig;
use postgre_client::PgClient;
use rocks_db::Storage;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use usecase::validation::{validate_opt_pubkey, validate_pubkey};

use self::util::ApiRequest;
use crate::{
    api::{
        dapi::{
            converters::SearchAssetsQuery,
            response::{
                AssetList, Check, GetGroupingResponse, HealthCheckResponse,
                MasterAssetEditionsInfoResponse, Status, TransactionSignatureListDeprecated,
            },
            rpc_asset_models::Asset,
        },
        error::DasApiError,
        *,
    },
    config::{HealthCheckInfo, JsonMiddlewareConfig},
};

const MAX_ITEMS_IN_BATCH_REQ: usize = 1000;
const DEFAULT_LIMIT: usize = MAX_ITEMS_IN_BATCH_REQ;

pub struct DasApi<PC, JD, JP, ABG, TPF, PPC>
where
    PC: ProofChecker + Sync + Send + 'static,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    ABG: AccountBalanceGetter + Sync + Send + 'static,
    TPF: TokenPriceFetcher + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
{
    pub(crate) pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    health_check_info: HealthCheckInfo,
    metrics: Arc<ApiMetricsConfig>,
    proof_checker: Option<Arc<PC>>,
    tree_gaps_checker: Option<Arc<PPC>>,
    max_page_limit: u32,
    json_downloader: Option<Arc<JD>>,
    json_persister: Option<Arc<JP>>,
    json_middleware_config: JsonMiddlewareConfig,
    account_balance_getter: Arc<ABG>,
    /// E.g. https://storage-service.xyz/
    storage_service_base_path: Option<String>,
    token_price_fetcher: Arc<TPF>,
    native_mint_pubkey: String,
}

pub fn not_found() -> DasApiError {
    DasApiError::NoDataFoundError
}

impl<PC, JD, JP, ABG, TPF, PPC> DasApi<PC, JD, JP, ABG, TPF, PPC>
where
    PC: ProofChecker + Sync + Send + 'static,
    JD: JsonDownloader + Sync + Send + 'static,
    JP: JsonPersister + Sync + Send + 'static,
    ABG: AccountBalanceGetter + Sync + Send + 'static,
    TPF: TokenPriceFetcher + Sync + Send + 'static,
    PPC: ProcessingPossibilityChecker + Sync + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        health_check_info: HealthCheckInfo,
        metrics: Arc<ApiMetricsConfig>,
        proof_checker: Option<Arc<PC>>,
        tree_gaps_checker: Option<Arc<PPC>>,
        max_page_limit: usize,
        json_downloader: Option<Arc<JD>>,
        json_persister: Option<Arc<JP>>,
        json_middleware_config: JsonMiddlewareConfig,
        account_balance_getter: Arc<ABG>,
        storage_service_base_path: Option<String>,
        token_price_fetcher: Arc<TPF>,
        native_mint_pubkey: String,
    ) -> Self {
        DasApi {
            pg_client,
            rocks_db,
            health_check_info,
            metrics,
            proof_checker,
            tree_gaps_checker,
            max_page_limit: max_page_limit as u32,
            json_downloader,
            json_persister,
            json_middleware_config,
            account_balance_getter,
            storage_service_base_path,
            token_price_fetcher,
            native_mint_pubkey,
        }
    }

    pub async fn check_health(&self) -> Result<Value, DasApiError> {
        let label = "check_health";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();
        let mut overall_status = Status::Ok;

        //List of Checks
        let mut check = Check::new("PostgresDB".to_string());

        if self.pg_client.check_health().await.is_err() {
            overall_status = Status::Unhealthy;
            check.status = Status::Unhealthy;
            check.description = Some(DasApiError::InternalDbError.to_string());
        }

        let response = HealthCheckResponse {
            status: overall_status,
            app_version: self.health_check_info.app_version.clone(),
            node_name: self.health_check_info.node_name.clone(),
            checks: vec![check],
            image_info: self.health_check_info.image_info.clone(),
        };

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(response))
    }

    fn validate_options(
        options: &DisplayOptions,
        query: &SearchAssetsQuery,
    ) -> Result<(), DasApiError> {
        if options.show_native_balance && query.owner_address.is_none() {
            return Err(DasApiError::MissingOwnerAddress);
        }
        if query.owner_address.is_none() && query.token_type.is_some() {
            return Err(DasApiError::Validation(
                "Must provide `owner_address` when using `token_type` field".to_string(),
            ));
        }
        if let Some(ref token_type) = query.token_type {
            if token_type == &TokenType::All || token_type == &TokenType::Fungible {
                if let Some(banned_param) = Self::banned_params_with_fungible_token_type(query) {
                    return Err(DasApiError::Validation(format!(
                        "'{banned_param}' is not supported for this `token_type`"
                    )));
                }
            }
        }

        Ok(())
    }

    fn banned_params_with_fungible_token_type(query: &SearchAssetsQuery) -> Option<String> {
        if query.creator_address.is_some() {
            return Some(String::from("creator_address"));
        }
        if query.creator_verified.is_some() {
            return Some(String::from("creator_verified"));
        }
        if query.grouping.is_some() {
            return Some(String::from("grouping"));
        }
        if query.owner_type.is_some() {
            return Some(String::from("owner_type"));
        }
        if query.specification_asset_class.is_some() {
            return Some(String::from("specification_asset_class"));
        }
        if query.compressed.is_some() {
            return Some(String::from("compressed"));
        }
        if query.compressible.is_some() {
            return Some(String::from("compressible"));
        }
        if query.specification_version.is_some() {
            return Some(String::from("specification_version"));
        }
        if query.authority_address.is_some() {
            return Some(String::from("authority_address"));
        }
        if query.delegate.is_some() {
            return Some(String::from("delegate"));
        }
        if query.frozen.is_some() {
            return Some(String::from("frozen"));
        }
        if query.supply.is_some() {
            return Some(String::from("supply"));
        }
        if query.supply_mint.is_some() {
            return Some(String::from("supply_mint"));
        }
        if query.royalty_target_type.is_some() {
            return Some(String::from("royalty_target_type"));
        }
        if query.royalty_target.is_some() {
            return Some(String::from("royalty_target"));
        }
        if query.royalty_amount.is_some() {
            return Some(String::from("royalty_amount"));
        }
        if query.burnt.is_some() {
            return Some(String::from("burnt"));
        }
        if query.json_uri.is_some() {
            return Some(String::from("json_uri"));
        }
        None
    }

    pub fn validate_basic_pagination(
        parameters: &Pagination,
        max_page_limit: u32,
    ) -> Result<(), DasApiError> {
        if let Some(limit) = parameters.limit {
            // make config item
            if limit > MAX_ITEMS_IN_BATCH_REQ as u32 {
                return Err(DasApiError::PaginationError);
            }
        }
        if (parameters.before.is_some() || parameters.after.is_some())
            && parameters.cursor.is_some()
        {
            return Err(DasApiError::PaginationError);
        }
        if let Some(page) = parameters.page {
            if page == 0 {
                return Err(DasApiError::PaginationEmptyError);
            }
            if page > max_page_limit {
                return Err(DasApiError::PageTooBig(max_page_limit as usize));
            }
            // make config item
            if parameters.before.is_some()
                || parameters.after.is_some()
                || parameters.cursor.is_some()
            {
                return Err(DasApiError::PaginationError);
            }
        }
        Ok(())
    }

    pub async fn get_asset_proof(&self, payload: GetAssetProof) -> Result<Value, DasApiError> {
        let label = "get_asset_proof";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let id = validate_pubkey(payload.id.clone())?;
        let assets = get_proof_for_assets(
            self.rocks_db.clone(),
            vec![id],
            self.proof_checker.clone(),
            &self.tree_gaps_checker,
            self.metrics.clone(),
        )
        .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        let res = assets
            .get(&id.to_string())
            .ok_or(DasApiError::ProofNotFound)?
            .as_ref()
            .ok_or::<DasApiError>(not_found())
            .cloned();

        Ok(json!(res?))
    }

    pub async fn get_asset_proof_batch(
        &self,
        payload: GetAssetProofBatch,
    ) -> Result<Value, DasApiError> {
        let label = "get_asset_proof_batch";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        if payload.ids.len() > MAX_ITEMS_IN_BATCH_REQ {
            return Err(DasApiError::BatchSizeError(MAX_ITEMS_IN_BATCH_REQ));
        }

        let ids: Vec<Pubkey> =
            payload.ids.into_iter().map(validate_pubkey).collect::<Result<Vec<_>, _>>()?;

        let res = get_proof_for_assets(
            self.rocks_db.clone(),
            ids,
            self.proof_checker.clone(),
            &self.tree_gaps_checker,
            self.metrics.clone(),
        )
        .await;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res?))
    }

    pub async fn get_asset(&self, payload: GetAsset) -> Result<Value, DasApiError> {
        let label = "get_asset";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let id = validate_pubkey(payload.id.clone())?;
        let options = payload.options.unwrap_or_default();

        let res = get_asset(
            self.rocks_db.clone(),
            id,
            options,
            self.json_downloader.clone(),
            self.json_persister.clone(),
            self.json_middleware_config.max_urls_to_parse,
            self.storage_service_base_path.clone(),
            self.token_price_fetcher.clone(),
            self.metrics.clone(),
            &self.tree_gaps_checker,
        )
        .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        if res.is_none() {
            return Err(not_found());
        }

        Ok(json!(res))
    }

    pub async fn get_asset_batch(&self, payload: GetAssetBatch) -> Result<Value, DasApiError> {
        let label = "get_asset_batch";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        if payload.ids.len() > MAX_ITEMS_IN_BATCH_REQ {
            return Err(DasApiError::BatchSizeError(MAX_ITEMS_IN_BATCH_REQ));
        }
        if payload.ids.is_empty() {
            return Ok(json!(Vec::<Option<Asset>>::new()));
        }

        let ids: Vec<Pubkey> =
            payload.ids.into_iter().map(validate_pubkey).collect::<Result<Vec<_>, _>>()?;
        let options = payload.options.unwrap_or_default();

        let res = get_asset_batch(
            self.rocks_db.clone(),
            ids,
            options,
            self.json_downloader.clone(),
            self.json_persister.clone(),
            self.json_middleware_config.max_urls_to_parse,
            self.storage_service_base_path.clone(),
            self.token_price_fetcher.clone(),
            self.metrics.clone(),
            &self.tree_gaps_checker,
        )
        .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_owner(
        &self,
        payload: GetAssetsByOwner,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_owner";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let res =
            self.process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_nft_editions(&self, payload: GetNftEditions) -> Result<Value, DasApiError> {
        let label = "get_nft_editions";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let pagination = Pagination {
            limit: payload.limit.unwrap_or(DEFAULT_LIMIT as u32).into(),
            page: payload.page,
            before: payload.before,
            after: payload.after,
            cursor: payload.cursor,
        };
        Self::validate_basic_pagination(&pagination, self.max_page_limit)?;
        let mint_address = validate_pubkey(payload.mint)?;

        if pagination.after.is_some() {
            pagination.after.clone().unwrap().parse::<u64>().map_err(|_| {
                DasApiError::Validation(
                    "Failed to parse edition after key. It should be a number.".to_string(),
                )
            })?;
        }
        if pagination.before.is_some() {
            pagination.before.clone().unwrap().parse::<u64>().map_err(|_| {
                DasApiError::Validation(
                    "Failed to parse edition before key. It should be a number.".to_string(),
                )
            })?;
        }

        let pagination_query = PaginationQuery::build(pagination.clone()).map_err(|_| {
            DasApiError::Validation("Failed to build pagination object.".to_string())
        })?;
        let master_asset_editions_info = self
            .rocks_db
            .get_master_edition_child_assets_info(mint_address, pagination_query)
            .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(MasterAssetEditionsInfoResponse::from((master_asset_editions_info, pagination))))
    }

    pub async fn get_assets_by_group(
        &self,
        payload: GetAssetsByGroup,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_group";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let res =
            self.process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_creator(
        &self,
        payload: GetAssetsByCreator,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_creator";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let res =
            self.process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_authority(
        &self,
        payload: GetAssetsByAuthority,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_authority";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let res =
            self.process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_token_accounts(
        &self,
        payload: GetTokenAccounts,
    ) -> Result<Value, DasApiError> {
        let label = "get_token_accounts";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetTokenAccounts {
            limit,
            page,
            owner_address: owner,
            mint_address: mint,
            options,
            before,
            after,
            cursor,
        } = payload;

        let pagination = Pagination { limit, page, before, after, cursor };

        let owner = validate_opt_pubkey(&owner)?;
        let mint = validate_opt_pubkey(&mint)?;

        // todo: a hack to return more pages where the before/after pagination is not implemented
        Self::validate_basic_pagination(&pagination, self.max_page_limit * 10)?;
        if owner.is_none() && mint.is_none() {
            return Err(DasApiError::Validation(
                "Must provide either 'owner' or 'mint'".to_string(),
            ));
        }

        let res = get_token_accounts(
            self.rocks_db.clone(),
            owner,
            mint,
            limit.unwrap_or(DEFAULT_LIMIT as u32).into(),
            page.map(|page| page as u64),
            pagination.before,
            pagination.after,
            pagination.cursor,
            options.map(|op| op.show_zero_balance).unwrap_or_default(),
        )
        .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_core_fees(&self, payload: GetCoreFees) -> Result<Value, DasApiError> {
        let label = "get_core_fees";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetCoreFees { limit, page, before, after, cursor } = payload;

        let pagination = Pagination { limit, page, before, after, cursor };

        Self::validate_basic_pagination(&pagination, self.max_page_limit)?;
        let res = get_core_fees(
            self.pg_client.clone(),
            limit.unwrap_or(DEFAULT_LIMIT as u32).into(),
            page.map(|page| page as u64),
            pagination.before,
            pagination.after,
            pagination.cursor,
        )
        .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn search_assets(&self, payload: SearchAssets) -> Result<Value, DasApiError> {
        // use names of the filter fields as a label for better understanding of the endpoint usage
        let label = payload.extract_some_fields();
        self.metrics.inc_search_asset_requests(&label);
        let latency_timer = Instant::now();

        let res =
            self.process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics.set_search_asset_latency(&label, latency_timer.elapsed().as_millis() as f64);
        self.metrics.set_latency("search_asset", latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_asset_signatures(
        &self,
        payload: GetAssetSignatures,
        is_deprecated: bool,
    ) -> Result<Value, DasApiError> {
        let label = "get_signatures_for_asset";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetAssetSignatures {
            id,
            limit,
            page,
            before,
            after,
            tree,
            leaf_index,
            sort_direction,
            cursor,
        } = payload;

        if !((id.is_some() && tree.is_none() && leaf_index.is_none())
            || (id.is_none() && tree.is_some() && leaf_index.is_some()))
        {
            return Err(DasApiError::Validation(
                "Must provide either 'id' or both 'tree' and 'leafIndex'".to_string(),
            ));
        }
        let id = validate_opt_pubkey(&id)?;
        let tree = validate_opt_pubkey(&tree)?;

        let pagination = Pagination {
            limit,
            page,
            before: before.clone(),
            after: after.clone(),
            cursor: cursor.clone(),
        };

        Self::validate_basic_pagination(&pagination, self.max_page_limit)?;

        let res = get_asset_signatures(
            self.rocks_db.clone(),
            id,
            tree,
            leaf_index,
            sort_direction,
            limit.unwrap_or(DEFAULT_LIMIT as u32).into(),
            page.map(|page| page as u64),
            before,
            if cursor.is_some() { cursor } else { after },
        )
        .await?;

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        if is_deprecated {
            return Ok(json!(TransactionSignatureListDeprecated::from(res)));
        }
        Ok(json!(res))
    }

    pub async fn get_grouping(&self, payload: GetGrouping) -> Result<Value, DasApiError> {
        let label = "get_grouping";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetGrouping { group_key, group_value } = payload;
        let group_value_pubkey = validate_pubkey(group_value.clone())?;

        let gs =
            self.pg_client.get_collection_size(group_value_pubkey.to_bytes().as_slice()).await?;

        let res = GetGroupingResponse { group_key, group_name: group_value, group_size: gs };

        self.metrics.set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    async fn process_request<T>(
        &self,
        pg_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
        rocks_db: Arc<Storage>,
        payload: T,
    ) -> Result<AssetList, DasApiError>
    where
        T: TryInto<SearchAssetsQuery>,
        T: ApiRequest,
        T::Error: Into<UsecaseError>,
    {
        let pagination = payload.get_all_pagination_parameters();
        let sort_by = payload.get_sort_parameter().unwrap_or_default();
        let options = payload.get_options();

        let query: SearchAssetsQuery =
            payload.try_into().map_err(|e| DasApiError::from(e.into()))?;

        Self::validate_basic_pagination(&pagination, self.max_page_limit)?;
        Self::validate_options(&options, &query)?;

        let res = search_assets(
            pg_client,
            rocks_db,
            query,
            sort_by,
            pagination.limit.map_or(DEFAULT_LIMIT as u64, |l| l as u64),
            pagination.page.map(|p| p as u64),
            pagination.before,
            pagination.after,
            pagination.cursor,
            options,
            self.json_downloader.clone(),
            self.json_persister.clone(),
            self.json_middleware_config.max_urls_to_parse,
            self.account_balance_getter.clone(),
            self.storage_service_base_path.clone(),
            self.token_price_fetcher.clone(),
            self.metrics.clone(),
            &self.tree_gaps_checker,
            &self.native_mint_pubkey,
        )
        .await?;

        Ok(res)
    }
}
