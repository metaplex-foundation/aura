use digital_asset_types::dao::scopes::asset::get_grouping;
use digital_asset_types::dao::SearchAssetsQuery;
use digital_asset_types::dapi::common::complement_asset_content_with_json_data;
use digital_asset_types::dapi::{get_asset, get_asset_batch, get_proof_for_assets, search_assets};
use digital_asset_types::rpc::response::AssetList;
use interface::api::APIJsonDownloaderMiddleware;
use interface::error::UsecaseError;
use interface::proofs::ProofChecker;
use log::debug;
use metrics_utils::red::RequestErrorDurationMetrics;
use postgre_client::PgClient;
use std::collections::HashSet;
use std::{sync::Arc, time::Instant};

use self::util::ApiRequest;
use crate::api::config::Config;
use digital_asset_types::dapi::get_asset_signatures::get_asset_signatures;
use digital_asset_types::dapi::get_token_accounts::get_token_accounts;
use digital_asset_types::rpc::response::TransactionSignatureListDeprecated;
use digital_asset_types::rpc::Asset;
use entities::api_req_params::{
    GetAsset, GetAssetBatch, GetAssetProof, GetAssetProofBatch, GetAssetSignatures,
    GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner, GetGrouping,
    GetTokenAccounts, Pagination, SearchAssets,
};
use metrics_utils::ApiMetricsConfig;
use rocks_db::Storage;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use usecase::validation::{validate_opt_pubkey, validate_pubkey};
use {
    crate::api::*,
    sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, SqlxPostgresConnector, Statement},
    sqlx::postgres::PgPoolOptions,
};

const MAX_ITEMS_IN_BATCH_REQ: usize = 1000;
const DEFAULT_LIMIT: usize = MAX_ITEMS_IN_BATCH_REQ;

pub struct DasApi<PC, J>
where
    PC: ProofChecker + Sync + Send + 'static,
    J: APIJsonDownloaderMiddleware,
{
    db_connection: DatabaseConnection,
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    metrics: Arc<ApiMetricsConfig>,
    proof_checker: Option<Arc<PC>>,
    max_page_limit: u32,
    json_downloader: Option<Arc<J>>,
}

impl<PC, J> DasApi<PC, J>
where
    PC: ProofChecker + Sync + Send + 'static,
    J: APIJsonDownloaderMiddleware,
{
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<ApiMetricsConfig>,
        proof_checker: Option<Arc<PC>>,
        max_page_limit: usize,
        json_downloader: Option<Arc<J>>,
    ) -> Self {
        let db_connection = SqlxPostgresConnector::from_sqlx_postgres_pool(pg_client.pool.clone());

        DasApi {
            db_connection,
            pg_client,
            rocks_db,
            metrics,
            proof_checker,
            max_page_limit: max_page_limit as u32,
            json_downloader,
        }
    }

    pub async fn from_config(
        config: Config,
        metrics: Arc<ApiMetricsConfig>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
        rocks_db: Arc<Storage>,
        proof_checker: Option<Arc<PC>>,
        json_downloader: Option<Arc<J>>,
    ) -> Result<Self, DasApiError> {
        let pool = PgPoolOptions::new()
            .max_connections(250)
            .connect(&config.database_url)
            .await?;

        let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
        let pg_client = PgClient::new_with_pool(pool, red_metrics);
        Ok(DasApi {
            db_connection: conn,
            pg_client: Arc::new(pg_client),
            rocks_db,
            metrics,
            proof_checker,
            max_page_limit: config.max_page_limit as u32,
            json_downloader,
        })
    }
}

pub fn not_found() -> DasApiError {
    DasApiError::NoDataFoundError
}

impl<PC, J> DasApi<PC, J>
where
    PC: ProofChecker + Sync + Send + 'static,
    J: APIJsonDownloaderMiddleware,
{
    pub async fn check_health(&self) -> Result<Value, DasApiError> {
        let label = "check_health";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        self.db_connection
            .execute(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 1".to_string(),
            ))
            .await?;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!("ok"))
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
            self.metrics.clone(),
        )
        .await?;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        let res = assets
            .get(&id.to_string())
            .ok_or(DasApiError::ProofNotFound)?
            .as_ref()
            .ok_or::<DasApiError>(not_found())
            .cloned()
            .map_err(Into::<DasApiError>::into);

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

        let ids: Vec<Pubkey> = payload
            .ids
            .into_iter()
            .map(validate_pubkey)
            .collect::<Result<Vec<_>, _>>()?;

        let res = get_proof_for_assets(
            self.rocks_db.clone(),
            ids,
            self.proof_checker.clone(),
            self.metrics.clone(),
        )
        .await
        .map_err(Into::<DasApiError>::into);

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res?))
    }

    pub async fn get_asset(&self, payload: GetAsset) -> Result<Value, DasApiError> {
        let label = "get_asset";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let id = validate_pubkey(payload.id.clone())?;
        let options = payload.options.unwrap_or_default();

        let res = match get_asset(self.rocks_db.clone(), id, options).await {
            Ok(Some(asset)) => {
                if let Some(json_downloader) = &self.json_downloader {
                    let mut a_v = vec![asset];
                    Self::check_and_supplement_jsons(json_downloader.clone(), &mut a_v).await;
                    Some(a_v[0].clone())
                } else {
                    Some(asset)
                }
            }
            Ok(None) => None,
            Err(e) => return Err(Into::<DasApiError>::into(e)),
        };

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

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

        let ids: Vec<Pubkey> = payload
            .ids
            .into_iter()
            .map(validate_pubkey)
            .collect::<Result<Vec<_>, _>>()?;
        let options = payload.options.unwrap_or_default();

        let res = match get_asset_batch(self.rocks_db.clone(), ids, options).await {
            Ok(assets) => {
                if let Some(json_downloader) = &self.json_downloader {
                    let mut a_v = assets.iter().filter_map(|opt| opt.clone()).collect();
                    Self::check_and_supplement_jsons(json_downloader.clone(), &mut a_v).await;
                    Some(a_v)
                } else {
                    Some(assets.iter().filter_map(|opt| opt.clone()).collect())
                }
            }
            Err(e) => return Err(Into::<DasApiError>::into(e)),
        };

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_owner(
        &self,
        payload: GetAssetsByOwner,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_owner";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let mut res = self
            .process_request(self.pg_client.clone(), self.rocks_db.clone(), payload)
            .await?;

        if let Some(json_downloader) = &self.json_downloader {
            Self::check_and_supplement_jsons(json_downloader.clone(), &mut res.items).await;
        }

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_group(
        &self,
        payload: GetAssetsByGroup,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_group";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let mut res = self
            .process_request(self.pg_client.clone(), self.rocks_db.clone(), payload)
            .await?;

        if let Some(json_downloader) = &self.json_downloader {
            Self::check_and_supplement_jsons(json_downloader.clone(), &mut res.items).await;
        }

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_creator(
        &self,
        payload: GetAssetsByCreator,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_creator";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let mut res = self
            .process_request(self.pg_client.clone(), self.rocks_db.clone(), payload)
            .await?;

        if let Some(json_downloader) = &self.json_downloader {
            Self::check_and_supplement_jsons(json_downloader.clone(), &mut res.items).await;
        }

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn get_assets_by_authority(
        &self,
        payload: GetAssetsByAuthority,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_authority";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let mut res = self
            .process_request(self.pg_client.clone(), self.rocks_db.clone(), payload)
            .await?;

        if let Some(json_downloader) = &self.json_downloader {
            Self::check_and_supplement_jsons(json_downloader.clone(), &mut res.items).await;
        }

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

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
            owner,
            mint,
            options,
        } = payload;

        let pagination = Pagination {
            limit,
            page,
            before: None,
            after: None,
            cursor: None,
        };

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
            options.map(|op| op.show_zero_balance).unwrap_or_default(),
        )
        .await?;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn search_assets(&self, payload: SearchAssets) -> Result<Value, DasApiError> {
        // use names of the filter fields as a label for better understanding of the endpoint usage
        let label = payload.extract_some_fields();
        self.metrics.inc_search_asset_requests(&label);
        let latency_timer = Instant::now();

        let mut res = self
            .process_request(self.pg_client.clone(), self.rocks_db.clone(), payload)
            .await?;

        if let Some(json_downloader) = &self.json_downloader {
            Self::check_and_supplement_jsons(json_downloader.clone(), &mut res.items).await;
        }

        self.metrics
            .set_search_asset_latency(&label, latency_timer.elapsed().as_millis() as f64);
        self.metrics
            .set_latency("search_asset", latency_timer.elapsed().as_millis() as f64);

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

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        if is_deprecated {
            return Ok(json!(TransactionSignatureListDeprecated::from(res)));
        }
        Ok(json!(res))
    }

    pub async fn get_grouping(&self, payload: GetGrouping) -> Result<Value, DasApiError> {
        let label = "get_grouping";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetGrouping {
            group_key,
            group_value,
        } = payload;
        let group_value_pubkey = validate_pubkey(group_value.clone())?;
        let gs = get_grouping(
            &self.db_connection,
            group_key.clone(),
            group_value_pubkey.to_bytes().as_slice(),
        )
        .await?;
        let res = GetGroupingResponse {
            group_key,
            group_name: group_value,
            group_size: gs.size,
        };

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

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
        let options = payload.get_options().unwrap_or_default();

        let query: SearchAssetsQuery = payload
            .try_into()
            .map_err(|e| DasApiError::from(e.into()))?;

        Self::validate_basic_pagination(&pagination, self.max_page_limit)?;

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
        )
        .await?;

        Ok(res)
    }

    async fn check_and_supplement_jsons(json_downloader: Arc<J>, assets: &mut Vec<Asset>) {
        let mut urls_set = HashSet::new();

        for a in assets.iter() {
            if let Some(content) = &a.content {
                if content.json_uri != "" && content.metadata.inner().is_empty() {
                    urls_set.insert(content.json_uri.clone());
                }
            }
        }

        if urls_set.is_empty() {
            return;
        }

        let downloaded_jsons = json_downloader.get_metadata(urls_set).await;

        if let Ok(jsons) = downloaded_jsons {
            for a in assets.iter_mut() {
                if let Some(content) = &mut a.content {
                    if let Some(json) = jsons.get(&content.json_uri) {
                        let metadata_value = serde_json::from_str(json);
                        if let Ok(metadata) = metadata_value {
                            complement_asset_content_with_json_data(content, metadata);
                        }
                    }
                }
            }
        } else {
            // safe to use unwrap here because we checked that it's not Ok
            debug!("API Json download error: {}", downloaded_jsons.err().unwrap());
        }
    }
}
