use digital_asset_types::dao::scopes::asset::get_grouping;
use digital_asset_types::dao::SearchAssetsQuery;
use digital_asset_types::dapi::{get_asset, get_asset_batch, get_proof_for_assets, search_assets};
use digital_asset_types::rpc::response::AssetList;
use interface::error::UsecaseError;
use interface::proofs::ProofChecker;
use metrics_utils::red::RequestErrorDurationMetrics;
use postgre_client::PgClient;
use std::{sync::Arc, time::Instant};

use self::util::RequestWithPagination;
use crate::api::config::Config;
use digital_asset_types::rpc::Asset;
use entities::api_req_params::{
    GetAsset, GetAssetBatch, GetAssetProof, GetAssetProofBatch, GetAssetsByAuthority,
    GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner, GetGrouping, Pagination, SearchAssets,
};
use metrics_utils::ApiMetricsConfig;
use rocks_db::Storage;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use usecase::validation::validate_pubkey;

use {
    crate::api::*,
    sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, SqlxPostgresConnector, Statement},
    sqlx::postgres::PgPoolOptions,
};

const MAX_ITEMS_IN_BATCH_REQ: usize = 1000;
const DEFAULT_LIMIT: usize = MAX_ITEMS_IN_BATCH_REQ;
const MAX_PAGE_LIMIT: usize = 50;

pub struct DasApi<PC>
where
    PC: ProofChecker + Sync + Send + 'static,
{
    db_connection: DatabaseConnection,
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    metrics: Arc<ApiMetricsConfig>,
    proof_checker: Option<Arc<PC>>,
}

impl<PC> DasApi<PC>
where
    PC: ProofChecker + Sync + Send + 'static,
{
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<ApiMetricsConfig>,
        proof_checker: Option<Arc<PC>>,
    ) -> Self {
        let db_connection = SqlxPostgresConnector::from_sqlx_postgres_pool(pg_client.pool.clone());

        DasApi {
            db_connection,
            pg_client,
            rocks_db,
            metrics,
            proof_checker,
        }
    }

    pub async fn from_config(
        config: Config,
        metrics: Arc<ApiMetricsConfig>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
        rocks_db: Arc<Storage>,
        proof_checker: Option<Arc<PC>>,
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
        })
    }
}

pub fn validate_basic_pagination(parameters: &Pagination) -> Result<(), DasApiError> {
    if let Some(limit) = parameters.limit {
        // make config item
        if limit > MAX_ITEMS_IN_BATCH_REQ as u32 {
            return Err(DasApiError::PaginationError);
        }
    }
    if (parameters.before.is_some() || parameters.after.is_some()) && parameters.cursor.is_some() {
        return Err(DasApiError::PaginationError);
    }
    if let Some(page) = parameters.page {
        if page == 0 {
            return Err(DasApiError::PaginationEmptyError);
        }
        if page > MAX_PAGE_LIMIT as u32 {
            return Err(DasApiError::PaginationError);
        }
        // make config item
        if parameters.before.is_some() || parameters.after.is_some() || parameters.cursor.is_some()
        {
            return Err(DasApiError::PaginationError);
        }
    }
    Ok(())
}

pub fn not_found() -> DasApiError {
    DasApiError::NoDataFoundError
}

impl<PC> DasApi<PC>
where
    PC: ProofChecker + Sync + Send + 'static,
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
        let res = get_asset(self.rocks_db.clone(), id)
            .await
            .map_err(Into::<DasApiError>::into)?;

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

        let res = get_asset_batch(self.rocks_db.clone(), ids)
            .await
            .map_err(Into::<DasApiError>::into)?;

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

        let res = process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

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

        let res = process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

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

        let res = process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

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

        let res = process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }

    pub async fn search_assets(&self, payload: SearchAssets) -> Result<Value, DasApiError> {
        // use names of the filter fields as a label for better understanding of the endpoint usage
        let label = payload.extract_some_fields();
        self.metrics.inc_search_asset_requests(&label);
        let latency_timer = Instant::now();

        let res = process_request(self.pg_client.clone(), self.rocks_db.clone(), payload).await?;

        self.metrics
            .set_search_asset_latency(&label, latency_timer.elapsed().as_millis() as f64);
        self.metrics
            .set_latency("search_asset", latency_timer.elapsed().as_millis() as f64);

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
        let gs = get_grouping(&self.db_connection, group_key.clone(), group_value.clone()).await?;
        let res = GetGroupingResponse {
            group_key,
            group_name: group_value,
            group_size: gs.size,
        };

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_millis() as f64);

        Ok(json!(res))
    }
}

async fn process_request<T>(
    pg_client: Arc<impl postgre_client::storage_traits::AssetPubkeyFilteredFetcher>,
    rocks_db: Arc<Storage>,
    payload: T,
) -> Result<AssetList, DasApiError>
where
    T: TryInto<SearchAssetsQuery>,
    T: RequestWithPagination,
    T::Error: Into<UsecaseError>,
{
    let pagination = payload.get_all_pagination_parameters();
    let sort_by = payload.get_sort_parameter().unwrap_or_default();

    let query: SearchAssetsQuery = payload
        .try_into()
        .map_err(|e| DasApiError::from(e.into()))?;

    validate_basic_pagination(&pagination)?;

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
    )
    .await?;

    Ok(res)
}
