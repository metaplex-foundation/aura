use digital_asset_types::dao::scopes::asset::get_grouping;
use digital_asset_types::dao::SearchAssetsQuery;
use digital_asset_types::dapi::{
    get_asset, get_asset_batch, get_assets_by_authority, get_assets_by_creator,
    get_assets_by_group, get_assets_by_owner, get_proof_for_assets, search_assets,
};
use postgre_client::PgClient;
use std::{sync::Arc, time::Instant};

use crate::api::{config::Config, validation::validate_pubkey};
use digital_asset_types::rpc::Asset;
use metrics_utils::ApiMetricsConfig;
use rocks_db::Storage;
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use {
    crate::api::*,
    sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, SqlxPostgresConnector, Statement},
    sqlx::postgres::PgPoolOptions,
};

const MAX_ITEMS_IN_BATCH_REQ: usize = 1000;
const DEFAULT_LIMIT: usize = MAX_ITEMS_IN_BATCH_REQ;

pub struct DasApi {
    db_connection: DatabaseConnection,
    pg_client: Arc<PgClient>,
    rocks_db: Arc<Storage>,
    metrics: Arc<ApiMetricsConfig>,
}

impl DasApi {
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<ApiMetricsConfig>,
    ) -> Self {
        let db_connection = SqlxPostgresConnector::from_sqlx_postgres_pool(pg_client.pool.clone());

        DasApi {
            db_connection,
            pg_client,
            rocks_db,
            metrics,
        }
    }

    pub async fn from_config(
        config: Config,
        metrics: Arc<ApiMetricsConfig>,
        rocks_db: Arc<Storage>,
    ) -> Result<Self, DasApiError> {
        let pool = PgPoolOptions::new()
            .max_connections(250)
            .connect(&config.database_url)
            .await?;

        let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
        let pg_client = PgClient::new_with_pool(pool);
        Ok(DasApi {
            db_connection: conn,
            pg_client: Arc::new(pg_client),
            rocks_db,
            metrics,
        })
    }

    fn validate_pagination(
        &self,
        limit: &Option<u32>,
        page: &Option<u32>,
        before: &Option<String>,
        after: &Option<String>,
    ) -> Result<(), DasApiError> {
        if page.is_none() && before.is_none() && after.is_none() {
            return Err(DasApiError::PaginationEmptyError);
        }

        validate_basic_pagination(limit, page, before, after)?;
        if let Some(before) = before {
            validate_pubkey(before.clone())?;
        }

        if let Some(after) = after {
            validate_pubkey(after.clone())?;
        }
        Ok(())
    }
}

pub fn validate_basic_pagination(
    limit: &Option<u32>,
    page: &Option<u32>,
    before: &Option<String>,
    after: &Option<String>,
) -> Result<(), DasApiError> {
    if let Some(limit) = limit {
        // make config item
        if *limit > MAX_ITEMS_IN_BATCH_REQ as u32 {
            return Err(DasApiError::PaginationError);
        }
    }
    if let Some(page) = page {
        if *page == 0 {
            return Err(DasApiError::PaginationEmptyError);
        }
        // make config item
        if before.is_some() || after.is_some() {
            return Err(DasApiError::PaginationError);
        }
    }
    Ok(())
}

pub fn not_found() -> DasApiError {
    DasApiError::NoDataFoundError
}

impl DasApi {
    pub async fn check_health(self: &DasApi) -> Result<Value, DasApiError> {
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
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!("ok"))
    }

    pub async fn get_asset_proof(
        self: &DasApi,
        payload: GetAssetProof,
    ) -> Result<Value, DasApiError> {
        let label = "get_asset_proof";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let id = validate_pubkey(payload.id.clone())?;
        let assets = get_proof_for_assets(self.rocks_db.clone(), vec![id]).await?;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        let res = assets
            .get(&id.to_string())
            .ok_or(not_found())?
            .as_ref()
            .ok_or::<DasApiError>(not_found())
            .cloned()
            .map_err(Into::<DasApiError>::into);

        Ok(json!(res?))
    }

    pub async fn get_asset_proof_batch(
        self: &DasApi,
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

        let res = get_proof_for_assets(self.rocks_db.clone(), ids)
            .await
            .map_err(Into::<DasApiError>::into);

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res?))
    }

    pub async fn get_asset(self: &DasApi, payload: GetAsset) -> Result<Value, DasApiError> {
        let label = "get_asset";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let id = validate_pubkey(payload.id.clone())?;
        let res = get_asset(self.rocks_db.clone(), id)
            .await
            .map_err(Into::<DasApiError>::into)?;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        if res.is_none() {
            return Err(not_found());
        }

        Ok(json!(res))
    }

    pub async fn get_asset_batch(
        self: &DasApi,
        payload: GetAssetBatch,
    ) -> Result<Value, DasApiError> {
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
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res))
    }

    pub async fn get_assets_by_owner(
        self: &DasApi,
        payload: GetAssetsByOwner,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_owner";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetAssetsByOwner {
            owner_address,
            sort_by,
            limit,
            page,
            before,
            after,
        } = payload;
        let before: Option<String> = before.filter(|before| !before.is_empty());
        let after: Option<String> = after.filter(|after| !after.is_empty());
        let owner_address = validate_pubkey(owner_address.clone())?;
        let sort_by = sort_by.unwrap_or_default();
        self.validate_pagination(&limit, &page, &before, &after)?;
        let res = get_assets_by_owner(
            &self.db_connection,
            self.rocks_db.clone(),
            owner_address,
            sort_by,
            limit.map(|x| x as u64).unwrap_or(DEFAULT_LIMIT as u64),
            page.map(|x| x as u64),
            before.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
            after.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
        )
        .await
        .map_err(Into::<DasApiError>::into);

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res?))
    }

    pub async fn get_assets_by_group(
        self: &DasApi,
        payload: GetAssetsByGroup,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_group";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetAssetsByGroup {
            group_key,
            group_value,
            sort_by,
            limit,
            page,
            before,
            after,
        } = payload;
        let before: Option<String> = before.filter(|before| !before.is_empty());
        let after: Option<String> = after.filter(|after| !after.is_empty());
        let sort_by = sort_by.unwrap_or_default();
        self.validate_pagination(&limit, &page, &before, &after)?;
        let group_value = validate_pubkey(group_value.clone())?;
        let res = get_assets_by_group(
            &self.db_connection,
            self.rocks_db.clone(),
            group_value.to_bytes().as_slice().to_vec(),
            group_key,
            sort_by,
            limit.map(|x| x as u64).unwrap_or(DEFAULT_LIMIT as u64),
            page.map(|x| x as u64),
            before.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
            after.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
        )
        .await
        .map_err(Into::<DasApiError>::into);

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res?))
    }

    pub async fn get_assets_by_creator(
        self: &DasApi,
        payload: GetAssetsByCreator,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_creator";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetAssetsByCreator {
            creator_address,
            only_verified,
            sort_by,
            limit,
            page,
            before,
            after,
        } = payload;
        let creator_address = validate_pubkey(creator_address.clone())?;
        let creator_address_bytes = creator_address.to_bytes().to_vec();

        self.validate_pagination(&limit, &page, &before, &after)?;
        let sort_by = sort_by.unwrap_or_default();
        let only_verified = only_verified.unwrap_or_default();
        let res = get_assets_by_creator(
            &self.db_connection,
            self.rocks_db.clone(),
            creator_address_bytes,
            only_verified,
            sort_by,
            limit.map(|x| x as u64).unwrap_or(DEFAULT_LIMIT as u64),
            page.map(|x| x as u64),
            before.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
            after.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
        )
        .await
        .map_err(Into::<DasApiError>::into);

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res?))
    }

    pub async fn get_assets_by_authority(
        self: &DasApi,
        payload: GetAssetsByAuthority,
    ) -> Result<Value, DasApiError> {
        let label = "get_assets_by_authority";
        self.metrics.inc_requests(label);
        let latency_timer = Instant::now();

        let GetAssetsByAuthority {
            authority_address,
            sort_by,
            limit,
            page,
            before,
            after,
        } = payload;
        let sort_by = sort_by.unwrap_or_default();
        let authority_address = validate_pubkey(authority_address.clone())?;
        let authority_address_bytes = authority_address.to_bytes().to_vec();

        self.validate_pagination(&limit, &page, &before, &after)?;
        let res = get_assets_by_authority(
            &self.db_connection,
            self.rocks_db.clone(),
            authority_address_bytes,
            sort_by,
            limit.map(|x| x as u64).unwrap_or(DEFAULT_LIMIT as u64),
            page.map(|x| x as u64),
            before.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
            after.map(|x| bs58::decode(x).into_vec().unwrap_or_default()),
        )
        .await;

        self.metrics
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res?))
    }

    pub async fn search_assets(self: &DasApi, payload: SearchAssets) -> Result<Value, DasApiError> {
        // use names of the filter fields as a label for better understanding of the endpoint usage
        let label = payload.extract_some_fields();
        self.metrics.inc_search_asset_requests(&label);
        let latency_timer = Instant::now();

        let limit = payload.limit;
        let page = payload.page;
        let before = payload.before.clone();
        let after = payload.after.clone();
        validate_basic_pagination(&limit, &page, &before, &after)?;

        let query: SearchAssetsQuery = payload.clone().try_into()?;

        let res = search_assets(
            self.pg_client.clone(),
            self.rocks_db.clone(),
            query,
            payload.sort_by.unwrap_or_default(),
            limit.map_or(DEFAULT_LIMIT as u64, |l| l as u64),
            page.map(|p| p as u64),
            before,
            after,
        )
        .await;

        self.metrics
            .set_latency(&label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res?))
    }

    pub async fn get_grouping(self: &DasApi, payload: GetGrouping) -> Result<Value, DasApiError> {
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
            .set_latency(label, latency_timer.elapsed().as_secs_f64());

        Ok(json!(res))
    }
}
