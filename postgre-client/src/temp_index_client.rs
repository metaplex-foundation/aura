use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{pool::PoolConnection, Connection, Postgres, QueryBuilder};
use tokio::sync::Mutex;

use crate::{
    asset_index_client::{split_assets_into_components, TableNames},
    storage_traits::{AssetIndexStorage, TempClientProvider},
    PgClient, BATCH_DELETE_ACTION, BATCH_UPSERT_ACTION, INSERT_ACTION, UPDATE_ACTION,
};
use entities::models::AssetIndex;

pub const TEMP_INDEXING_TABLE_PREFIX: &str = "indexing_temp_";
#[derive(Clone)]
pub struct TempClient {
    pooled_connection: Arc<Mutex<PoolConnection<Postgres>>>,
    pg_client: Arc<PgClient>,
}

impl TempClient {
    pub async fn create_new(pg_client: Arc<PgClient>) -> Result<Self, sqlx::Error> {
        let pooled_connection = Arc::new(Mutex::new(pg_client.pool.acquire().await?));
        Ok(Self {
            pooled_connection,
            pg_client,
        })
    }

    pub async fn initialize(&self, initial_key: &[u8]) -> Result<(), String> {
        let mut c = self.pooled_connection.lock().await;
        let mut tx = c.begin().await.map_err(|e| e.to_string())?;
        for table in ["tasks", "asset_creators_v3", "assets_v3", "last_synced_key"] {
            self.pg_client
                .create_temp_tables(table, &mut tx, false, TEMP_INDEXING_TABLE_PREFIX)
                .await?;
        }

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {}last_synced_key (id, last_synced_asset_update_key) VALUES (1, null);",
            TEMP_INDEXING_TABLE_PREFIX
        ));
        let last_key_table_name = format!("{}last_synced_key", TEMP_INDEXING_TABLE_PREFIX);
        self.pg_client
            .execute_query_with_metrics(
                &mut tx,
                &mut query_builder,
                INSERT_ACTION,
                last_key_table_name.as_str(),
            )
            .await?;
        self.pg_client
            .update_last_synced_key(initial_key, &mut tx, last_key_table_name.as_str())
            .await?;
        self.pg_client
            .commit_transaction(tx)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn copy_to_main(&self) -> Result<(), String> {
        let mut c = self.pooled_connection.lock().await;
        let mut tx = c.begin().await.map_err(|e| e.to_string())?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO tasks SELECT * FROM ");
        query_builder.push(TEMP_INDEXING_TABLE_PREFIX);
        query_builder.push("tasks ON CONFLICT DO NOTHING;");
        self.pg_client
            .execute_query_with_metrics(&mut tx, &mut query_builder, BATCH_UPSERT_ACTION, "tasks")
            .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO assets_v3 SELECT * FROM ");
        query_builder.push(TEMP_INDEXING_TABLE_PREFIX);
        query_builder.push("assets_v3 ON CONFLICT (ast_pubkey) 
        DO UPDATE SET 
            ast_specification_version = EXCLUDED.ast_specification_version,
            ast_specification_asset_class = EXCLUDED.ast_specification_asset_class,
            ast_royalty_target_type = EXCLUDED.ast_royalty_target_type,
            ast_royalty_amount = EXCLUDED.ast_royalty_amount,
            ast_slot_created = EXCLUDED.ast_slot_created,
            ast_owner_type = EXCLUDED.ast_owner_type,
            ast_owner = EXCLUDED.ast_owner,
            ast_delegate = EXCLUDED.ast_delegate,
            ast_authority = EXCLUDED.ast_authority,
            ast_collection = EXCLUDED.ast_collection,
            ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
            ast_is_burnt = EXCLUDED.ast_is_burnt,
            ast_is_compressible = EXCLUDED.ast_is_compressible,
            ast_is_compressed = EXCLUDED.ast_is_compressed,
            ast_is_frozen = EXCLUDED.ast_is_frozen,
            ast_supply = EXCLUDED.ast_supply,
            ast_metadata_url_id = EXCLUDED.ast_metadata_url_id,
            ast_slot_updated = EXCLUDED.ast_slot_updated
            WHERE assets_v3.ast_slot_updated <= EXCLUDED.ast_slot_updated OR assets_v3.ast_slot_updated IS NULL;");

        self.pg_client
            .execute_query_with_metrics(
                &mut tx,
                &mut query_builder,
                BATCH_UPSERT_ACTION,
                "assets_v3",
            )
            .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("INSERT INTO asset_creators_v3 SELECT * FROM ");
        query_builder.push(TEMP_INDEXING_TABLE_PREFIX);
        query_builder.push(
            "asset_creators_v3 
        ON CONFLICT (asc_creator, asc_pubkey) 
        DO UPDATE SET asc_verified = EXCLUDED.asc_verified 
        WHERE asset_creators_v3.asc_slot_updated <= EXCLUDED.asc_slot_updated;",
        );

        self.pg_client
            .execute_query_with_metrics(
                &mut tx,
                &mut query_builder,
                BATCH_UPSERT_ACTION,
                "asset_creators_v3",
            )
            .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "DELETE FROM asset_creators_v3 WHERE asc_pubkey IN (SELECT distinct asc_pubkey FROM ",
        );
        query_builder.push(TEMP_INDEXING_TABLE_PREFIX);
        query_builder.push("asset_creators_v3) AND (asc_creator, asc_pubkey) NOT IN (SELECT asc_creator, asc_pubkey FROM ");
        query_builder.push(TEMP_INDEXING_TABLE_PREFIX);
        query_builder.push("asset_creators_v3);");
        self.pg_client
            .execute_query_with_metrics(
                &mut tx,
                &mut query_builder,
                BATCH_DELETE_ACTION,
                "asset_creators_v3",
            )
            .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("UPDATE last_synced_key SET last_synced_asset_update_key = (SELECT last_synced_asset_update_key FROM ");
        query_builder.push(TEMP_INDEXING_TABLE_PREFIX);
        query_builder.push("last_synced_key WHERE id = 1) WHERE id = 1;");
        self.pg_client
            .execute_query_with_metrics(
                &mut tx,
                &mut query_builder,
                UPDATE_ACTION,
                "last_synced_key",
            )
            .await?;
        self.pg_client.commit_transaction(tx).await
    }
}

#[async_trait]
impl AssetIndexStorage for Arc<TempClient> {
    async fn fetch_last_synced_id(&self) -> Result<Option<Vec<u8>>, String> {
        let mut c = self.pooled_connection.lock().await;
        let mut tx = c.begin().await.map_err(|e| e.to_string())?;
        self.pg_client
            .fetch_last_synced_id_impl(
                format!("{}last_synced_key", TEMP_INDEXING_TABLE_PREFIX).as_str(),
                &mut tx,
            )
            .await
    }

    async fn update_asset_indexes_batch(
        &self,
        asset_indexes: &[AssetIndex],
        last_key: &[u8],
    ) -> Result<(), String> {
        let updated_components = split_assets_into_components(asset_indexes);
        let mut c = self.pooled_connection.lock().await;
        let mut transaction = c.begin().await.map_err(|e| e.to_string())?;
        let table_names = TableNames {
            metadata_table: format!("{}tasks", TEMP_INDEXING_TABLE_PREFIX),
            assets_table: format!("{}assets_v3", TEMP_INDEXING_TABLE_PREFIX),
            creators_table: format!("{}asset_creators_v3", TEMP_INDEXING_TABLE_PREFIX),
            last_synced_key_table: format!("{}last_synced_key", TEMP_INDEXING_TABLE_PREFIX),
        };
        self.pg_client
            .upsert_batched(&mut transaction, table_names, updated_components, last_key)
            .await?;
        self.pg_client.commit_transaction(transaction).await
    }

    async fn load_from_dump(
        &self,
        _base_path: &std::path::Path,
        _last_key: &[u8],
    ) -> Result<(), String> {
        Err("Temporary client does not support batch load from file".to_string())
    }
}

#[async_trait]
impl TempClientProvider for Arc<PgClient> {
    async fn create_temp_client(&self) -> Result<TempClient, String> {
        TempClient::create_new(self.clone())
            .await
            .map_err(|e| e.to_string())
    }
}
