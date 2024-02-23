use sqlx::{Execute, Postgres, QueryBuilder, Transaction};

use crate::{
    PgClient, ALTER_ACTION, COPY_ACTION, CREATE_ACTION, DROP_ACTION, SQL_COMPONENT, TRUNCATE_ACTION,
};

impl PgClient {
    pub async fn copy_metadata_from(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        path: String,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("COPY tasks (tsk_id, tsk_metadata_url, tsk_status) FROM '");
        query_builder.push(path);
        query_builder.push("' WITH (FORMAT csv);");
        self.execute_query_with_metrics(transaction, &mut query_builder, COPY_ACTION, "tasks")
            .await
    }

    pub async fn copy_asset_creators_from(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        path: String,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "COPY asset_creators_v3 (asc_pubkey, asc_creator, asc_verified, asc_slot_updated) FROM '",
        );
        query_builder.push(path);
        query_builder.push("' WITH (FORMAT csv);");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            COPY_ACTION,
            "asset_creators",
        )
        .await
    }

    pub async fn truncate_asset_creators(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("TRUNCATE asset_creators_v3;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            TRUNCATE_ACTION,
            "asset_creators",
        )
        .await
    }

    pub async fn truncate_assets(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("TRUNCATE assets_v3;");
        self.execute_query_with_metrics(transaction, &mut query_builder, TRUNCATE_ACTION, "assets")
            .await
    }

    pub async fn copy_assets_from(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        path: String,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("COPY assets_v3 (ast_pubkey, ast_specification_version, ast_specification_asset_class, ast_royalty_target_type, ast_royalty_amount, ast_slot_created, ast_owner_type, ast_owner, ast_delegate, ast_authority, ast_collection, ast_is_collection_verified, ast_is_burnt, ast_is_compressible, ast_is_compressed, ast_is_frozen, ast_supply, ast_metadata_url_id, ast_slot_updated) FROM '");
        query_builder.push(path);
        query_builder.push("' WITH (FORMAT csv);");
        self.execute_query_with_metrics(transaction, &mut query_builder, COPY_ACTION, "assets")
            .await
    }
    pub async fn execute_query_with_metrics(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        query_builder: &mut QueryBuilder<'_, Postgres>,
        action: &str,
        endpoint: &str,
    ) -> Result<(), String> {
        let query = query_builder.build();
        tracing::info!("Executing query: {:?}", query.sql());
        let start_time = chrono::Utc::now();
        match query.execute(transaction).await {
            Ok(_) => {
                self.metrics
                    .observe_request(SQL_COMPONENT, action, endpoint, start_time);
                Ok(())
            }
            Err(err) => {
                self.metrics.observe_error(SQL_COMPONENT, action, endpoint);
                Err(err.to_string())
            }
        }
    }

    async fn drop_index(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        index: &str,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("DROP INDEX ");
        query_builder.push(index);
        query_builder.push(";");
        self.execute_query_with_metrics(transaction, &mut query_builder, DROP_ACTION, index)
            .await
    }

    pub async fn drop_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE assets_v3 DISABLE TRIGGER ALL;");
        self.execute_query_with_metrics(transaction, &mut query_builder, ALTER_ACTION, "assets_v3")
            .await?;
        for index in [
            "asset_creators_v3_creator",
            "assets_v3_specification_version",
            "assets_v3_specification_asset_class",
            "assets_v3_royalty_target_type",
            "assets_v3_royalty_amount",
            "assets_v3_slot_created",
            "assets_v3_owner_type",
            "assets_v3_metadata_url",
            "assets_v3_owner",
            "assets_v3_delegate",
            "assets_v3_authority",
            "assets_v3_collection_is_collection_verified",
            "assets_v3_is_burnt",
            "assets_v3_is_compressible",
            "assets_v3_is_compressed",
            "assets_v3_is_frozen",
            "assets_v3_supply",
            "assets_v3_slot_updated",
        ] {
            self.drop_index(transaction, index).await?;
        }
        Ok(())
    }

    async fn create_index(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        name: &str,
        on_query_string: &str,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("CREATE INDEX ");
        query_builder.push(name);
        query_builder.push(" ON ");
        query_builder.push(on_query_string);
        query_builder.push(";");
        self.execute_query_with_metrics(transaction, &mut query_builder, CREATE_ACTION, name)
            .await
    }
    pub async fn recreate_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE assets_v3 ENABLE TRIGGER ALL;");
        self.execute_query_with_metrics(transaction, &mut query_builder, ALTER_ACTION, "assets_v3")
            .await?;

        for (index, on_query_string) in [
("asset_creators_v3_creator", "asset_creators_v3(asc_creator, asc_verified)"),
("assets_v3_specification_version", "assets_v3 (ast_specification_version) WHERE ast_specification_version <> 'v1'::specification_versions"),
("assets_v3_specification_asset_class", "assets_v3 (ast_specification_asset_class) WHERE ast_specification_asset_class IS NOT NULL AND ast_specification_asset_class <> 'unknown'::specification_asset_class"),
("assets_v3_royalty_target_type", "assets_v3 (ast_royalty_target_type) WHERE ast_royalty_target_type <> 'creators'::royalty_target_type"),
("assets_v3_royalty_amount", "assets_v3 (ast_royalty_amount)"),
("assets_v3_slot_created", "assets_v3 (ast_slot_created)"),
("assets_v3_owner_type", "assets_v3 (ast_owner_type) WHERE ast_owner_type IS NOT NULL AND ast_owner_type <> 'unknown'::owner_type"),
("assets_v3_metadata_url", "assets_v3 (ast_metadata_url_id) WHERE ast_metadata_url_id IS NOT NULL"),
("assets_v3_owner", "assets_v3(ast_owner) WHERE ast_owner IS NOT NULL"),
("assets_v3_delegate", "assets_v3(ast_delegate) WHERE ast_delegate IS NOT NULL"),
("assets_v3_authority", "assets_v3(ast_authority) WHERE ast_authority IS NOT NULL"),
("assets_v3_collection_is_collection_verified", "assets_v3(ast_collection, ast_is_collection_verified) WHERE ast_collection IS NOT NULL"),
("assets_v3_is_burnt", "assets_v3(ast_is_burnt) WHERE ast_is_burnt IS TRUE"),
("assets_v3_is_compressible", "assets_v3(ast_is_compressible) WHERE ast_is_compressible IS TRUE"),
("assets_v3_is_compressed", "assets_v3(ast_is_compressed)"),
("assets_v3_is_frozen", "assets_v3(ast_is_frozen) WHERE ast_is_frozen IS TRUE"),
("assets_v3_supply", "assets_v3(ast_supply) WHERE ast_supply IS NOT NULL"),
("assets_v3_slot_updated", "assets_v3(ast_slot_updated)"),
            ]{
                self.create_index(transaction, index, on_query_string).await?;
            }
        Ok(())
    }

    pub(crate) async fn copy_all(
        &self,
        matadata_copy_path: String,
        asset_creators_copy_path: String,
        assets_copy_path: String,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        self.drop_indexes(transaction).await?;
        self.truncate_assets(transaction).await?;
        self.truncate_asset_creators(transaction).await?;
        self.copy_metadata_from(transaction, matadata_copy_path)
            .await?;
        self.copy_asset_creators_from(transaction, asset_creators_copy_path)
            .await?;
        self.copy_assets_from(transaction, assets_copy_path).await?;
        self.recreate_indexes(transaction).await?;
        Ok(())
    }
}
