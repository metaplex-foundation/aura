use sqlx::{Execute, Postgres, QueryBuilder, Transaction};

use crate::{PgClient, ALTER_ACTION, COPY_ACTION, CREATE_ACTION, DROP_ACTION, SQL_COMPONENT};

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
    pub async fn drop_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), String> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE assets_v3 DISABLE TRIGGER ALL;");
        self.execute_query_with_metrics(transaction, &mut query_builder, ALTER_ACTION, "assets_v3")
            .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX asset_creators_v3_creator;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_asset_creators_v3_creator",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_specification_version;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_specification_version",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_specification_asset_class;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_specification_asset_class",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_royalty_target_type;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_royalty_target_type",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_royalty_amount;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_royalty_amount",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_slot_created;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_slot_created",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_owner_type;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_owner_type",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_metadata_url;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_metadata_url",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_owner;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_owner",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_delegate;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_delegate",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_authority;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_authority",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_collection_is_collection_verified;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_collection_is_collection_verified",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_is_burnt;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_is_burnt",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_is_compressible;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_is_compressible",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_is_compressed;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_is_compressed",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_is_frozen;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_is_frozen",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_supply;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_supply",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("DROP INDEX assets_v3_slot_updated;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            DROP_ACTION,
            "ind_assets_v3_slot_updated",
        )
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
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX asset_creators_v3_creator ON asset_creators_v3(asc_creator, asc_verified);");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_asset_creators_v3_creator",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_specification_version ON assets_v3 (ast_specification_version) WHERE ast_specification_version <> 'v1'::specification_versions;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_specification_version",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_specification_asset_class ON assets_v3 (ast_specification_asset_class) WHERE ast_specification_asset_class IS NOT NULL AND ast_specification_asset_class <> 'unknown'::specification_asset_class;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_specification_asset_class",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_royalty_target_type ON assets_v3 (ast_royalty_target_type) WHERE ast_royalty_target_type <> 'creators'::royalty_target_type;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_royalty_target_type",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "CREATE INDEX assets_v3_royalty_amount ON assets_v3 (ast_royalty_amount);",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_royalty_amount",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "CREATE INDEX assets_v3_slot_created ON assets_v3 (ast_slot_created);",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_slot_created",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_owner_type ON assets_v3 (ast_owner_type) WHERE ast_owner_type IS NOT NULL AND ast_owner_type <> 'unknown'::owner_type;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_owner_type",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_metadata_url ON assets_v3 (ast_metadata_url_id) WHERE ast_metadata_url_id IS NOT NULL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_metadata_url",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "CREATE INDEX assets_v3_owner ON assets_v3(ast_owner) WHERE ast_owner IS NOT NULL;",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_owner",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_delegate ON assets_v3(ast_delegate) WHERE ast_delegate IS NOT NULL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_delegate",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_authority ON assets_v3(ast_authority) WHERE ast_authority IS NOT NULL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_authority",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_collection_is_collection_verified ON assets_v3(ast_collection, ast_is_collection_verified) WHERE ast_collection IS NOT NULL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_collection_is_collection_verified",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_is_burnt ON assets_v3(ast_is_burnt) WHERE ast_is_burnt IS TRUE;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_is_burnt",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_is_compressible ON assets_v3(ast_is_compressible) WHERE ast_is_compressible IS TRUE;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_is_compressible",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "CREATE INDEX assets_v3_is_compressed ON assets_v3(ast_is_compressed);",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_is_compressed",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("CREATE INDEX assets_v3_is_frozen ON assets_v3(ast_is_frozen) WHERE ast_is_frozen IS TRUE;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_is_frozen",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "CREATE INDEX assets_v3_supply ON assets_v3(ast_supply) WHERE ast_supply IS NOT NULL;",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_supply",
        )
        .await?;

        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "CREATE INDEX assets_v3_slot_updated ON assets_v3(ast_slot_updated);",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            CREATE_ACTION,
            "ind_assets_v3_slot_updated",
        )
        .await
    }

    pub async fn copy_all(
        &self,
        matadata_copy_path: String,
        asset_creators_copy_path: String,
        assets_copy_path: String,
    ) -> Result<(), String> {
        let mut transaction = self.start_transaction().await?;
        self.drop_indexes(&mut transaction).await?;
        self.copy_metadata_from(&mut transaction, matadata_copy_path)
            .await?;
        self.copy_asset_creators_from(&mut transaction, asset_creators_copy_path)
            .await?;
        self.copy_assets_from(&mut transaction, assets_copy_path)
            .await?;
        self.recreate_indexes(&mut transaction).await?;
        self.commit_transaction(transaction).await
    }
}
