use std::sync::Arc;

use sqlx::{Execute, Postgres, QueryBuilder, Transaction};

use crate::{
    error::IndexDbError, PgClient, ALTER_ACTION, COPY_ACTION, CREATE_ACTION, DROP_ACTION,
    INSERT_ACTION, SQL_COMPONENT, TRUNCATE_ACTION,
};

impl PgClient {
    async fn copy_table_from(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        path: String,
        table: &str,
        columns: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "COPY {} ({}) FROM '{}' WITH (FORMAT csv);",
            table, columns, path,
        ));
        self.execute_query_with_metrics(transaction, &mut query_builder, COPY_ACTION, table)
            .await?;
        Ok(())
    }

    async fn insert_from_temp_table(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
        temp_table: &str,
        on_conflict_do_nothing: bool,
    ) -> Result<(), IndexDbError> {
        let conflict_clause = if on_conflict_do_nothing {
            " ON CONFLICT DO NOTHING"
        } else {
            ""
        };
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {} SELECT * FROM {} {};",
            table, temp_table, conflict_clause
        ));
        self.execute_query_with_metrics(transaction, &mut query_builder, INSERT_ACTION, table)
            .await?;

        Ok(())
    }

    pub(crate) async fn set_unlogged_on(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new(format!("ALTER TABLE {} SET UNLOGGED;", table));
        self.execute_query_with_metrics(transaction, &mut query_builder, "set_unlogged", table)
            .await
    }

    pub(crate) async fn set_logged_on(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new(format!("ALTER TABLE {} SET LOGGED;", table));
        self.execute_query_with_metrics(transaction, &mut query_builder, "set_logged", table)
            .await
    }

    pub(crate) async fn set_autovacuum_off_on(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "ALTER TABLE {} SET (autovacuum_enabled = false);",
            table
        ));
        self.execute_query_with_metrics(transaction, &mut query_builder, ALTER_ACTION, table)
            .await
    }

    pub(crate) async fn reset_autovacuum_on(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "ALTER TABLE {} RESET (autovacuum_enabled); ",
            table
        ));
        self.execute_query_with_metrics(transaction, &mut query_builder, ALTER_ACTION, table)
            .await
    }

    pub(crate) async fn truncate_table(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new(format!("TRUNCATE {};", table));
        self.execute_query_with_metrics(transaction, &mut query_builder, TRUNCATE_ACTION, table)
            .await
    }

    pub async fn execute_query_with_metrics(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        query_builder: &mut QueryBuilder<'_, Postgres>,
        action: &str,
        endpoint: &str,
    ) -> Result<(), IndexDbError> {
        let query = query_builder.build();
        tracing::trace!("Executing query: {:?}", query.sql());
        let start_time = chrono::Utc::now();
        match query.execute(transaction).await {
            Ok(_) => {
                self.metrics
                    .observe_request(SQL_COMPONENT, action, endpoint, start_time);
                Ok(())
            }
            Err(err) => {
                self.metrics.observe_error(SQL_COMPONENT, action, endpoint);
                Err(err.into())
            }
        }
    }

    async fn drop_index(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        index: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new(format!("DROP INDEX IF EXISTS {};", index));
        self.execute_query_with_metrics(transaction, &mut query_builder, DROP_ACTION, index)
            .await
    }

    pub async fn drop_fungible_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE fungible_tokens DISABLE TRIGGER ALL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "fungible_tokens",
        )
        .await?;

        for index in [
            "fungible_tokens_fbt_asset_idx",
            "fungible_tokens_fbt_owner_balance_idx",
        ] {
            self.drop_index(transaction, index).await?;
        }
        Ok(())
    }

    pub async fn drop_nft_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE assets_v3 DISABLE TRIGGER ALL;");
        self.execute_query_with_metrics(transaction, &mut query_builder, ALTER_ACTION, "assets_v3")
            .await?;

        //alternative would be to use ALTER INDEX index_name ALTER INDEX SET DISABLED; but that might not work on all versions of postgres and might be slower

        for index in [
            "asset_creators_v3_creator",
            "assets_authority",
            "assets_v3_authority_fk",
            "assets_v3_collection_is_collection_verified",
            "assets_v3_delegate",
            "assets_v3_is_burnt",
            "assets_v3_is_compressed",
            "assets_v3_is_compressible",
            "assets_v3_is_frozen",
            "assets_v3_metadata_url",
            "assets_v3_owner",
            "assets_v3_owner_type",
            "assets_v3_royalty_amount",
            "assets_v3_royalty_target_type",
            "assets_v3_slot_created",
            "assets_v3_slot_updated",
            "assets_v3_specification_asset_class",
            "assets_v3_specification_version",
            "assets_v3_supply",
        ] {
            self.drop_index(transaction, index).await?;
        }
        Ok(())
    }

    pub (crate) async fn recreate_asset_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE assets_v3 ENABLE TRIGGER ALL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            "trigger_enable",
            "assets_v3",
        )
        .await?;

        for (index, on_query_string) in [
                ("assets_v3_authority_fk", "assets_v3(ast_authority_fk) WHERE ast_authority_fk IS NOT NULL"),
                ("assets_v3_collection_is_collection_verified", "assets_v3(ast_collection, ast_is_collection_verified) WHERE ast_collection IS NOT NULL"),
                ("assets_v3_delegate", "assets_v3(ast_delegate) WHERE ast_delegate IS NOT NULL"),
                ("assets_v3_is_burnt", "assets_v3(ast_is_burnt) WHERE ast_is_burnt IS TRUE"),
                ("assets_v3_is_compressed", "assets_v3(ast_is_compressed)"),
                ("assets_v3_is_compressible", "assets_v3(ast_is_compressible) WHERE ast_is_compressible IS TRUE"),
                ("assets_v3_is_frozen", "assets_v3(ast_is_frozen) WHERE ast_is_frozen IS TRUE"),
                ("assets_v3_metadata_url", "assets_v3 (ast_metadata_url_id) WHERE ast_metadata_url_id IS NOT NULL"),
                ("assets_v3_owner", "assets_v3(ast_owner) WHERE ast_owner IS NOT NULL"),
                ("assets_v3_owner_type", "assets_v3 (ast_owner_type) WHERE ast_owner_type IS NOT NULL AND ast_owner_type <> 'unknown'::owner_type"),
                ("assets_v3_royalty_amount", "assets_v3 (ast_royalty_amount)"),
                ("assets_v3_royalty_target_type", "assets_v3 (ast_royalty_target_type) WHERE ast_royalty_target_type <> 'creators'::royalty_target_type"),
                ("assets_v3_slot_created", "assets_v3 (ast_slot_created)"),
                ("assets_v3_slot_updated", "assets_v3(ast_slot_updated)"),
                ("assets_v3_specification_asset_class", "assets_v3 (ast_specification_asset_class) WHERE ast_specification_asset_class IS NOT NULL AND ast_specification_asset_class <> 'unknown'::specification_asset_class"),
                ("assets_v3_specification_version", "assets_v3 (ast_specification_version) WHERE ast_specification_version <> 'v1'::specification_versions"),
                ("assets_v3_supply", "assets_v3(ast_supply) WHERE ast_supply IS NOT NULL"),
            ]{
                self.create_index(transaction, index, on_query_string).await?;
            }
        Ok(())
    }


    pub (crate) async fn recreate_creators_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.create_index(transaction, "asset_creators_v3_creator", "asset_creators_v3(asc_creator, asc_verified)").await
    }

    pub (crate) async fn recreate_authorities_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.create_index(transaction, "assets_authority", "assets_authorities(auth_authority) WHERE auth_authority IS NOT NULL").await
    }

    pub async fn drop_fungible_constraints(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "ALTER TABLE fungible_tokens DROP CONSTRAINT IF EXISTS fungible_tokens_pkey;",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "fungible_tokens_pkey",
        )
        .await?;
        Ok(())
    }

    pub async fn recreate_fungible_constraints(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("ALTER TABLE fungible_tokens ADD CONSTRAINT fungible_tokens_pkey PRIMARY KEY (fbt_pubkey);");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "fungible_tokens_pkey",
        )
        .await?;
        Ok(())
    }

    pub async fn drop_constraints(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        for (table, constraint) in [
            ("assets_v3", "assets_v3_authority_fk_constraint"),
            ("assets_v3", "assets_v3_ast_metadata_url_id_fkey"),
            ("asset_creators_v3", "asset_creators_v3_pkey"),
            ("assets_authorities", "assets_authorities_pkey"),
            ("assets_v3", "assets_pkey"),
        ] {
            let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
                "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
                table, constraint
            ));
            self.execute_query_with_metrics(
                transaction,
                &mut query_builder,
                ALTER_ACTION,
                constraint,
            )
            .await?;
        }
        Ok(())
    }

    pub (crate) async fn recreate_asset_creators_constraints(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("ALTER TABLE asset_creators_v3 ADD CONSTRAINT asset_creators_v3_pkey PRIMARY KEY (asc_pubkey, asc_creator);");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "asset_creators_v3_pkey",
        )
        .await
    }

    pub (crate) async fn recreate_asset_authorities_constraints(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("ALTER TABLE assets_authorities ADD CONSTRAINT assets_authorities_pkey PRIMARY KEY (auth_pubkey);");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "assets_authorities_pkey",
        )
        .await
    }

    pub (crate) async fn recreate_asset_constraints(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            "ALTER TABLE assets_v3 ADD CONSTRAINT assets_pkey PRIMARY KEY (ast_pubkey);",
        );
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "assets_pkey",
        )
        .await?;
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("ALTER TABLE assets_v3 ADD CONSTRAINT assets_v3_authority_fk_constraint FOREIGN KEY (ast_authority_fk) REFERENCES assets_authorities(auth_pubkey) ON DELETE SET NULL ON UPDATE CASCADE NOT VALID;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "assets_v3_authority_fk_constraint",
        )
        .await?;
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("ALTER TABLE assets_v3 ADD CONSTRAINT assets_v3_ast_metadata_url_id_fkey FOREIGN KEY (ast_metadata_url_id) REFERENCES tasks(tsk_id) ON DELETE RESTRICT ON UPDATE CASCADE NOT VALID;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            ALTER_ACTION,
            "assets_v3_ast_metadata_url_id_fkey",
        )
        .await?;
        Ok(())
    }

    async fn create_index(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        name: &str,
        on_query_string: &str,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new("CREATE INDEX ");
        query_builder.push(name);
        query_builder.push(" ON ");
        query_builder.push(on_query_string);
        query_builder.push(";");
        self.execute_query_with_metrics(transaction, &mut query_builder, CREATE_ACTION, name)
            .await
    }

    pub async fn recreate_fungible_indexes(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> =
            QueryBuilder::new("ALTER TABLE fungible_tokens ENABLE TRIGGER ALL;");
        self.execute_query_with_metrics(
            transaction,
            &mut query_builder,
            "trigger_enable",
            "fungible_tokens",
        )
        .await?;

        for (index, on_query_string) in [
            (
                "fungible_tokens_fbt_owner_balance_idx",
                "fungible_tokens(fbt_owner, fbt_balance)",
            ),
            (
                "fungible_tokens_fbt_asset_idx",
                "fungible_tokens(fbt_asset)",
            ),
        ] {
            self.create_index(transaction, index, on_query_string)
                .await?;
        }
        Ok(())
    }

    pub async fn create_temp_tables(
        &self,
        main_table: &str,
        temp_table: &str,
        transaction: &mut Transaction<'_, Postgres>,
        drop_on_commit: bool,
    ) -> Result<(), IndexDbError> {
        let mut query_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(format!(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL)",
            temp_table, main_table
        ));
        if drop_on_commit {
            query_builder.push(" ON COMMIT DROP");
        }
        query_builder.push(";");

        self.execute_query_with_metrics(transaction, &mut query_builder, CREATE_ACTION, main_table)
            .await?;

        Ok(())
    }

    pub(crate) async fn load_through_temp_table(
        &self,
        file_path: String,
        table: &str,
        temp_postfix: &str,
        columns: &str,
        on_conflict_do_nothing: bool,
        semaphore: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<(), IndexDbError> {
        let guard = if let Some(ref s) = semaphore {
            Some(s.acquire().await.map_err(|e| {
                IndexDbError::BadArgument(format!("Failed to acquire semaphore: {}", e))
            })?)
        } else {
            None
        };
        let mut transaction = self.start_transaction().await?;
        let temp_table = format!("{}_{}", table, temp_postfix);
        match self
            .copy_through_temp_table_tx(
                &mut transaction,
                table,
                temp_table.as_str(),
                file_path,
                columns,
                on_conflict_do_nothing,
            )
            .await
        {
            Ok(_) => {
                transaction.commit().await?;
                guard.map(|g| drop(g));
                Ok(())
            }
            Err(e) => {
                transaction.rollback().await?;
                guard.map(|g| drop(g));
                Err(e)
            }
        }
    }

    async fn copy_through_temp_table_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        table: &str,
        temp_table: &str,
        file_path: String,
        columns: &str,
        on_conflict_do_nothing: bool,
    ) -> Result<(), IndexDbError> {
        self.create_temp_tables(table, temp_table, transaction, true)
            .await?;
        self.copy_table_from(transaction, file_path, temp_table, columns)
            .await?;
        self.insert_from_temp_table(transaction, table, temp_table, on_conflict_do_nothing)
            .await
    }

    pub(crate) async fn destructive_prep_to_batch_nft_load_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.drop_nft_indexes(transaction).await?;
        self.drop_constraints(transaction).await?;
        // the only constraints left are the NOT NULL constraints, which are not dropped due to the performance impact
        for table in ["assets_v3", "asset_creators_v3", "assets_authorities"] {
            self.truncate_table(transaction, table).await?;
            self.set_unlogged_on(transaction, table).await?;
            self.set_autovacuum_off_on(transaction, table).await?;
        }
        Ok(())
    }

    pub(crate) async fn finalize_assets_load(
        &self,
    ) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self.finalize_assets_load_tx(&mut transaction).await {
            Ok(_) => {
                transaction.commit().await?;
                Ok(())
            }
            Err(e) => {
                transaction.rollback().await?;
                Err(e)
            }
        }
    }

    pub(crate) async fn finalize_creators_load(
        &self,
    ) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self.finalize_creators_load_tx(&mut transaction).await {
            Ok(_) => {
                transaction.commit().await?;
                Ok(())
            }
            Err(e) => {
                transaction.rollback().await?;
                Err(e)
            }
        }
    }

    pub(crate) async fn finalize_authorities_load(
        &self,
    ) -> Result<(), IndexDbError> {
        let mut transaction = self.start_transaction().await?;
        match self.finalize_authorities_load_tx(&mut transaction).await {
            Ok(_) => {
                transaction.commit().await?;
                Ok(())
            }
            Err(e) => {
                transaction.rollback().await?;
                Err(e)
            }
        }
    }
    
    async fn finalize_assets_load_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.recreate_asset_constraints(transaction).await?;
        self.recreate_asset_indexes(transaction).await?;
        self.reset_autovacuum_on(transaction, "assets_v3").await?;
        self.set_logged_on(transaction, "assets_v3").await?;
        Ok(())
    }

    async fn finalize_creators_load_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.recreate_asset_creators_constraints(transaction).await?;
        self.recreate_creators_indexes(transaction).await?;
        self.reset_autovacuum_on(transaction, "asset_creators_v3").await?;
        self.set_logged_on(transaction, "asset_creators_v3").await?;
        Ok(())
    }

    async fn finalize_authorities_load_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.recreate_asset_authorities_constraints(transaction).await?;
        self.recreate_authorities_indexes(transaction).await?;
        self.reset_autovacuum_on(transaction, "assets_authorities").await?;
        self.set_logged_on(transaction, "assets_authorities").await?;
        Ok(())
    }

    pub(crate) async fn destructive_prep_to_batch_fungible_load_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        self.drop_fungible_indexes(transaction).await?;
        self.drop_fungible_constraints(transaction).await?;
        let table = "fungible_tokens";
        self.truncate_table(transaction, table).await?;
        self.set_unlogged_on(transaction, table).await?;
        self.set_autovacuum_off_on(transaction, table).await?;
        Ok(())
    }

    pub(crate) async fn finalize_batch_fungible_load_tx(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), IndexDbError> {
        let table = "fungible_tokens";
        self.recreate_fungible_constraints(transaction).await?;
        self.recreate_fungible_indexes(transaction).await?;
        self.reset_autovacuum_on(transaction, table).await?;
        self.set_logged_on(transaction, table).await?;
        Ok(())
    }
}
