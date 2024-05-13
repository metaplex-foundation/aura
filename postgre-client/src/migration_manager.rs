use crate::{PgClient, INSERT_ACTION, SELECT_ACTION, SQL_COMPONENT};
use async_trait::async_trait;
use interface::migration_version_manager::MigrationVersionManager;
use sqlx::{QueryBuilder, Row};
use std::collections::HashSet;

#[async_trait]
impl MigrationVersionManager for PgClient {
    async fn get_all_applied_migrations(&self) -> Result<HashSet<u64>, String> {
        let mut query_builder =
            QueryBuilder::new("SELECT rm_migration_version FROM rocks_migrations");
        let start_time = chrono::Utc::now();
        let query = query_builder.build();
        let result = query
            .fetch_all(&self.pool)
            .await
            .map(|rows| {
                rows.iter().fold(HashSet::new(), |mut acc, row| {
                    let version = row.get::<i64, _>("rm_migration_version");
                    acc.insert(version as u64);
                    acc
                })
            })
            .map_err(|e| {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "rocks_migrations");
                e.to_string()
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, SELECT_ACTION, "rocks_migrations", start_time);

        Ok(result)
    }

    async fn apply_migration(&self, version: u64) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO rocks_migrations (
                rm_migration_version
            ) VALUES ($1) ON CONFLICT (rm_migration_version) DO NOTHING;",
        );

        let query = query_builder.build();
        query
            .bind(version as i64)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                self.metrics
                    .observe_error(SQL_COMPONENT, INSERT_ACTION, "rocks_migrations");
                format!("Insert rollup: {}", err)
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, INSERT_ACTION, "rocks_migrations", start_time);

        Ok(())
    }
}
