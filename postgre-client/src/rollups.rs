use crate::model::RollupState;
use crate::{PgClient, INSERT_ACTION, SELECT_ACTION, SQL_COMPONENT, UPDATE_ACTION};
use chrono::Utc;
use entities::models::BatchMintWithState;
use sqlx::database::HasArguments;
use sqlx::query::Query;
use sqlx::{Postgres, QueryBuilder, Row};

impl PgClient {
    pub async fn insert_new_rollup(&self, file_path: &str) -> Result<(), String> {
        let start_time = Utc::now();
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO rollups (
                rlp_file_name,
                rlp_state
            ) VALUES ($1, $2) ON CONFLICT (rlp_file_name) DO NOTHING;",
        );

        let query = query_builder.build();
        query
            .bind(file_path)
            .bind(RollupState::Uploaded)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                self.metrics
                    .observe_error(SQL_COMPONENT, INSERT_ACTION, "rollups");
                format!("Insert rollup: {}", err)
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, INSERT_ACTION, "rollups", start_time);

        Ok(())
    }

    pub async fn fetch_rollup_for_processing(&self) -> Result<Option<BatchMintWithState>, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT rlp_file_name, rlp_state, rlp_error, rlp_url, EXTRACT(EPOCH FROM rlp_created_at) as created_at FROM rollups
            WHERE rlp_state in ('uploaded', 'validation_complete', 'fail_upload_to_arweave', 'uploaded_to_arweave', 'fail_sending_transaction') ORDER BY rlp_created_at ASC"
        );
        self.fetch_rollup(query_builder.build()).await
    }

    pub async fn get_rollup_by_url(&self, url: &str) -> Result<Option<BatchMintWithState>, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT rlp_file_name, rlp_state, rlp_error, rlp_url, EXTRACT(EPOCH FROM rlp_created_at) as created_at FROM rollups
            WHERE rlp_url = $1"
        );
        self.fetch_rollup(query_builder.build().bind(url)).await
    }

    pub async fn mark_rollup_as_failed(
        &self,
        file_path: &str,
        error_message: &str,
        state: RollupState,
    ) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "UPDATE rollups SET rlp_state = $1, rlp_error = $2 WHERE rlp_file_name = $3",
        );
        self.update_rollup(
            query_builder
                .build()
                .bind(state)
                .bind(error_message)
                .bind(file_path),
        )
        .await
    }

    pub async fn set_rollup_url_and_reward(
        &self,
        file_path: &str,
        url: &str,
        reward: i64,
    ) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "UPDATE rollups SET rlp_url = $1, rlp_tx_reward = $2, rlp_state = $3 WHERE rlp_file_name = $4",
        );
        self.update_rollup(
            query_builder
                .build()
                .bind(url)
                .bind(reward)
                .bind(RollupState::UploadedToArweave)
                .bind(file_path),
        )
        .await
    }

    pub async fn update_rollup_state(
        &self,
        file_path: &str,
        state: RollupState,
    ) -> Result<(), String> {
        let mut query_builder =
            QueryBuilder::new("UPDATE rollups SET rlp_state = $1 WHERE rlp_file_name = $2");
        self.update_rollup(query_builder.build().bind(state).bind(file_path))
            .await
    }

    async fn fetch_rollup(
        &self,
        query: Query<'_, Postgres, <Postgres as HasArguments<'_>>::Arguments>,
    ) -> Result<Option<BatchMintWithState>, String> {
        let start_time = chrono::Utc::now();
        let result = query
            .fetch_optional(&self.pool)
            .await
            .map(|row| {
                row.map(|row| BatchMintWithState {
                    file_name: row.try_get("rlp_file_name").unwrap_or_default(),
                    state: row
                        .try_get::<RollupState, _>("rlp_state")
                        .unwrap_or(RollupState::Uploaded)
                        .into(),
                    error: row.try_get("rlp_error").ok(),
                    url: row.try_get("rlp_url").ok(),
                    created_at: row
                        .try_get::<f64, _>("created_at")
                        .map(|sec| sec as u64)
                        .unwrap_or_default(),
                })
            })
            .map_err(|e| {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "rollups");
                e.to_string()
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, SELECT_ACTION, "rollups", start_time);

        Ok(result)
    }

    async fn update_rollup(
        &self,
        query: Query<'_, Postgres, <Postgres as HasArguments<'_>>::Arguments>,
    ) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        let result = query.execute(&self.pool).await.map_err(|e| {
            self.metrics
                .observe_error(SQL_COMPONENT, UPDATE_ACTION, "rollups");
            e.to_string()
        })?;

        self.metrics
            .observe_request(SQL_COMPONENT, UPDATE_ACTION, "rollups", start_time);

        if result.rows_affected() == 0 {
            return Err("No rollup updated".to_string());
        }
        Ok(())
    }
}
