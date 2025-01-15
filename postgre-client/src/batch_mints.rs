use chrono::Utc;
use entities::models::BatchMintWithState;
use sqlx::{database::HasArguments, query::Query, Postgres, QueryBuilder, Row};

use crate::{
    model::BatchMintState, PgClient, INSERT_ACTION, SELECT_ACTION, SQL_COMPONENT, UPDATE_ACTION,
};

impl PgClient {
    pub async fn insert_new_batch_mint(&self, file_path: &str) -> Result<(), String> {
        let start_time = Utc::now();
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO batch_mints (
                btm_file_name,
                btm_state
            ) VALUES ($1, $2) ON CONFLICT (btm_file_name) DO NOTHING;",
        );

        let query = query_builder.build();
        query.bind(file_path).bind(BatchMintState::Uploaded).execute(&self.pool).await.map_err(
            |err| {
                self.metrics.observe_error(SQL_COMPONENT, INSERT_ACTION, "batch_mints");
                format!("Insert batch_mint: {}", err)
            },
        )?;

        self.metrics.observe_request(SQL_COMPONENT, INSERT_ACTION, "batch_mints", start_time);

        Ok(())
    }

    pub async fn fetch_batch_mint_for_processing(
        &self,
    ) -> Result<Option<BatchMintWithState>, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT btm_file_name, btm_state, btm_error, btm_url, EXTRACT(EPOCH FROM btm_created_at) as created_at FROM batch_mints
            WHERE btm_state in ('uploaded', 'validation_complete', 'fail_upload_to_arweave', 'uploaded_to_arweave', 'fail_sending_transaction') ORDER BY btm_created_at ASC"
        );
        self.fetch_batch_mint(query_builder.build()).await
    }

    pub async fn get_batch_mint_by_url(
        &self,
        url: &str,
    ) -> Result<Option<BatchMintWithState>, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT btm_file_name, btm_state, btm_error, btm_url, EXTRACT(EPOCH FROM btm_created_at) as created_at FROM batch_mints
            WHERE btm_url = $1"
        );
        self.fetch_batch_mint(query_builder.build().bind(url)).await
    }

    pub async fn mark_batch_mint_as_failed(
        &self,
        file_path: &str,
        error_message: &str,
        state: BatchMintState,
    ) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "UPDATE batch_mints SET btm_state = $1, btm_error = $2 WHERE btm_file_name = $3",
        );
        self.update_batch_mint(
            query_builder.build().bind(state).bind(error_message).bind(file_path),
        )
        .await
    }

    pub async fn set_batch_mint_url_and_reward(
        &self,
        file_path: &str,
        url: &str,
        reward: i64,
    ) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "UPDATE batch_mints SET btm_url = $1, btm_tx_reward = $2, btm_state = $3 WHERE btm_file_name = $4",
        );
        self.update_batch_mint(
            query_builder
                .build()
                .bind(url)
                .bind(reward)
                .bind(BatchMintState::UploadedToArweave)
                .bind(file_path),
        )
        .await
    }

    pub async fn update_batch_mint_state(
        &self,
        file_path: &str,
        state: BatchMintState,
    ) -> Result<(), String> {
        let mut query_builder =
            QueryBuilder::new("UPDATE batch_mints SET btm_state = $1 WHERE btm_file_name = $2");
        self.update_batch_mint(query_builder.build().bind(state).bind(file_path)).await
    }

    async fn fetch_batch_mint(
        &self,
        query: Query<'_, Postgres, <Postgres as HasArguments<'_>>::Arguments>,
    ) -> Result<Option<BatchMintWithState>, String> {
        let start_time = chrono::Utc::now();
        let result = query
            .fetch_optional(&self.pool)
            .await
            .map(|row| {
                row.map(|row| BatchMintWithState {
                    file_name: row.try_get("btm_file_name").unwrap_or_default(),
                    state: row
                        .try_get::<BatchMintState, _>("btm_state")
                        .unwrap_or(BatchMintState::Uploaded)
                        .into(),
                    error: row.try_get("btm_error").ok(),
                    url: row.try_get("btm_url").ok(),
                    created_at: row
                        .try_get::<f64, _>("created_at")
                        .map(|sec| sec as u64)
                        .unwrap_or_default(),
                })
            })
            .map_err(|e| {
                self.metrics.observe_error(SQL_COMPONENT, SELECT_ACTION, "batch_mints");
                e.to_string()
            })?;

        self.metrics.observe_request(SQL_COMPONENT, SELECT_ACTION, "batch_mints", start_time);

        Ok(result)
    }

    async fn update_batch_mint(
        &self,
        query: Query<'_, Postgres, <Postgres as HasArguments<'_>>::Arguments>,
    ) -> Result<(), String> {
        let start_time = chrono::Utc::now();
        let result = query.execute(&self.pool).await.map_err(|e| {
            self.metrics.observe_error(SQL_COMPONENT, UPDATE_ACTION, "batch_mints");
            e.to_string()
        })?;

        self.metrics.observe_request(SQL_COMPONENT, UPDATE_ACTION, "batch_mints", start_time);

        if result.rows_affected() == 0 {
            return Err("No batch_mint updated".to_string());
        }
        Ok(())
    }
}
