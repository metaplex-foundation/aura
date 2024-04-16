use crate::model::RollupState;
use crate::{PgClient, SELECT_ACTION, SQL_COMPONENT, UPDATE_ACTION};
use entities::models::RollupWithState;
use sqlx::{QueryBuilder, Row};

impl PgClient {
    pub async fn insert_new_rollup(&self, file_path: &str) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO rollups (
                rlp_file_name,
                rlp_state
            ) ",
        );

        query_builder.push_bind(file_path);
        query_builder.push_bind(RollupState::Uploaded);
        query_builder.push(" ON CONFLICT (rlp_file_name) DO NOTHING;");

        let query = query_builder.build();
        query
            .execute(&self.pool)
            .await
            .map_err(|err| format!("Insert rollup: {}", err))?;

        Ok(())
    }

    pub async fn fetch_rollup_for_processing(&self) -> Result<RollupWithState, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT rlp_file_name, rlp_state, rlp_error, rlp_url, EXTRACT(EPOCH FROM rlp_created_at) as created_at FROM rollups
            WHERE rlp_state = $1 ORDER BY rlp_created_at ASC"
        );
        let start_time = chrono::Utc::now();
        let query = query_builder.build();
        let result = query
            .bind(RollupState::Uploaded)
            .fetch_one(&self.pool)
            .await
            .map(|row| RollupWithState {
                file_path: row.try_get("rlp_file_name").unwrap_or_default(),
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
            .map_err(|e| {
                self.metrics
                    .observe_error(SQL_COMPONENT, SELECT_ACTION, "rollups");
                e.to_string()
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, SELECT_ACTION, "rollups", start_time);

        Ok(result)
    }

    pub async fn mark_rollup_as_verification_failed(
        &self,
        file_path: &str,
        error_message: &str,
    ) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "UPDATE rollups SET rlp_state = $1, rlp_error = $2 WHERE rlp_file_name = $3",
        );
        let start_time = chrono::Utc::now();
        let query = query_builder.build();
        let result = query
            .bind(RollupState::ValidationFail)
            .bind(error_message)
            .bind(file_path)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                self.metrics
                    .observe_error(SQL_COMPONENT, UPDATE_ACTION, "rollups");
                e.to_string()
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, UPDATE_ACTION, "rollups", start_time);

        if result.rows_affected() == 0 {
            return Err("No rollup updated; the file path may not exist.".to_string());
        }
        Ok(())
    }

    pub async fn mark_rollup_as_complete(&self, file_path: &str, url: &str) -> Result<(), String> {
        let mut query_builder = QueryBuilder::new(
            "UPDATE rollups SET rlp_state = $1, rlp_url = $2 WHERE rlp_file_name = $3",
        );
        let start_time = chrono::Utc::now();
        let query = query_builder.build();
        let result = query
            .bind(RollupState::Complete)
            .bind(url)
            .bind(file_path)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                self.metrics
                    .observe_error(SQL_COMPONENT, UPDATE_ACTION, "rollups");
                e.to_string()
            })?;

        self.metrics
            .observe_request(SQL_COMPONENT, UPDATE_ACTION, "rollups", start_time);

        if result.rows_affected() == 0 {
            return Err("No rollup updated; the file path may not exist or it's already marked as complete.".to_string());
        }
        Ok(())
    }
}

// #[cfg(test)]
// #[tokio::test]
// async fn test_insert_new_rollup() {
//     let cli = Cli::default();
//     let env = TestEnvironment::new(&cli).await;
// }
