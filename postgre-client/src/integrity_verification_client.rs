use crate::storage_traits::IntegrityVerificationKeysFetcher;
use crate::PgClient;
use async_trait::async_trait;
use base58::ToBase58;
use sqlx::Row;
use std::collections::HashSet;

impl PgClient {
    async fn get_verification_required_keys_by_field(
        &self,
        field: &str,
    ) -> Result<Vec<String>, String> {
        // Select 50 newer keys and 50 random ones
        let query = &format!(
            "WITH sorted AS (
                SELECT DISTINCT ON (ast_slot_updated) {0}
                FROM assets_v3
                WHERE {0} IS NOT NULL
                ORDER BY ast_slot_updated DESC
                LIMIT 50
            ),
            random AS (
                SELECT {0}
                FROM assets_v3
                WHERE {0} IS NOT NULL
                  AND {0} NOT IN (SELECT {0} FROM sorted)
                ORDER BY RANDOM() LIMIT 50
            )
            SELECT {0} FROM sorted
            UNION ALL
            SELECT {0} FROM random",
            field
        );

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        let mut result = rows
            .into_iter()
            .map(|row| {
                let key: Vec<u8> = row.get(field);
                key.to_base58()
            })
            .collect();

        // Query can return duplicates,
        // so filtering out them from final result.
        // Do not filtered out them in query stage in
        // purpose not to slow down execution time
        let mut seen = HashSet::new();
        Ok(result.retain(|e| seen.insert(e.clone())))
    }

    async fn get_verification_required_keys(&self) -> Result<Vec<String>, String> {
        // select 50 newer keys and 50 random ones
        let query = "WITH sorted AS (
                SELECT ast_pubkey
                FROM assets_v3
                ORDER BY ast_slot_updated DESC
                LIMIT 50
            ),
            random AS (
                SELECT ast_pubkey
                FROM assets_v3
                  where ast_pubkey NOT IN (SELECT ast_pubkey FROM sorted)
                ORDER BY RANDOM() LIMIT 50
            )
            SELECT ast_pubkey FROM sorted
            UNION ALL
            SELECT ast_pubkey FROM random";

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let key: Vec<u8> = row.get("ast_pubkey");
                key.to_base58()
            })
            .collect())
    }
}

#[async_trait]
impl IntegrityVerificationKeysFetcher for PgClient {
    async fn get_verification_required_owners_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys_by_field("ast_owner")
            .await
    }

    async fn get_verification_required_creators_keys(&self) -> Result<Vec<String>, String> {
        // Select 50 newer keys and 50 random ones
        let query = "WITH sorted AS (
                SELECT DISTINCT ON (asc_slot_updated) asc_creator
                FROM asset_creators_v3
                ORDER BY asc_slot_updated DESC
                LIMIT 50
            ),
            random AS (
                SELECT asc_creator
                FROM asset_creators_v3
                WHERE asc_creator NOT IN (SELECT asc_creator FROM sorted)
                ORDER BY RANDOM() LIMIT 50
            )
            SELECT asc_creator FROM sorted
            UNION ALL
            SELECT asc_creator FROM random";

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        let mut result = rows
            .into_iter()
            .map(|row| {
                let owner: Vec<u8> = row.get("asc_creator");
                owner.to_base58()
            })
            .collect();

        // Query can return duplicates,
        // so filtering out them from final result.
        // Do not filtered out them in query stage in
        // purpose not to slow down execution time
        let mut seen = HashSet::new();
        Ok(result.retain(|e| seen.insert(e.clone())))
    }

    async fn get_verification_required_authorities_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys_by_field("ast_authority")
            .await
    }

    async fn get_verification_required_collections_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys_by_field("ast_collection")
            .await
    }

    async fn get_verification_required_assets_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys().await
    }

    async fn get_verification_required_assets_proof_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys().await
    }
}
