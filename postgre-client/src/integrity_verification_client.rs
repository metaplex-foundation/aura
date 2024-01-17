use crate::storage_traits::IntegrityVerificationKeysFetcher;
use crate::PgClient;
use async_trait::async_trait;
use base58::ToBase58;
use sqlx::Row;

impl PgClient {
    async fn get_verification_required_keys_by_field(
        &self,
        field: &str,
    ) -> Result<Vec<String>, String> {
        let query = &format!(
            "
            WITH first_selection AS (
                SELECT {0} FROM (
                    SELECT {0}, ROW_NUMBER() OVER (PARTITION BY ast_slot_updated ORDER BY ast_slot_updated DESC) as rn
                    FROM assets_v3
                    WHERE {0} IS NOT NULL
                ) sub
                WHERE sub.rn = 1
                LIMIT 50
            ),
            additional_selection AS (
                SELECT {0}
                FROM assets_v3
                WHERE {0} IS NOT NULL
                  AND {0} NOT IN (SELECT {0} FROM first_selection)
                ORDER BY RANDOM() LIMIT 50
            )
            SELECT {0} FROM first_selection
            UNION ALL
            SELECT {0} FROM additional_selection
        ",
            field
        );

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let owner: Vec<u8> = row.get(field);
                owner.to_base58()
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
        todo!()
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
        todo!()
    }

    async fn get_verification_required_assets_proof_keys(&self) -> Result<Vec<String>, String> {
        todo!()
    }
}
