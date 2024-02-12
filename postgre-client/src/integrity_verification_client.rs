use crate::model::VerificationRequiredField;
use crate::storage_traits::IntegrityVerificationKeysFetcher;
use crate::PgClient;
use async_trait::async_trait;
use solana_sdk::bs58;
use sqlx::{QueryBuilder, Row};

impl PgClient {
    async fn get_verification_required_keys_by_field(
        &self,
        field: VerificationRequiredField,
    ) -> Result<Vec<String>, String> {
        // Select 50 newer keys and 50 random ones
        let mut query_builder = QueryBuilder::new("WITH sorted AS (SELECT DISTINCT ON (");
        query_builder.push(&field);
        query_builder.push(") ");
        query_builder.push(&field);
        query_builder.push(" FROM (SELECT ");
        query_builder.push(&field);
        query_builder.push(" FROM assets_v3 ORDER BY ast_slot_updated DESC LIMIT 1000) sub LIMIT 50), random AS (SELECT ");
        query_builder.push(&field);
        query_builder.push(" FROM assets_v3 WHERE ");
        query_builder.push(&field);
        query_builder.push(" IS NOT NULL AND ");
        query_builder.push(&field);
        query_builder.push(" NOT IN (SELECT ");
        query_builder.push(&field);
        query_builder.push(" FROM sorted) ORDER BY RANDOM() LIMIT 50) SELECT ");
        query_builder.push(&field);
        query_builder.push(" FROM sorted UNION ALL SELECT ");
        query_builder.push(&field);
        query_builder.push(" FROM random");
        let query = query_builder.build();

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let key: Vec<u8> = row.get(field.to_string().as_str());
                bs58::encode(key.as_slice()).into_string()
            })
            .collect::<Vec<_>>())
    }

    async fn get_verification_required_keys(
        &self,
        proof_check: bool,
    ) -> Result<Vec<String>, String> {
        // select 50 newer keys and 50 random ones
        let mut query_builder =
            QueryBuilder::new("WITH sorted AS (SELECT ast_pubkey FROM assets_v3 ");
        if proof_check {
            query_builder.push(" WHERE ast_is_compressed IS TRUE ");
        }
        query_builder.push(" ORDER BY ast_slot_updated DESC LIMIT 50), random AS (SELECT ast_pubkey FROM assets_v3 WHERE ");
        if proof_check {
            query_builder.push(" ast_is_compressed IS TRUE AND ");
        }
        query_builder.push(" ast_pubkey NOT IN (SELECT ast_pubkey FROM sorted) ORDER BY RANDOM() LIMIT 50) SELECT ast_pubkey FROM sorted UNION ALL SELECT ast_pubkey FROM random");

        let rows = query_builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let key: Vec<u8> = row.get("ast_pubkey");
                bs58::encode(key.as_slice()).into_string()
            })
            .collect::<Vec<_>>())
    }
}

#[async_trait]
impl IntegrityVerificationKeysFetcher for PgClient {
    async fn get_verification_required_owners_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys_by_field(VerificationRequiredField::Owner)
            .await
    }

    async fn get_verification_required_creators_keys(&self) -> Result<Vec<String>, String> {
        // Select 50 newer keys and 50 random ones
        let query = "WITH sorted AS (
                SELECT DISTINCT ON (asc_creator) asc_creator
                FROM (
                    SELECT asc_creator
                    FROM asset_creators_v3
                    INNER JOIN assets_v3 ON asc_pubkey = ast_pubkey
                    ORDER BY ast_slot_updated DESC
                    LIMIT 1000
                ) sub
                LIMIT 50
            ),
            random AS (
                SELECT asc_creator
                FROM asset_creators_v3
                WHERE asc_creator NOT IN (SELECT asc_creator FROM sorted)
                ORDER BY RANDOM()
                LIMIT 50
            )
            SELECT asc_creator FROM sorted
            UNION ALL
            SELECT asc_creator FROM random";

        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let creator: Vec<u8> = row.get("asc_creator");
                bs58::encode(creator.as_slice()).into_string()
            })
            .collect::<Vec<_>>())
    }

    async fn get_verification_required_authorities_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys_by_field(VerificationRequiredField::Authority)
            .await
    }

    async fn get_verification_required_groups_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys_by_field(VerificationRequiredField::Group)
            .await
    }

    async fn get_verification_required_assets_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys(false).await
    }

    async fn get_verification_required_assets_proof_keys(&self) -> Result<Vec<String>, String> {
        self.get_verification_required_keys(true).await
    }
}
