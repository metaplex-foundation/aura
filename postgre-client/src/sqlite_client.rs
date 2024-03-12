use crate::model::{OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions};
use crate::storage_traits;
use async_trait::async_trait;
use entities::models::AssetIndex;
use hex;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use sqlx::Sqlite;
use sqlx::{
    sqlite::SqlitePoolOptions, ConnectOptions, Error, PgPool, Postgres, QueryBuilder, SqlitePool,
    Transaction,
};
use std::collections::HashSet;
use std::str::FromStr;
use std::{fs, vec};
use storage_traits::AssetIndexStorage;

pub struct SqliteClient {
    pub pool: SqlitePool,
}

impl SqliteClient {
    pub async fn new(path: String) -> Result<Self, Error> {
        let pool = SqlitePoolOptions::new()
            .connect(format!("sqlite://{}?mode=rwc", path).as_str())
            .await?;

        Ok(Self { pool })
    }

    pub async fn setup(&self) -> Result<(), Error> {
        // Run migrations or schema setup here
        run_sql_script(&self.pool, "../init_sqlite.sql").await
    }

    pub async fn setup_indexes(&self) -> Result<(), Error> {
        // Run migrations or schema setup here
        run_sql_script(&self.pool, "../init_sqlite_indexes.sql").await
    }

    pub async fn copy_all(
        &self,
        metadata_path: String,
        creators_path: String,
        assets_path: String,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), String> {
        let metadata = fs::read_to_string(metadata_path).expect("Failed to read metadata file");
        let creators = fs::read_to_string(creators_path).expect("Failed to read creators file");
        let assets = fs::read_to_string(assets_path).expect("Failed to read assets file");

        let metadata = metadata.split('\n');
        let creators = creators.split('\n');
        let assets = assets.split('\n');
        let mut metadata_id = vec![];
        for chunk in metadata.collect::<Vec<&str>>().chunks(10000) {
            let items = chunk
                .iter()
                .map(|x| x.trim().split(',').collect::<Vec<&str>>())
                .filter(|x| x.len() == 3);
            let mut query_builder: QueryBuilder<'_, Sqlite> =
                QueryBuilder::new("INSERT INTO tasks (tsk_id, tsk_metadata_url, tsk_status) ");
            query_builder.push_values(items, |mut builder, item| {
                metadata_id = hex::decode(item[0].trim().replace("\\x", "")).unwrap();
                builder.push_bind(metadata_id.clone());
                builder.push_bind(item.get(1).unwrap().trim());
                builder.push_bind(item.get(2).unwrap().trim());
            });
            self.execute_query(transaction, &mut query_builder).await?;
        }

        for chunk in assets.collect::<Vec<&str>>().chunks(32000 / 19) {
            let items = chunk
                .iter()
                .map(|x| x.trim().split(',').collect::<Vec<&str>>())
                .filter(|x| x.len() == 19);

            let mut query_builder: QueryBuilder<'_, Sqlite> = QueryBuilder::new(
                "INSERT INTO assets_v3 (
                    ast_pubkey,
                    ast_specification_version,
                    ast_specification_asset_class,
                    ast_royalty_target_type,
                    ast_royalty_amount,
                    ast_slot_created,
                    ast_owner_type,
                    ast_owner,
                    ast_delegate,
                    ast_authority,
                    ast_collection,
                    ast_is_collection_verified,
                    ast_is_burnt,
                    ast_is_compressible,
                    ast_is_compressed,
                    ast_is_frozen,
                    ast_supply,
                    ast_metadata_url_id,
                    ast_slot_updated) ",
            );
            query_builder.push_values(items, |mut builder, item| {
                builder
                    .push_bind(hex::decode(item[0].trim().replace("\\x", "")).unwrap())
                    .push_bind(item[1])
                    .push_bind(item[2])
                    .push_bind(item[3])
                    .push_bind(item[4].parse::<i64>().unwrap()) //ast_royalty_amount
                    .push_bind(item[5].parse::<i64>().unwrap()) //slot_created
                    .push_bind(item[6])
                    .push_bind(hex::decode(item[7].trim().replace("\\x", "")).unwrap())
                    .push_bind(hex::decode(item[8].trim().replace("\\x", "")).unwrap())
                    .push_bind(hex::decode(item[9].trim().replace("\\x", "")).unwrap())
                    .push_bind(hex::decode(item[10].trim().replace("\\x", "")).unwrap())
                    .push_bind(item[11].parse::<bool>().unwrap())
                    .push_bind(item[12].parse::<bool>().unwrap())
                    .push_bind(item[13].parse::<bool>().unwrap())
                    .push_bind(item[14].parse::<bool>().unwrap())
                    .push_bind(item[15].parse::<bool>().unwrap())
                    .push_bind(item[16].parse::<i64>().unwrap())
                    .push_bind(hex::decode(item[17].trim().replace("\\x", "")).unwrap())
                    .push_bind(item[18].parse::<i64>().unwrap());
            });
            query_builder.push(" ON CONFLICT (ast_pubkey) 
                DO UPDATE SET 
                    ast_specification_version = EXCLUDED.ast_specification_version,
                    ast_specification_asset_class = EXCLUDED.ast_specification_asset_class,
                    ast_royalty_target_type = EXCLUDED.ast_royalty_target_type,
                    ast_royalty_amount = EXCLUDED.ast_royalty_amount,
                    ast_slot_created = EXCLUDED.ast_slot_created,
                    ast_owner_type = EXCLUDED.ast_owner_type,
                    ast_owner = EXCLUDED.ast_owner,
                    ast_delegate = EXCLUDED.ast_delegate,
                    ast_authority = EXCLUDED.ast_authority,
                    ast_collection = EXCLUDED.ast_collection,
                    ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
                    ast_is_burnt = EXCLUDED.ast_is_burnt,
                    ast_is_compressible = EXCLUDED.ast_is_compressible,
                    ast_is_compressed = EXCLUDED.ast_is_compressed,
                    ast_is_frozen = EXCLUDED.ast_is_frozen,
                    ast_supply = EXCLUDED.ast_supply,
                    ast_metadata_url_id = EXCLUDED.ast_metadata_url_id,
                    ast_slot_updated = EXCLUDED.ast_slot_updated
                    WHERE assets_v3.ast_slot_updated <= EXCLUDED.ast_slot_updated OR assets_v3.ast_slot_updated IS NULL;");

            self.execute_query(transaction, &mut query_builder).await?;
        }
        for chunk in creators.collect::<Vec<&str>>().chunks(32000 / 4) {
            let items = chunk
                .iter()
                .map(|x| x.trim().split(',').collect::<Vec<&str>>())
                .filter(|x| x.len() == 4);

                let mut query_builder: QueryBuilder<'_, Sqlite> = QueryBuilder::new(
                    "INSERT INTO asset_creators_v3 (asc_pubkey, asc_creator, asc_verified, asc_slot_updated) ",
                );
                query_builder.push_values(items, |mut builder, item| {
                    builder
                    .push_bind(hex::decode(item[0].trim().replace("\\x", "")).unwrap())
                    .push_bind(hex::decode(item[1].trim().replace("\\x", "")).unwrap())
                    .push_bind(item[2].parse::<bool>().unwrap())
                    .push_bind(item[3].parse::<i64>().unwrap());
                });
                query_builder.push(" ON CONFLICT (asc_creator, asc_pubkey) DO UPDATE SET asc_verified = EXCLUDED.asc_verified WHERE asset_creators_v3.asc_slot_updated <= EXCLUDED.asc_slot_updated;");
        
                self.execute_query(transaction, &mut query_builder).await?;
        }

        Ok(())
    }

    pub async fn execute_query(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        query_builder: &mut QueryBuilder<'_, Sqlite>,
    ) -> Result<(), String> {
        let query = query_builder.build();
        query
            .execute(transaction)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub async fn get_metadata(&self) -> Result<Vec<TaskIdRawResponse>, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let mut query_builder: QueryBuilder<'_, Sqlite> =
            QueryBuilder::new("SELECT tsk_id FROM tasks WHERE tsk_id IS NOT NULL");
        let query = query_builder.build_query_as::<TaskIdRawResponse>();
        let rows = query.fetch_all(&mut tx).await.map_err(|e| e.to_string())?;
        Ok(rows)
    }

    async fn insert_assets(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        asset_indexes: &[AssetIndex],
        metadata_id: Vec<u8>,
    ) -> Result<(), String> {
        if asset_indexes.is_empty() {
            return Ok(());
        }
        let mut query_builder: QueryBuilder<'_, Sqlite> = QueryBuilder::new(
            "INSERT INTO assets_v3 (
            ast_pubkey,
            ast_specification_version,
            ast_specification_asset_class,
            ast_royalty_target_type,
            ast_royalty_amount,
            ast_slot_created,
            ast_owner_type,
            ast_owner,
            ast_delegate,
            ast_authority,
            ast_collection,
            ast_is_collection_verified,
            ast_is_burnt,
            ast_is_compressible,
            ast_is_compressed,
            ast_is_frozen,
            ast_supply,
            ast_metadata_url_id,
            ast_slot_updated) ",
        );
        query_builder.push_values(asset_indexes, |mut builder, asset_index| {
            builder
                .push_bind(asset_index.pubkey.to_bytes().to_vec())
                .push_bind(SpecificationVersions::from(
                    asset_index.specification_version,
                ))
                .push_bind(SpecificationAssetClass::from(
                    asset_index.specification_asset_class,
                ))
                .push_bind(RoyaltyTargetType::from(asset_index.royalty_target_type))
                .push_bind(asset_index.royalty_amount)
                .push_bind(asset_index.slot_created)
                .push_bind(asset_index.owner_type.map(OwnerType::from))
                .push_bind(asset_index.owner.map(|owner| owner.to_bytes().to_vec()))
                .push_bind(asset_index.delegate.map(|k| k.to_bytes().to_vec()))
                .push_bind(asset_index.authority.map(|k| k.to_bytes().to_vec()))
                .push_bind(asset_index.collection.map(|k| k.to_bytes().to_vec()))
                .push_bind(asset_index.is_collection_verified)
                .push_bind(asset_index.is_burnt)
                .push_bind(asset_index.is_compressible)
                .push_bind(asset_index.is_compressed)
                .push_bind(asset_index.is_frozen)
                .push_bind(asset_index.supply)
                .push_bind(metadata_id.clone())
                .push_bind(asset_index.slot_updated);
        });
        query_builder.push(" ON CONFLICT (ast_pubkey) 
        DO UPDATE SET 
            ast_specification_version = EXCLUDED.ast_specification_version,
            ast_specification_asset_class = EXCLUDED.ast_specification_asset_class,
            ast_royalty_target_type = EXCLUDED.ast_royalty_target_type,
            ast_royalty_amount = EXCLUDED.ast_royalty_amount,
            ast_slot_created = EXCLUDED.ast_slot_created,
            ast_owner_type = EXCLUDED.ast_owner_type,
            ast_owner = EXCLUDED.ast_owner,
            ast_delegate = EXCLUDED.ast_delegate,
            ast_authority = EXCLUDED.ast_authority,
            ast_collection = EXCLUDED.ast_collection,
            ast_is_collection_verified = EXCLUDED.ast_is_collection_verified,
            ast_is_burnt = EXCLUDED.ast_is_burnt,
            ast_is_compressible = EXCLUDED.ast_is_compressible,
            ast_is_compressed = EXCLUDED.ast_is_compressed,
            ast_is_frozen = EXCLUDED.ast_is_frozen,
            ast_supply = EXCLUDED.ast_supply,
            ast_metadata_url_id = EXCLUDED.ast_metadata_url_id,
            ast_slot_updated = EXCLUDED.ast_slot_updated
            WHERE assets_v3.ast_slot_updated <= EXCLUDED.ast_slot_updated OR assets_v3.ast_slot_updated IS NULL;");

        self.execute_query(transaction, &mut query_builder).await?;
        Ok(())
    }
}

#[async_trait]
impl AssetIndexStorage for SqliteClient {
    async fn fetch_last_synced_id(&self) -> Result<Option<Vec<u8>>, String> {
        todo!()
    }

    async fn update_asset_indexes_batch(
        &self,
        _asset_indexes: &[AssetIndex],
        _last_key: &[u8],
    ) -> Result<(), String> {
        todo!()
    }

    async fn load_from_dump(
        &self,
        base_path: &std::path::Path,
        last_key: &[u8],
    ) -> Result<(), String> {
        let Some(metadata_path) = base_path.join("metadata.csv").to_str().map(str::to_owned) else {
            return Err("invalid path".to_string());
        };
        let Some(creators_path) = base_path.join("creators.csv").to_str().map(str::to_owned) else {
            return Err("invalid path".to_string());
        };
        let Some(assets_path) = base_path.join("assets.csv").to_str().map(str::to_owned) else {
            return Err("invalid path".to_string());
        };

        let mut transaction = self.pool.begin().await.map_err(|e| e.to_string())?;

        self.copy_all(metadata_path, creators_path, assets_path, &mut transaction)
            .await?;
        // self.update_last_synced_key(last_key, &mut transaction)
        //     .await?;
        transaction.commit().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_existing_metadata_keys(&self) -> Result<HashSet<Vec<u8>>, String> {
        Ok(HashSet::new())
    }
}

async fn run_sql_script(pool: &SqlitePool, file_path: &str) -> Result<(), sqlx::Error> {
    let sql = fs::read_to_string(file_path).expect("Failed to read SQL file");
    for statement in sql.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }
    Ok(())
}
#[derive(sqlx::FromRow, Debug)]
pub struct TaskIdRawResponse {
    pub(crate) tsk_id: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use solana_sdk::pubkey::Pubkey;

    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sqlite_client() {
        let temp_dir = TempDir::new().expect("Failed to create a temporary directory");

        let client = SqliteClient::new(
            temp_dir
                .path()
                .join("test.db")
                .to_str()
                .unwrap()
                .to_string(),
        )
        .await
        .unwrap();
        client.setup().await.unwrap();
        let base_path = Path::new("../rocks-db/tests/data");
        client.load_from_dump(base_path, &vec![]).await.unwrap();
        client.setup_indexes().await.unwrap();
        let metadata = client.get_metadata().await.unwrap();
        assert_eq!(metadata.len(), 1);
        let decoded: Pubkey = Pubkey::try_from(metadata[0].tsk_id.as_slice()).unwrap();
        assert_eq!(
            decoded.to_string(),
            "HDNqWe2A8jsAFPHWqbCvggLzj2v3EGCxMAyeba7DJxaf"
        );
    }
}
