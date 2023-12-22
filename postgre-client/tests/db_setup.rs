use entities::enums::*;
use entities::models::{AssetIndex, Creator};
use postgre_client::storage_traits::AssetIndexStorage;
use postgre_client::PgClient;
use rand::Rng;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Executor, Pool, Postgres};
use std::fs;
use testcontainers::*;
use uuid::Uuid;

async fn run_sql_script(pool: &Pool<Postgres>, file_path: &str) -> Result<(), sqlx::Error> {
    let sql = fs::read_to_string(file_path).expect("Failed to read SQL file");
    for statement in sql.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(pool).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub async fn setup_database(
    node: &Container<'_, images::postgres::Postgres>,
) -> (Pool<Postgres>, String) {
    let default_connection_string = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)
    );

    // Connect to the default 'postgres' database to create a new database
    let default_pool = Pool::<Postgres>::connect(&default_connection_string)
        .await
        .unwrap();
    let db_name = format!("test_{}", Uuid::new_v4()).replace("-", "_");
    default_pool
        .execute(format!("CREATE DATABASE {}", db_name).as_str())
        .await
        .unwrap();

    let connection_string = format!(
        "postgres://postgres:postgres@localhost:{}/{}",
        node.get_host_port_ipv4(5432),
        db_name
    );

    let test_db_pool = Pool::<Postgres>::connect(&connection_string).await.unwrap();

    // Run migrations or schema setup here
    run_sql_script(&test_db_pool, "../init_v3.sql")
        .await
        .unwrap();
    let asset_index_storage = PgClient::new_with_pool(test_db_pool.clone());

    // Verify initial fetch_last_synced_id returns None
    assert!(asset_index_storage
        .fetch_last_synced_id()
        .await
        .unwrap()
        .is_none());

    (test_db_pool, db_name)
}

#[cfg(test)]
pub async fn teardown(node: &Container<'_, images::postgres::Postgres>, db_name: &str) {
    let default_connection_string = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)
    );

    // Connect to the default 'postgres' database to create a new database
    let default_pool = Pool::<Postgres>::connect(&default_connection_string)
        .await
        .unwrap();
    default_pool
        .execute(format!("DROP DATABASE IF EXISTS {}", db_name).as_str())
        .await
        .unwrap();
}

#[cfg(test)]
pub fn generate_random_vec(n: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let random_vector: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
    random_vector
}

#[cfg(test)]
pub fn generate_random_pubkey() -> Pubkey {
    Pubkey::new_unique()
}

#[cfg(test)]
pub fn generate_asset_index_records(n: usize) -> Vec<AssetIndex> {
    let mut asset_indexes = Vec::new();
    for i in 0..n {
        let asset_index = AssetIndex {
            pubkey: generate_random_pubkey(),
            specification_version: SpecificationVersions::V1,
            specification_asset_class: SpecificationAssetClass::Nft,
            royalty_target_type: RoyaltyTargetType::Creators,
            royalty_amount: 1,
            slot_created: (n - i) as i64,
            owner: Some(generate_random_pubkey()),
            delegate: Some(generate_random_pubkey()),
            authority: Some(generate_random_pubkey()),
            collection: Some(generate_random_pubkey()),
            is_collection_verified: Some(rand::thread_rng().gen_bool(0.5)),
            is_burnt: false,
            is_compressible: false,
            is_compressed: false,
            is_frozen: false,
            supply: Some(1),
            metadata_url: Some("https://www.google.com".to_string()),
            slot_updated: (n + 10 + i) as i64,
            creators: vec![Creator {
                creator: generate_random_pubkey(),
                creator_verified: rand::thread_rng().gen_bool(0.5),
                creator_share: 100,
            }],
            owner_type: Some(OwnerType::Single),
        };
        asset_indexes.push(asset_index);
    }
    asset_indexes
}
