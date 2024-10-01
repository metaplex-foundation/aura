use entities::enums::*;
use entities::models::{AssetIndex, Creator, UrlWithStatus};
use metrics_utils::red::RequestErrorDurationMetrics;
use postgre_client::storage_traits::AssetIndexStorage;
use postgre_client::PgClient;
use rand::Rng;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Executor, Pool, Postgres};
use std::collections::BTreeMap;
use std::sync::Arc;
use testcontainers::core::WaitFor;
use testcontainers::{Container, Image};
use testcontainers_modules::testcontainers::clients::Cli;
use uuid::Uuid;

pub struct TestEnvironment<'a> {
    pub client: Arc<PgClient>,
    pub pool: Pool<Postgres>,
    pub db_name: String,
    pub node: Container<'a, PgContainer>,
}

pub struct PgContainer {
    c: testcontainers_modules::postgres::Postgres,
    volumes: BTreeMap<String, String>,
}

impl Default for PgContainer {
    fn default() -> Self {
        Self::new()
    }
}

impl PgContainer {
    pub fn new() -> Self {
        let c = testcontainers_modules::postgres::Postgres::default();
        let volumes = BTreeMap::new();
        Self { c, volumes }
    }
    pub fn with_volume(mut self, host_path: String, container_path: String) -> Self {
        self.volumes.insert(host_path, container_path);
        self
    }
}

impl Image for PgContainer {
    type Args = ();

    fn name(&self) -> String {
        self.c.name()
    }

    fn tag(&self) -> String {
        self.c.tag()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        self.c.ready_conditions()
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        self.c.env_vars()
    }

    fn volumes(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.volumes.iter())
    }
}

impl<'a> TestEnvironment<'a> {
    pub async fn new_with_mount(cli: &'a Cli, path: &str) -> TestEnvironment<'a> {
        let image = PgContainer::new().with_volume(path.to_string(), path.to_string());

        let node = cli.run(image);
        let (pool, db_name) = setup_database(&node).await;
        let client =
            PgClient::new_with_pool(pool.clone(), Arc::new(RequestErrorDurationMetrics::new()));

        TestEnvironment {
            client: Arc::new(client),
            pool,
            db_name,
            node,
        }
    }
    pub async fn new(cli: &'a Cli) -> TestEnvironment<'a> {
        let image = PgContainer::new();

        let node = cli.run(image);
        let (pool, db_name) = setup_database(&node).await;
        let client =
            PgClient::new_with_pool(pool.clone(), Arc::new(RequestErrorDurationMetrics::new()));

        TestEnvironment {
            client: Arc::new(client),
            pool,
            db_name,
            node,
        }
    }

    pub async fn teardown(&self) {
        teardown(&self.node, &self.db_name).await;
    }

    pub async fn count_rows_in_assets(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM assets_v3")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    pub async fn count_rows_in_creators(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM asset_creators_v3")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    pub async fn count_rows_in_metadata(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tasks")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    pub async fn count_rows_in_authorities(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM assets_authorities")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }

    pub async fn count_rows_in_fungible_tokens(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM fungible_tokens")
            .fetch_one(&self.pool)
            .await?;

        Ok(count)
    }
}

pub async fn setup_database<T: Image>(node: &Container<'_, T>) -> (Pool<Postgres>, String) {
    let default_connection_string = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)
    );

    // Connect to the default 'postgres' database to create a new database
    let default_pool = Pool::<Postgres>::connect(&default_connection_string)
        .await
        .unwrap();
    let db_name = format!("test_{}", Uuid::new_v4()).replace('-', "_");
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

    let asset_index_storage = PgClient::new_with_pool(
        test_db_pool.clone(),
        Arc::new(RequestErrorDurationMetrics::new()),
    );

    asset_index_storage
        .run_migration("../migrations")
        .await
        .unwrap();

    // Verify initial fetch_last_synced_id returns None
    assert!(asset_index_storage
        .fetch_last_synced_id()
        .await
        .unwrap()
        .is_none());

    (test_db_pool, db_name)
}

pub async fn teardown<T: Image>(node: &Container<'_, T>, db_name: &str) {
    let default_connection_string = format!(
        "postgres://postgres:postgres@localhost:{}",
        node.get_host_port_ipv4(5432)
    );

    // Connect to the default 'postgres' database to create a new database
    let default_pool = Pool::<Postgres>::connect(&default_connection_string)
        .await
        .unwrap();
    if let Err(err) = default_pool
        .execute(format!("DROP DATABASE IF EXISTS {}", db_name).as_str())
        .await
    {
        println!("Failed to drop database: {}", err);
    }
}

pub fn generate_random_vec(n: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let random_vector: Vec<u8> = (0..n).map(|_| rng.gen()).collect();
    random_vector
}

pub fn generate_random_pubkey() -> Pubkey {
    Pubkey::new_unique()
}

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
            metadata_url: Some(UrlWithStatus {
                metadata_url: "https://www.google.com".to_string(),
                is_downloaded: true,
            }),
            update_authority: None,
            slot_updated: (n + 10 + i) as i64,
            creators: vec![Creator {
                creator: generate_random_pubkey(),
                creator_verified: rand::thread_rng().gen_bool(0.5),
                creator_share: 100,
            }],
            owner_type: Some(OwnerType::Single),
            fungible_tokens: vec![],
        };
        asset_indexes.push(asset_index);
    }
    asset_indexes
}
