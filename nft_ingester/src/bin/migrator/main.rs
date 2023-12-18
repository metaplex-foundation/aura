use log::error;
use nft_ingester::config::{setup_config, IngesterConfig};
use nft_ingester::db_v2::DBClient;
use nft_ingester::error::IngesterError;
use rocks_db::offchain_data::OffChainData;
use rocks_db::Storage;
use sqlx::{Executor, QueryBuilder, Row};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Instant;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config: IngesterConfig = setup_config();
    let database_pool = DBClient::new(&config.database_config.clone()).await?;

    let storage = Storage::open(
        &config
            .rocks_db_path_container
            .clone()
            .unwrap_or("./my_rocksdb".to_string()),
    )
    .unwrap();

    let rocks_db = Arc::new(storage);

    migrate_data(
        database_pool.clone(),
        rocks_db.clone(),
        config.migration_batch_size.unwrap_or(50_000),
        config.migrator_workers.unwrap_or(10),
    )
    .await?;

    Ok(())
}

#[derive(Clone)]
pub struct MigrateJson {
    pub pubkey: i64,
    pub metadata_url: String,
    pub metadata: String,
}

async fn migrate_data(
    pg_pool: DBClient,
    rocks_db: Arc<Storage>,
    limit: u32,
    max_concurrent_tasks: u32,
) -> Result<(), IngesterError> {
    let mut last_id = 0;
    let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks as usize));

    loop {
        let mut query = QueryBuilder::new(
            "SELECT
                ofd_pubkey,
                ofd_metadata_url,
                ofd_metadata
             FROM offchain_data
             WHERE ofd_pubkey > ",
        );

        query.push_bind(last_id);
        query.push(" ORDER BY ofd_pubkey LIMIT ");
        query.push_bind(limit as i64);

        let query = query.build();
        let select_tine = Instant::now();

        let rows = query
            .fetch_all(&pg_pool.pool)
            .await
            .map_err(|err| IngesterError::DatabaseError(format!("Get old db batch: {}", err)))?;

        let metadata: Vec<MigrateJson> = rows
            .iter()
            .map(|q| MigrateJson {
                pubkey: q.get("ofd_pubkey"),
                metadata_url: q.get("ofd_metadata_url"),
                metadata: q.get("ofd_metadata"),
            })
            .collect();

        println!(
            "select items: {}, time elapsed ms: {}, limit: {}, last_id: {:?}",
            metadata.len(),
            select_tine.elapsed().as_millis(),
            limit,
            last_id.clone(),
        );

        if metadata.is_empty() {
            println!("FINAL");
            break;
        }

        let mut tasks = Vec::new();

        let insert_tine = Instant::now();

        for m in &metadata.clone() {
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            let m = m.to_owned();
            let rocks_clone = rocks_db.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = permit;

                if &m.metadata == "processing" {
                    return;
                }
                match rocks_clone.asset_offchain_data.put(
                    m.metadata_url.clone(),
                    &OffChainData {
                        url: m.metadata_url.clone(),
                        metadata: m.metadata.clone(),
                    },
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("offchain_data.put: {}", e)
                    }
                };
            }));
        }

        for task in tasks {
            task.await.unwrap_or_else(|e| error!("{}", e));
        }

        println!(
            "insert time elapsed ms: {}",
            insert_tine.elapsed().as_millis()
        );

        last_id = match metadata.last() {
            None => 0,
            Some(asset) => asset.clone().pubkey,
        };
    }

    Ok(())
}
