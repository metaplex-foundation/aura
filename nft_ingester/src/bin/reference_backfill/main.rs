use metrics_utils::red::RequestErrorDurationMetrics;
use nft_ingester::error::IngesterError;
use postgre_client::PgClient;
use rocks_db::Storage;
use std::env;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

const CONTAINER_DUMP_DIR: &str = "/usr/src/dump";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    // Retrieve the database paths from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!(
            "Usage: {} <source_db_path> <reference_db_url> <dump_dir_path>",
            args[0]
        );
        std::process::exit(1);
    }
    let source_db_path = &args[1];
    let reference_db_url = &args[2];
    let dump_dir_path = &args[3];

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let secondary_rocks_dir = TempDir::new().unwrap();
    let source_storage = Storage::open_secondary(
        source_db_path,
        secondary_rocks_dir.path().to_str().unwrap(),
        mutexed_tasks.clone(),
        red_metrics.clone(),
    )
    .unwrap();
    let (_, shutdown_rx) = broadcast::channel::<()>(1);

    let reference_storage =
        Arc::new(PgClient::new(reference_db_url, 100, 250, red_metrics.clone()).await?);
    // let dump_dir = TempDir::new().unwrap();
    println!("Starting creating dump...");
    source_storage
        .dump_reference_db(
            std::path::Path::new(dump_dir_path.as_str()),
            200_000,
            &shutdown_rx.resubscribe(),
        )
        .await
        .unwrap();
    println!("Starting insert dump...");
    reference_storage
        .load_from_reference_dump(std::path::Path::new(CONTAINER_DUMP_DIR))
        .await
        .unwrap();

    Ok(())
}
