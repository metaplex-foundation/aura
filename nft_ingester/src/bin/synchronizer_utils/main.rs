use clap::Parser;
use itertools::Itertools;
use nft_ingester::error::IngesterError;
use rocks_db::key_encoders::decode_u64x2_pubkey;
use rocks_db::migrator::MigrationState;
use rocks_db::storage_traits::AssetIndexReader;
use rocks_db::storage_traits::AssetUpdateIndexStorage;
use rocks_db::Storage;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the RocksDB instance with slots
    #[arg(short, long)]
    db_path: PathBuf,

    /// List of Pubkeys to fetch from the RPC and ingest into the RocksDB
    #[arg(short, long, value_delimiter = ',', num_args = 0..)]
    pubkeys_to_parse: Option<Vec<String>>,

    #[arg(short, long)]
    index_after: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();
    info!("Starting sync util...");

    let args = Args::parse();

    let tx_storage_dir = tempfile::TempDir::new().unwrap();

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());

    let storage = Storage::open_secondary(
        &args.db_path,
        &tx_storage_dir.path().to_path_buf(),
        mutexed_tasks.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();
    if let Some(index_after) = args.index_after {
        // Decode the Base58 string back into Vec<u8>
        let decoded_data = bs58::decode(&index_after)
            .into_vec()
            .expect("index after should be base58 encoded");
        let starting_key = decode_u64x2_pubkey(decoded_data).expect("Failed to decode index after");
        let (updated_keys, last_included_key) = storage
            .fetch_asset_updated_keys(Some(starting_key), None, 500, None)
            .unwrap();
        let index = storage
            .get_asset_indexes(updated_keys.into_iter().collect_vec().as_slice())
            .await
            .expect("Failed to get indexes");
        println!("{:?}", index);
    }
    Ok(())
}
