use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{command, Parser};
use nft_ingester::config::{
    init_logger, setup_config, SynchronizerConfig, SYNCHRONIZER_CONFIG_PREFIX,
};
use nft_ingester::error::IngesterError;
use nft_ingester::index_syncronizer::Synchronizer;
use nft_ingester::init::{graceful_stop, init_index_storage_with_migration};
use postgre_client::PG_MIGRATIONS_PATH;
use prometheus_client::registry::Registry;

use metrics_utils::utils::setup_metrics;
use metrics_utils::SynchronizerMetricsConfig;
use rocks_db::key_encoders::encode_u64x2_pubkey;
use rocks_db::migrator::MigrationState;
use rocks_db::storage_traits::{AssetUpdateIndexStorage, Dumper};
use rocks_db::Storage;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";
pub const DEFAULT_SECONDARY_ROCKSDB_PATH: &str = "./my_rocksdb_secondary";
pub const DEFAULT_MAX_POSTGRES_CONNECTIONS: u32 = 100;
pub const DEFAULT_MIN_POSTGRES_CONNECTIONS: u32 = 100;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the RocksDB instance
    #[arg(short, long)]
    source_path: PathBuf,

    /// Buffer capacity for the dumper
    /// The dumper will wait until the buffer is full before writing to the CSV files
    /// This is to reduce the number of writes to the disk
    #[arg(short, long, default_value = "33554432")]
    buffer_capacity: usize,

    /// Limit the number of assets to dump
    /// If not set, all assets will be dumped
    /// If set, the dumper will stop after dumping the specified number of assets
    /// This is useful for testing
    #[arg(short, long)]
    limit: Option<usize>,

    /// Path to dump the CSV files
    /// If not set, the CSV files will be dumped to a temporary directory
    #[arg(short, long)]
    dump_path: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();
    tracing::info!("Starting Synchronizer server...");
    let args = Args::parse();
    let start_time = std::time::Instant::now();

    let secondary_storage_path = tempfile::TempDir::new().unwrap().path().to_path_buf();
    let metrics = Arc::new(SynchronizerMetricsConfig::new());
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let rocks_storage = Arc::new(
        Storage::open_secondary(
            &args.source_path,
            &secondary_storage_path,
            mutexed_tasks.clone(),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap(),
    );

    let cloned_tasks = mutexed_tasks.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    mutexed_tasks.lock().await.spawn(async move {
        // --stop
        graceful_stop(
            cloned_tasks,
            shutdown_tx,
            None,
            None,
            None,
            "",
        )
        .await;

        Ok(())
    });
    if let Err(e) = rocks_storage.db.try_catch_up_with_primary() {
        tracing::error!("Sync rocksdb error: {}", e);
    }

    tracing::info!("Preparation took {:?}", start_time.elapsed());
    let start_time = std::time::Instant::now();

    let Some(last_known_key) = rocks_storage.last_known_asset_updated_key()? else {
        return Ok(());
    };
    let last_included_rocks_key = encode_u64x2_pubkey(
        last_known_key.seq,
        last_known_key.slot,
        last_known_key.pubkey,
    );
    let base_path = args
        .dump_path
        .unwrap_or_else(|| tempfile::TempDir::new().unwrap().path().to_path_buf());

    let metadata_path = base_path
        .join("metadata.csv")
        .to_str()
        .map(str::to_owned)
        .unwrap();
    let creators_path = base_path
        .join("creators.csv")
        .to_str()
        .map(str::to_owned)
        .unwrap();
    let assets_path = base_path
        .join("assets.csv")
        .to_str()
        .map(str::to_owned)
        .unwrap();
    let authorities_path = base_path
        .join("assets_authorities.csv")
        .to_str()
        .map(str::to_owned)
        .unwrap();
    let fungible_tokens_path = base_path
        .join("fungible_tokens.csv")
        .to_str()
        .map(str::to_owned)
        .unwrap();
    tracing::info!(
            "Dumping to metadata: {:?}, creators: {:?}, assets: {:?}, authorities: {:?}, fungible_tokens: {:?}",
            metadata_path,
            creators_path,
            assets_path,
            authorities_path,
            fungible_tokens_path
        );

    let metadata_file = File::create(metadata_path.clone())
        .map_err(|e| format!("Could not create file for metadata dump: {}", e))?;
    let assets_file = File::create(assets_path.clone())
        .map_err(|e| format!("Could not create file for assets dump: {}", e))?;
    let creators_file = File::create(creators_path.clone())
        .map_err(|e| format!("Could not create file for creators dump: {}", e))?;
    let authority_file = File::create(authorities_path.clone())
        .map_err(|e| format!("Could not create file for authority dump: {}", e))?;
    let fungible_tokens_file = File::create(fungible_tokens_path.clone())
        .map_err(|e| format!("Could not create file for fungible tokens dump: {}", e))?;

    rocks_storage
        .dump_csv(
            (metadata_file, metadata_path),
            (assets_file, assets_path),
            (creators_file, creators_path),
            (authority_file, authorities_path),
            (fungible_tokens_file, fungible_tokens_path),
            0,
            args.buffer_capacity,
            args.limit,
            &shutdown_rx,
            metrics,
        )
        .await?;
    let duration = start_time.elapsed();
    tracing::info!(
        "Dumping of {} assets took {:?}, average rate: {:.2} assets/s",
        args.limit.unwrap_or(0),
        duration,
        args.limit.unwrap_or(0) as f64 / duration.as_secs_f64()
    );
    Ok(())
}
