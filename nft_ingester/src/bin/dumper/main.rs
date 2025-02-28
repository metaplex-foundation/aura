use std::{fs::File, path::PathBuf, sync::Arc};

use clap::{command, Parser};
use metrics_utils::SynchronizerMetricsConfig;
use nft_ingester::{error::IngesterError, index_synchronizer::shard_pubkeys};
use rocks_db::{
    migrator::MigrationState,
    storage_traits::{AssetUpdateIndexStorage, Dumper},
    Storage,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::error;

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

    /// Number of shards to use for the dump
    /// If not set, the dump will be done in a single shard
    /// If set, the dump will be done in the specified number of shards
    #[arg(short, long, default_value = "1")]
    num_shards: u64,

    /// Number of shards for fungible tokens
    /// If not set, the dump will be done in a single shard
    /// If set, the dump will be done in the specified number of shards
    #[arg(short, long, default_value = "1")]
    fungible_num_shards: u64,
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

    let cancellation_token = CancellationToken::new();
    let stop_handle = tokio::task::spawn({
        let cancellation_token = cancellation_token.clone();
        async move {
            #[cfg(not(feature = "profiling"))]
            usecase::graceful_stop::graceful_shutdown(cancellation_token).await;
            #[cfg(feature = "profiling")]
            nft_ingester::init::graceful_stop(cancellation_token, None, None, "").await;
        }
    });
    let rocks_storage = Arc::new(
        Storage::open_secondary(
            &args.source_path,
            &secondary_storage_path,
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap(),
    );

    if let Err(e) = rocks_storage.db.try_catch_up_with_primary() {
        tracing::error!("Sync rocksdb error: {}", e);
    }

    tracing::info!("Preparation took {:?}", start_time.elapsed());
    let start_time = std::time::Instant::now();

    let Some(last_known_key) = rocks_storage.last_known_nft_asset_updated_key()? else {
        return Ok(());
    };
    let Some(last_known_fungible_key) = rocks_storage.last_known_fungible_asset_updated_key()?
    else {
        return Ok(());
    };

    let base_path =
        args.dump_path.unwrap_or_else(|| tempfile::TempDir::new().unwrap().path().to_path_buf());

    let shards = shard_pubkeys(args.num_shards);
    let fungible_shards = shard_pubkeys(args.fungible_num_shards);

    let mut tasks = JoinSet::new();
    let mut fungible_tasks = JoinSet::new();

    for (start, end) in shards.iter() {
        let name_postfix =
            if args.num_shards > 1 { format!("_shard_{}_{}", start, end) } else { "".to_string() };
        let creators_path = base_path
            .join(format!("creators{}.csv", name_postfix))
            .to_str()
            .map(str::to_owned)
            .unwrap();
        let assets_path = base_path
            .join(format!("assets{}.csv", name_postfix))
            .to_str()
            .map(str::to_owned)
            .unwrap();
        let authorities_path = base_path
            .join(format!("assets_authorities{}.csv", name_postfix))
            .to_str()
            .map(str::to_owned)
            .unwrap();
        let metadata_path = base_path
            .join(format!("metadata{}.csv", name_postfix))
            .to_str()
            .map(str::to_owned)
            .unwrap();
        tracing::info!(
            "Dumping to creators: {:?}, assets: {:?}, authorities: {:?}, metadata: {:?}",
            creators_path,
            assets_path,
            authorities_path,
            metadata_path,
        );

        let assets_file = File::create(assets_path.clone())
            .map_err(|e| format!("Could not create file for assets dump: {}", e))?;
        let creators_file = File::create(creators_path.clone())
            .map_err(|e| format!("Could not create file for creators dump: {}", e))?;
        let authority_file = File::create(authorities_path.clone())
            .map_err(|e| format!("Could not create file for authority dump: {}", e))?;
        let metadata_file = File::create(metadata_path.clone())
            .map_err(|e| format!("Could not create file for metadata dump: {}", e))?;

        let start = *start;
        let end = *end;
        let metrics = metrics.clone();
        let rocks_storage = rocks_storage.clone();
        tasks.spawn_blocking({
            let cancellation_token = cancellation_token.child_token();
            move || {
                rocks_storage.dump_nft_csv(
                    assets_file,
                    creators_file,
                    authority_file,
                    metadata_file,
                    args.buffer_capacity,
                    args.limit,
                    Some(start),
                    Some(end),
                    cancellation_token,
                    metrics,
                )
            }
        });
    }

    for (start, end) in fungible_shards.iter() {
        let name_postfix = if args.fungible_num_shards > 1 {
            format!("_shard_{}_{}", start, end)
        } else {
            "".to_string()
        };
        let fungible_tokens_path = base_path
            .join(format!("fungible_tokens{}.csv", name_postfix))
            .to_str()
            .map(str::to_owned)
            .unwrap();
        tracing::info!("Dumping to fungible tokens: {:?}", fungible_tokens_path);
        let fungible_tokens_file = File::create(fungible_tokens_path.clone())
            .map_err(|e| format!("Could not create file for fungible tokens dump: {}", e))?;

        let start = *start;
        let end = *end;
        let metrics = metrics.clone();
        let rocks_storage = rocks_storage.clone();
        fungible_tasks.spawn_blocking({
            let cancellation_token = cancellation_token.child_token();
            move || {
                rocks_storage.dump_fungible_csv(
                    (fungible_tokens_file, fungible_tokens_path),
                    args.buffer_capacity,
                    Some(start),
                    Some(end),
                    cancellation_token,
                    metrics,
                )
            }
        });
    }

    let mut total_assets = 0;
    while let Some(task) = tasks.join_next().await {
        let cnt = task.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
        total_assets += cnt;
    }
    let duration = start_time.elapsed();
    tracing::info!(
        "Dumping of {} assets took {:?}, average rate: {:.2} assets/s",
        total_assets,
        duration,
        total_assets as f64 / duration.as_secs_f64()
    );

    while let Some(task) = fungible_tasks.join_next().await {
        task.map_err(|e| e.to_string())?.map_err(|e| e.to_string())?;
    }
    tracing::info!("Dumping fungible tokens done");
    let keys_file = File::create(base_path.join("keys.csv")).expect("should create keys file");
    Storage::dump_last_keys(keys_file, last_known_key, last_known_fungible_key)?;
    if let Err(_) = stop_handle.await {
        error!("Error joining graceful shutdown!");
    }
    Ok(())
}
