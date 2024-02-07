use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clap::Parser;
use log::info;
use nft_ingester::{backfiller, transaction_ingester};
use solana_program::pubkey::Pubkey;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinSet;

use metrics_utils::utils::start_metrics;
use metrics_utils::{
    ApiMetricsConfig, BackfillerMetricsConfig, IngesterMetricsConfig, JsonDownloaderMetricsConfig,
    JsonMigratorMetricsConfig, MetricState, MetricsTrait, RpcBackfillerMetricsConfig,
    SequenceConsistentGapfillMetricsConfig, SynchronizerMetricsConfig,
};
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::buffer::Buffer;
use nft_ingester::config::{
    setup_config, BackfillerConfig, TreeBackfillerConfig, INGESTER_CONFIG_PREFIX,
    TREE_BACKFILLER_CONFIG_PREFIX,
};
use nft_ingester::init::graceful_stop;
use nft_ingester::{config::init_logger, error::IngesterError};
use rocks_db::Storage;

use nft_ingester::backfiller::{connect_new_bigtable_from_config, DirectBlockParser};

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    info!("Starting Ingester");

    let config: TreeBackfillerConfig = setup_config(TREE_BACKFILLER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());

    let mut metrics_state = MetricState::new(
        IngesterMetricsConfig::new(),
        ApiMetricsConfig::new(),
        JsonDownloaderMetricsConfig::new(),
        BackfillerMetricsConfig::new(),
        RpcBackfillerMetricsConfig::new(),
        SynchronizerMetricsConfig::new(),
        JsonMigratorMetricsConfig::new(),
        SequenceConsistentGapfillMetricsConfig::new(),
    );
    metrics_state.register_metrics();

    let keep_running = Arc::new(AtomicBool::new(true));

    let tasks = JoinSet::new();

    // setup buffer
    let buffer = Arc::new(Buffer::new());

    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    // start parsers
    let storage = Storage::open(&config.target_rocks.clone(), mutexed_tasks.clone()).unwrap();

    let target_rocks = Arc::new(storage);

    let backfill_bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        target_rocks.clone(),
        metrics_state.ingester_metrics.clone(),
        buffer.json_tasks.clone(),
    ));
    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        backfill_bubblegum_updates_processor.clone(),
    ));
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let backfiller_config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);

    let source_rocks = Storage::open_as_secondary(
        &config.source_rocks.clone(),
        "./secondary",
        mutexed_tasks.clone(),
    )
    .unwrap();
    let producer = Arc::new(source_rocks);

    let lookup_key = Pubkey::from_str(&backfiller_config.lookup_key).unwrap();

    let consumer = Arc::new(DirectBlockParser::new(
        tx_ingester.clone(),
        target_rocks.clone(),
        metrics_state.backfiller_metrics.clone(),
        Some(lookup_key),
    ));

    let big_table_client = Arc::new(
        connect_new_bigtable_from_config(backfiller_config.clone())
            .await
            .unwrap(),
    );

    let backfiller = backfiller::Backfiller::new(
        target_rocks.clone(),
        big_table_client.clone(),
        backfiller_config.clone(),
    );

    backfiller
        .start_backfill(
            mutexed_tasks.clone(),
            shutdown_rx.resubscribe(),
            metrics_state.backfiller_metrics.clone(),
            consumer,
            producer.clone(),
        )
        .await
        .unwrap();

    start_metrics(metrics_state.registry, Some(config.metrics_port)).await;

    // --stop
    graceful_stop(mutexed_tasks, keep_running.clone(), shutdown_tx, None, None).await;

    Ok(())
}
