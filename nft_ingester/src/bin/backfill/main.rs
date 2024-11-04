use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use entities::models::RawBlock;
use indicatif::{ProgressBar, ProgressStyle};
use interface::signature_persistence::BlockConsumer;
use metrics_utils::BackfillerMetricsConfig;
use metrics_utils::{red::RequestErrorDurationMetrics, IngesterMetricsConfig};
use nft_ingester::{
    backfiller::DirectBlockParser,
    processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
    transaction_ingester,
};
use rocks_db::migrator::MigrationState;
use rocks_db::{column::TypedColumn, Storage};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the source RocksDB with slots (readonly)
    #[arg(short, long)]
    source_db_path: PathBuf,

    /// Path to the target RocksDB instance
    #[arg(short, long)]
    target_db_path: PathBuf,

    /// Optional starting slot number
    #[arg(short, long)]
    start_slot: Option<u64>,
}

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Open source RocksDB in readonly mode
    let source_db =
        Storage::open_readonly_with_cfs_only_db(&args.source_db_path, [RawBlock::NAME].to_vec())
            .expect("Failed to open source RocksDB");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());

    // Open target RocksDB
    let target_db = Arc::new(
        Storage::open(
            &args.target_db_path,
            Arc::new(Mutex::new(JoinSet::new())),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .expect("Failed to open target RocksDB"),
    );

    // Get the last slot
    let mut iter = source_db.raw_iterator_cf(&source_db.cf_handle(RawBlock::NAME).unwrap());
    iter.seek_to_last();
    if !iter.valid() {
        error!("Failed to seek to last slot");
        return;
    }
    let last_slot = iter
        .key()
        .map(|k| u64::from_be_bytes(k.try_into().expect("Failed to decode the last slot key")))
        .expect("Failed to get the last slot");

    // Determine the starting slot
    if let Some(start_slot) = args.start_slot {
        info!("Starting from slot: {}", start_slot);
        iter.seek(RawBlock::encode_key(start_slot));
    } else {
        info!("Starting from the first slot");
        iter.seek_to_first();
    }
    let start_slot = iter
        .key()
        .map(|k| u64::from_be_bytes(k.try_into().expect("Failed to decode the start slot key")))
        .expect("Failed to get the start slot");

    info!("Start slot: {}, Last slot: {}", start_slot, last_slot);

    // Set up progress bar
    let total_slots = last_slot - start_slot + 1;
    let progress_bar = ProgressBar::new(total_slots);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent}% \
                 ({pos}/{len}) {msg}",
            )
            .expect("Failed to set progress bar style")
            .progress_chars("#>-"),
    );

    // Initialize the DirectBlockParser
    let ingester_metrics = Arc::new(IngesterMetricsConfig::new());
    let metrics = Arc::new(BackfillerMetricsConfig::new());
    let bubblegum_updates_processor = Arc::new(BubblegumTxProcessor::new(
        target_db.clone(),
        ingester_metrics.clone(),
    ));

    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        bubblegum_updates_processor.clone(),
    ));

    let consumer = Arc::new(DirectBlockParser::new(
        tx_ingester.clone(),
        target_db.clone(),
        metrics.clone(),
    ));
    // Counter for slots processed
    // let slots_processed = Arc::new(Mutex::new(0u64));
    let mut slots_processed = 0;
    let start_ts = std::time::Instant::now();
    while iter.valid() {
        if let Some((key, value)) = iter.item() {
            let slot = u64::from_be_bytes(key.try_into().expect("Failed to decode the slot key"));
            let raw_block: RawBlock = serde_cbor::from_slice(&value)
                .expect(format!("Failed to decode the value for slot {}", slot).as_str());

            if let Err(e) = consumer.consume_block(slot, raw_block.block).await {
                error!("Error processing slot {}: {}", slot, e);
            } else {
                // // Increment slots processed
                // let mut sp = slots_processed.lock();
                // *sp += 1;
                slots_processed += 1;
            }

            // Update progress bar position and messages
            let progress = slot - start_slot + 1;
            progress_bar.set_position(progress);

            progress_bar.set_message(format!(
                "Slots Processed: {} Current Slot: {}",
                slots_processed, slot
            ));
            // Move to the next slot
            iter.next();
        }
    }
    progress_bar
        .with_elapsed(start_ts.elapsed())
        .finish_with_message("Processing complete");
}
