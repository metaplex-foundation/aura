use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
use tracing::{error, info};

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

    /// Number of concurrent workers (default: 16)
    #[arg(short = 'w', long, default_value_t = 16)]
    workers: usize,
}

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Open source RocksDB in readonly mode
    let source_db =
        Storage::open_readonly_with_cfs_only_db(&args.source_db_path, vec![RawBlock::NAME])
            .expect("Failed to open source RocksDB");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());

    // Open target RocksDB
    let target_db = Arc::new(
        Storage::open(
            &args.target_db_path,
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
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
    let start_slot = if let Some(start_slot) = args.start_slot {
        info!("Starting from slot: {}", start_slot);
        iter.seek(RawBlock::encode_key(start_slot));
        start_slot
    } else {
        info!("Starting from the first slot");
        iter.seek_to_first();
        iter.key()
            .map(|k| u64::from_be_bytes(k.try_into().expect("Failed to decode the start slot key")))
            .expect("Failed to get the start slot")
    };

    info!("Start slot: {}, Last slot: {}", start_slot, last_slot);

    // Set up progress bar
    let total_slots = last_slot - start_slot + 1;
    let progress_bar = Arc::new(ProgressBar::new(total_slots));
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

    // Concurrency setup
    let num_workers = args.workers;
    let (slot_sender, slot_receiver) = async_channel::bounded::<(u64, Vec<u8>)>(num_workers * 2);
    let slots_processed = Arc::new(AtomicU64::new(0));
    let last_slot_processed = Arc::new(AtomicU64::new(start_slot));
    let rate = Arc::new(Mutex::new(0.0));
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Spawn a task to handle graceful shutdown on Ctrl+C
    let shutdown_flag_clone = shutdown_flag.clone();
    let slot_sender_clone = slot_sender.clone();
    tokio::spawn(async move {
        // Wait for Ctrl+C signal
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Received Ctrl+C, shutting down gracefully...");
                shutdown_flag_clone.store(true, Ordering::SeqCst);
                // Close the channel to signal workers to stop
                slot_sender_clone.close();
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    // Spawn worker tasks
    let mut worker_handles = Vec::new();
    for _ in 0..num_workers {
        let consumer = consumer.clone();
        let progress_bar = progress_bar.clone();
        let slots_processed = slots_processed.clone();
        let last_slot_processed = last_slot_processed.clone();
        let rate = rate.clone();
        let shutdown_flag = shutdown_flag.clone();

        let slot_receiver = slot_receiver.clone();

        let handle = tokio::spawn(async move {
            let slot_receiver = slot_receiver;
            while let Ok((slot, raw_block_data)) = slot_receiver.recv().await {
                if shutdown_flag.load(Ordering::SeqCst) {
                    break;
                }

                // Process the slot
                let raw_block: RawBlock = match serde_cbor::from_slice(&raw_block_data) {
                    Ok(rb) => rb,
                    Err(e) => {
                        error!("Failed to decode the value for slot {}: {}", slot, e);
                        continue;
                    }
                };

                if let Err(e) = consumer.consume_block(slot, raw_block.block).await {
                    error!("Error processing slot {}: {}", slot, e);
                }

                // Update last_slot_processed
                last_slot_processed.store(slot, Ordering::Relaxed);

                // Increment slots_processed
                let current_slots_processed = slots_processed.fetch_add(1, Ordering::Relaxed) + 1;

                // Update progress bar position and message
                let progress = slot - start_slot + 1;
                progress_bar.set_position(progress);

                let current_rate = {
                    let rate_guard = rate.lock().unwrap();
                    *rate_guard
                };
                progress_bar.set_message(format!(
                    "Slots Processed: {} Current Slot: {} Rate: {:.2}/s",
                    current_slots_processed, slot, current_rate
                ));
            }
        });

        worker_handles.push(handle);
    }

    // Spawn a task to update the rate periodically
    let slots_processed_clone = slots_processed.clone();
    let rate_clone = rate.clone();
    let shutdown_flag_clone = shutdown_flag.clone();

    tokio::spawn(async move {
        let mut last_time = std::time::Instant::now();
        let mut last_count = slots_processed_clone.load(Ordering::Relaxed);

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            if shutdown_flag_clone.load(Ordering::SeqCst) {
                break;
            }

            let current_time = std::time::Instant::now();
            let current_count = slots_processed_clone.load(Ordering::Relaxed);

            let elapsed = current_time.duration_since(last_time).as_secs_f64();
            let count = current_count - last_count;

            let current_rate = if elapsed > 0.0 {
                (count as f64) / elapsed
            } else {
                0.0
            };

            // Update rate
            {
                let mut rate_guard = rate_clone.lock().unwrap();
                *rate_guard = current_rate;
            }

            // Update for next iteration
            last_time = current_time;
            last_count = current_count;
        }
    });

    // Send slots to the channel
    while iter.valid() {
        if shutdown_flag.load(Ordering::SeqCst) {
            info!("Shutdown signal received. Stopping the submission of new slots.");
            break;
        }

        if let Some((key, value)) = iter.item() {
            let slot = u64::from_be_bytes(key.try_into().expect("Failed to decode the slot key"));
            let raw_block_data = value.to_vec();

            // Send the slot and data to the channel
            if slot_sender.send((slot, raw_block_data)).await.is_err() {
                error!("Failed to send slot {} to workers", slot);
                break;
            }

            // Move to the next slot
            iter.next();
        } else {
            break;
        }
    }

    // Close the sender to signal that no more items will be sent
    slot_sender.close();

    // Wait for workers to finish
    for handle in worker_handles {
        let _ = handle.await;
    }

    progress_bar.finish_with_message("Processing complete");
}