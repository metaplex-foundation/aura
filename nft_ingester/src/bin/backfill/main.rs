use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use clap::Parser;
use entities::models::RawBlock;
use indicatif::{ProgressBar, ProgressStyle};
use interface::signature_persistence::BlockConsumer;
use metrics_utils::{
    red::RequestErrorDurationMetrics, BackfillerMetricsConfig, IngesterMetricsConfig,
};
use nft_ingester::{
    backfiller::DirectBlockParser,
    processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
    transaction_ingester,
};
use rocks_db::{column::TypedColumn, migrator::MigrationState, SlotStorage, Storage};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the source RocksDB with slots (readonly)
    #[arg(short, long, env = "SOURCE_SLOTS_DB_PATH")]
    source_db_path: PathBuf,

    /// Path to the target RocksDB instance
    #[arg(short, long, env = "TARGET_MAIN_DB_PATH")]
    target_db_path: PathBuf,

    /// Optional first slot number to start processing from
    #[arg(long)]
    first_slot: Option<u64>,

    /// Optional last slot number to process until
    #[arg(long)]
    last_slot: Option<u64>,

    /// Number of concurrent workers (default: 16)
    #[arg(short = 'w', long, default_value_t = 16)]
    workers: usize,

    /// Optional comma-separated list of slot numbers to process
    #[arg(long)]
    slots: Option<String>,
}

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Open source RocksDB in readonly mode
    let source_db =
        Storage::open_readonly_with_cfs_only_db(&args.source_db_path, SlotStorage::cf_names())
            .expect("Failed to open source RocksDB");
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let cancellation_token = CancellationToken::new();
    let stop_handle = tokio::task::spawn({
        let cancellation_token = cancellation_token.clone();
        async move {
            usecase::graceful_stop::graceful_shutdown(cancellation_token).await;
        }
    });

    // Open target RocksDB
    let target_db = Arc::new(
        Storage::open(&args.target_db_path, red_metrics.clone(), MigrationState::Last)
            .expect("Failed to open target RocksDB"),
    );

    // Initialize the DirectBlockParser
    let ingester_metrics = Arc::new(IngesterMetricsConfig::new());
    let metrics = Arc::new(BackfillerMetricsConfig::new());
    let bubblegum_updates_processor =
        Arc::new(BubblegumTxProcessor::new(target_db.clone(), ingester_metrics.clone()));

    let tx_ingester = Arc::new(transaction_ingester::BackfillTransactionIngester::new(
        bubblegum_updates_processor.clone(),
    ));

    let consumer =
        Arc::new(DirectBlockParser::new(tx_ingester.clone(), target_db.clone(), metrics.clone()));

    // Concurrency setup
    let num_workers = args.workers;
    let (slot_sender, slot_receiver) = async_channel::bounded::<(u64, Vec<u8>)>(num_workers * 2);
    let slots_processed = Arc::new(AtomicU64::new(0));
    let rate = Arc::new(Mutex::new(0.0));

    usecase::executor::spawn({
        let cancellation_token = cancellation_token.clone();
        let slot_sender = slot_sender.clone();
        async move {
            // Wait for Ctrl+C signal
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received Ctrl+C, shutting down gracefully...");
                    cancellation_token.cancel();
                    // Close the channel to signal workers to stop
                    slot_sender.close();
                },
                Err(err) => {
                    error!("Unable to listen for shutdown signal: {}", err);
                },
            }
        }
    });

    // Parse slots if provided
    let mut slots_to_process = Vec::new();
    if let Some(slots_str) = args.slots {
        info!("Processing specific slots provided via command line.");
        for part in slots_str.split(',') {
            let slot_str = part.trim();
            if let Ok(slot) = slot_str.parse::<u64>() {
                slots_to_process.push(slot);
            } else {
                warn!("Invalid slot number provided: {}", slot_str);
            }
        }

        // Remove duplicates and sort slots
        slots_to_process.sort_unstable();
        slots_to_process.dedup();

        if slots_to_process.is_empty() {
            error!("No valid slots to process. Exiting.");
            return;
        }

        info!("Total slots to process: {}", slots_to_process.len());
    }

    // Set up progress bar
    let total_slots = if !slots_to_process.is_empty() {
        slots_to_process.len() as u64
    } else {
        // Get the last slot
        let mut iter = source_db.raw_iterator_cf(&source_db.cf_handle(RawBlock::NAME).unwrap());
        iter.seek_to_last();
        if !iter.valid() {
            error!("Failed to seek to last slot");
            return;
        }
        let db_last_slot = iter
            .key()
            .map(|k| u64::from_be_bytes(k.try_into().expect("Failed to decode the last slot key")))
            .expect("Failed to get the last slot");

        // Determine the starting slot
        let first_slot = if let Some(first_slot) = args.first_slot {
            info!("Starting from slot: {}", first_slot);
            first_slot
        } else {
            iter.seek_to_first();
            iter.key()
                .map(|k| {
                    u64::from_be_bytes(k.try_into().expect("Failed to decode the first slot key"))
                })
                .expect("Failed to get the first slot")
        };

        // Determine the last slot to process
        let last_slot = args.last_slot.unwrap_or(db_last_slot);
        info!("First slot: {}, Last slot: {}", first_slot, last_slot);

        if last_slot < first_slot {
            error!("Last slot {} is less than first slot {}", last_slot, first_slot);
            return;
        }

        last_slot - first_slot + 1
    };

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

    // Spawn worker tasks
    let mut worker_handles = Vec::new();
    for _ in 0..num_workers {
        let consumer = consumer.clone();
        let progress_bar = progress_bar.clone();
        let slots_processed = slots_processed.clone();
        let rate = rate.clone();

        let slot_receiver = slot_receiver.clone();

        let handle = tokio::task::spawn({
            let cancellation_token = cancellation_token.child_token();
            async move {
                while let Ok((slot, raw_block_data)) = slot_receiver.recv().await {
                    if cancellation_token.is_cancelled() {
                        break;
                    }

                    // Process the slot
                    let raw_block: RawBlock = match RawBlock::decode(&raw_block_data) {
                        Ok(rb) => rb,
                        Err(e) => {
                            error!("Failed to decode the value for slot {}: {}", slot, e);
                            continue;
                        },
                    };

                    if let Err(e) = consumer.consume_block(slot, raw_block.block).await {
                        error!("Error processing slot {}: {}", slot, e);
                    }

                    // Increment slots_processed
                    let current_slots_processed =
                        slots_processed.fetch_add(1, Ordering::Relaxed) + 1;

                    // Update progress bar position and message
                    progress_bar.inc(1);

                    let current_rate = {
                        let rate_guard = rate.lock().unwrap();
                        *rate_guard
                    };
                    progress_bar.set_message(format!(
                        "Slots Processed: {} Current Slot: {} Rate: {:.2}/s",
                        current_slots_processed, slot, current_rate
                    ));
                }
            }
        });

        worker_handles.push(handle);
    }

    usecase::executor::spawn({
        let cancellation_token = cancellation_token.clone();
        let slots_processed = slots_processed.clone();
        let rate = rate.clone();
        async move {
            let mut last_time = std::time::Instant::now();
            let mut last_count = slots_processed.load(Ordering::Relaxed);

            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                if cancellation_token.is_cancelled() {
                    break;
                }

                let current_time = std::time::Instant::now();
                let current_count = slots_processed.load(Ordering::Relaxed);

                let elapsed = current_time.duration_since(last_time).as_secs_f64();
                let count = current_count - last_count;

                let current_rate = if elapsed > 0.0 { (count as f64) / elapsed } else { 0.0 };

                // Update rate
                {
                    let mut rate_guard = rate.lock().unwrap();
                    *rate_guard = current_rate;
                }

                // Update for next iteration
                last_time = current_time;
                last_count = current_count;
            }
        }
    });

    // Send slots to the channel
    if !slots_to_process.is_empty() {
        // Process only the specified slots
        send_slots_to_workers(
            slots_to_process,
            source_db,
            slot_sender.clone(),
            cancellation_token.child_token(),
        )
        .await;
    } else {
        // Process all slots from first_slot to last_slot
        send_all_slots_to_workers(
            source_db,
            slot_sender.clone(),
            cancellation_token.child_token(),
            args.first_slot,
            args.last_slot,
        )
        .await;
    }

    // Close the sender to signal that no more items will be sent
    slot_sender.close();

    // Wait for workers to finish
    for handle in worker_handles {
        let _ = handle.await;
    }

    if let Err(_) = stop_handle.await {
        error!("Error joining graceful shutdown!");
    }
    progress_bar.finish_with_message("Processing complete");
}

// Function to send specified slots to workers
async fn send_slots_to_workers(
    slots_to_process: Vec<u64>,
    source_db: rocksdb::DB,
    slot_sender: async_channel::Sender<(u64, Vec<u8>)>,
    cancellation_token: CancellationToken,
) {
    let cf_handle = source_db.cf_handle(RawBlock::NAME).unwrap();

    for slot in slots_to_process {
        if cancellation_token.is_cancelled() {
            info!("Shutdown signal received. Stopping the submission of new slots.");
            break;
        }

        let key = RawBlock::encode_key(slot);
        match source_db.get_pinned_cf(&cf_handle, key) {
            Ok(Some(value)) => {
                let raw_block_data = value.to_vec();
                if slot_sender.send((slot, raw_block_data)).await.is_err() {
                    error!("Failed to send slot {} to workers", slot);
                    break;
                }
            },
            Ok(None) => {
                warn!("Slot {} not found in source database", slot);
            },
            Err(e) => {
                error!("Error fetching slot {}: {}", slot, e);
            },
        }
    }
}

// Function to send all slots backwards up to the first_slot to workers
async fn send_all_slots_to_workers(
    source_db: rocksdb::DB,
    slot_sender: async_channel::Sender<(u64, Vec<u8>)>,
    cancellation_token: CancellationToken,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
) {
    let cf_handle = source_db.cf_handle(RawBlock::NAME).unwrap();
    let mut iter = source_db.raw_iterator_cf(&cf_handle);
    let final_key = first_slot.map(RawBlock::encode_key);

    // Start from the last slot or the provided last_slot
    if let Some(last_slot) = last_slot {
        iter.seek(RawBlock::encode_key(last_slot));
        if !iter.valid() {
            iter.seek_to_last();
        }
    } else {
        iter.seek_to_last();
    }

    // Send slots to the channel
    while iter.valid() {
        if cancellation_token.is_cancelled() {
            info!("Shutdown signal received. Stopping the submission of new slots.");
            break;
        }

        if let Some((key, value)) = iter.item() {
            if final_key.is_some() && key < final_key.as_ref().unwrap().as_slice() {
                break;
            }
            let slot = u64::from_be_bytes(key.try_into().expect("Failed to decode the slot key"));
            let raw_block_data = value.to_vec();

            // Send the slot and data to the channel
            if slot_sender.send((slot, raw_block_data)).await.is_err() {
                error!("Failed to send slot {} to workers", slot);
                break;
            }

            // Move to the previous slot
            iter.prev();
        } else {
            break;
        }
    }
}
