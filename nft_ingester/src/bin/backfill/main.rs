use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
use rocks_db::SlotStorage;
use rocks_db::{column::TypedColumn, Storage};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the source RocksDB with slots (readonly)
    #[arg(short, long, env = "SLOTS_DB_PRIMARY_PATH")]
    source_db_path: PathBuf,

    /// Path to the target RocksDB instance
    #[arg(short, long, env = "DB_PRIMARY_PATH")]
    target_db_path: PathBuf,

    /// Optional starting slot number
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Number of concurrent workers (default: 16)
    #[arg(short = 'w', long, default_value_t = 16)]
    workers: usize,

    /// Optional comma-separated list of slot numbers to process
    #[arg(short = 's', long)]
    slots: Option<String>,
}

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Open source RocksDB in readonly mode
    let source_db = Arc::new(
        Storage::open_readonly_with_cfs_only_db(&args.source_db_path, SlotStorage::cf_names())
            .expect("Failed to open source RocksDB"),
    );
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

    // Spawn a task to handle graceful shutdown on Ctrl+C
    let shutdown_token = CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();

    tokio::spawn(async move {
        // Wait for Ctrl+C signal
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Received Ctrl+C, shutting down gracefully...");
                shutdown_token_clone.cancel();
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
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
        let mut slots_set = HashSet::new();
        slots_to_process = slots_to_process
            .into_iter()
            .filter(|x| slots_set.insert(*x))
            .collect();
        slots_to_process.sort_unstable();

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
        // Get the number of slots
        let cnt = source_db
            .property_int_value_cf(
                &source_db.cf_handle(RawBlock::NAME).unwrap(),
                "rocksdb.estimate-num-keys",
            )
            .expect("Failed to get the number of keys");
        cnt.unwrap_or_default()
    };

    let progress_bar = Arc::new(ProgressBar::new(total_slots));
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent}% \
                 ({pos}/{len}) [{per_sec_precise}] [{eta_precise}] {msg}",
            )
            .expect("Failed to set progress bar style")
            .progress_chars("#>-"),
    );
    progress_bar.enable_steady_tick(Duration::from_millis(100)); // Update every 100ms

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
        start_slot
    } else {
        iter.seek_to_first();
        iter.key()
            .map(|k| u64::from_be_bytes(k.try_into().expect("Failed to decode the start slot key")))
            .expect("Failed to get the start slot")
    };

    // Spawn worker tasks
    let mut worker_handles = JoinSet::new();
    let chunk_size = (last_slot - start_slot) / num_workers as u64;
    for i in 0..num_workers {
        let start_slot = start_slot + (i as u64) * chunk_size;
        let end_slot = if i == num_workers - 1 {
            last_slot
        } else {
            start_slot + chunk_size
        };

        let consumer = consumer.clone();
        let progress_bar = progress_bar.clone();
        let shutdown_token = shutdown_token.clone();
        let source_db = source_db.clone();
        let mut processed_slots = 0;
        worker_handles.spawn(async move {
            let mut iter = source_db.raw_iterator_cf(&source_db.cf_handle(RawBlock::NAME).unwrap());
            iter.seek(RawBlock::encode_key(start_slot));
            while iter.valid() {
                if shutdown_token.is_cancelled() {
                    info!(
                        "Worker for slots {}-{} is cancelled after processing {} slots",
                        start_slot, end_slot, processed_slots
                    );
                    return;
                }
                let slot = u64::from_be_bytes(iter.key().unwrap().try_into().unwrap());
                if slot > end_slot {
                    break;
                }
                // Process the slot
                let raw_block_data = iter.value().unwrap();
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

                // Update progress bar position
                progress_bar.inc(1);
                processed_slots += 1;
                iter.next();
            }
            info!(
                "Worker for slots {}-{} is done after processing {} slots",
                start_slot, end_slot, processed_slots
            );
        });
    }
    while let Some(task) = worker_handles.join_next().await {
        task.expect("Failed to join worker task");
    }

    // TODO: Send slots to the channel
    // if !slots_to_process.is_empty() {
    //     // Process only the specified slots
    //     send_slots_to_workers(
    //         slots_to_process,
    //         source_db,
    //         slot_sender.clone(),
    //         shutdown_token.clone(),
    //     )
    //     .await;
    // } else {
    //     // Process all slots from start_slot
    //     send_all_slots_to_workers(
    //         source_db,
    //         slot_sender.clone(),
    //         shutdown_token.clone(),
    //         args.start_slot,
    //     )
    //     .await;
    // }

    progress_bar.finish_with_message("Processing complete");
}

// Function to send specified slots to workers
async fn send_slots_to_workers(
    slots_to_process: Vec<u64>,
    source_db: rocksdb::DB,
    slot_sender: async_channel::Sender<(u64, Vec<u8>)>,
    shutdown_token: CancellationToken,
) {
    let cf_handle = source_db.cf_handle(RawBlock::NAME).unwrap();

    for slot in slots_to_process {
        if shutdown_token.is_cancelled() {
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
            }
            Ok(None) => {
                warn!("Slot {} not found in source database", slot);
            }
            Err(e) => {
                error!("Error fetching slot {}: {}", slot, e);
            }
        }
    }
}

// Function to send all slots starting from start_slot to workers
async fn send_all_slots_to_workers(
    source_db: rocksdb::DB,
    slot_sender: async_channel::Sender<(u64, Vec<u8>)>,
    shutdown_token: CancellationToken,
    start_slot: Option<u64>,
) {
    let cf_handle = source_db.cf_handle(RawBlock::NAME).unwrap();
    let mut iter = source_db.raw_iterator_cf(&cf_handle);

    // Determine starting point
    if let Some(start_slot) = start_slot {
        iter.seek(RawBlock::encode_key(start_slot));
    } else {
        iter.seek_to_first();
    }

    // Send slots to the channel
    while iter.valid() {
        if shutdown_token.is_cancelled() {
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
}
