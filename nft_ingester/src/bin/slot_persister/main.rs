use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use backfill_rpc::rpc::BackfillRPC;
use clap::Parser;
use entities::models::RawBlock;
use futures::future::join_all;
use interface::{signature_persistence::BlockProducer, slot_getter::FinalizedSlotGetter};
use metrics_utils::{utils::start_metrics, MetricState, MetricsTrait};
use nft_ingester::{backfiller::BackfillSource, inmemory_slots_dumper::InMemorySlotsDumper};
use rocks_db::{column::TypedColumn, SlotStorage};
use tokio::sync::{broadcast, Semaphore};
use tokio_retry::{strategy::ExponentialBackoff, RetryIf};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use usecase::{bigtable::BigTableClient, slots_collector::SlotsCollector};

const MAX_RETRIES: usize = 5;
const INITIAL_DELAY_MS: u64 = 100;

const MAX_BATCH_RETRIES: usize = 5;
const INITIAL_BATCH_DELAY_MS: u64 = 500;
// Offset to start collecting slots from, approximately 2 minutes before the finalized slot, given the eventual consistency of the big table
const SLOT_COLLECTION_OFFSET: u64 = 300;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    long_about = "Slot persister reads the slot data from the BigTable and persists it to the RocksDB."
)]
struct Args {
    /// Path to the target RocksDB instance with slots
    #[arg(short, long, env = "SLOTS_DB_PRIMARY_PATH")]
    target_db_path: PathBuf,

    /// RPC host
    #[arg(short, long, env = "RPC_HOST")]
    rpc_host: String,

    /// Optional starting slot number, this will override the last saved slot in the RocksDB
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Big table credentials file path
    #[arg(short, long, env = "BIG_TABLE_CREDENTIALS")]
    big_table_credentials: Option<String>,

    /// Optional big table timeout (default: 1000)
    #[arg(short = 'B', long, default_value_t = 1000)]
    big_table_timeout: u32,

    /// Metrics port
    /// Default: 9090
    #[arg(short, long, default_value = "9090", env = "METRICS_PORT")]
    metrics_port: u16,

    /// Number of slots to process in each batch
    #[arg(short, long, default_value_t = 200, env = "SLOT_PERSISTER_CHUNK_SIZE")]
    chunk_size: usize,

    /// Maximum number of concurrent requests
    #[arg(short = 'M', long, default_value_t = 20)]
    max_concurrency: usize,

    /// Optional comma-separated list of slot numbers to check
    #[arg(long)]
    slots: Option<String>,
}

pub fn get_last_persisted_slot(rocks_db: Arc<SlotStorage>) -> u64 {
    let mut it = rocks_db.db.raw_iterator_cf(&rocks_db.db.cf_handle(RawBlock::NAME).unwrap());
    it.seek_to_last();
    if !it.valid() {
        return 0;
    }
    it.key().map(|b| RawBlock::decode_key(b.to_vec()).unwrap_or_default()).unwrap_or_default()
}

#[derive(Debug)]
enum FetchError {
    Cancelled,
    // NOTE: the compiler incorrectly highlights the String field as being never read
    // while it is clearly logged in other places in the code.
    #[allow(dead_code)]
    Other(String),
}

async fn fetch_block_with_retries(
    block_getter: Arc<BackfillSource>,
    slot: u64,
    shutdown_token: CancellationToken,
) -> Result<(u64, RawBlock), (u64, FetchError)> {
    let retry_strategy = ExponentialBackoff::from_millis(INITIAL_DELAY_MS)
        .factor(2)
        .max_delay(Duration::from_secs(10))
        .take(MAX_RETRIES);

    RetryIf::spawn(
        retry_strategy,
        || {
            let block_getter = block_getter.clone();
            let shutdown_token = shutdown_token.clone();
            async move {
                if shutdown_token.is_cancelled() {
                    info!("Fetch cancelled for slot {} due to shutdown signal.", slot);
                    Err((slot, FetchError::Cancelled))
                } else {
                    debug!("Fetching slot {}", slot);
                    match block_getter.get_block(slot, None::<Arc<BigTableClient>>).await {
                        Ok(block_data) => {
                            debug!("Successfully fetched block for slot {}", slot);
                            Ok((slot, RawBlock { slot, block: block_data }))
                        },
                        Err(e) => {
                            error!("Error fetching block for slot {}: {}", slot, e);
                            Err((slot, FetchError::Other(e.to_string())))
                        },
                    }
                }
            }
        },
        |e: &(u64, FetchError)| {
            let (_, ref err) = *e;
            match err {
                FetchError::Cancelled => false, // Do not retry if cancelled
                _ => true,                      // Retry on other errors
            }
        },
    )
    .await
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();
    info!("Starting Slot persister...");

    let args = Args::parse();

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();

    start_metrics(metrics_state.registry, Some(args.metrics_port)).await;
    // Open target RocksDB
    let target_db = Arc::new(
        SlotStorage::open(
            &args.target_db_path,
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            metrics_state.red_metrics.clone(),
        )
        .expect("Failed to open target RocksDB"),
    );

    let last_persisted_slot = get_last_persisted_slot(target_db.clone());
    let start_slot = if let Some(start_slot) = args.start_slot {
        info!(
            "Starting from slot: {}, while last persisted slot: {}",
            start_slot, last_persisted_slot
        );
        start_slot
    } else {
        info!("Starting from last persisted slot: {}", last_persisted_slot);
        last_persisted_slot
    };

    let shutdown_token = CancellationToken::new();
    let shutdown_token_clone = shutdown_token.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Spawn a task to handle graceful shutdown on Ctrl+C
    tokio::spawn(async move {
        // Wait for Ctrl+C signal
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Received Ctrl+C, shutting down gracefully...");
                shutdown_token_clone.cancel();
                shutdown_tx.send(()).unwrap();
            },
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            },
        }
    });

    let rpc_client = Arc::new(BackfillRPC::connect(args.rpc_host.clone()));

    let backfill_source = {
        if let Some(ref bg_creds) = args.big_table_credentials {
            Arc::new(BackfillSource::Bigtable(Arc::new(
                BigTableClient::connect_new_with(bg_creds.clone(), args.big_table_timeout)
                    .await
                    .expect("expected to connect to big table"),
            )))
        } else {
            Arc::new(BackfillSource::Rpc(rpc_client.clone()))
        }
    };

    let in_mem_dumper = Arc::new(InMemorySlotsDumper::new());
    let slots_collector = SlotsCollector::new(
        in_mem_dumper.clone(),
        backfill_source.clone(),
        metrics_state.backfiller_metrics.clone(),
    );
    let wait_period = Duration::from_secs(1);
    // Check if slots are provided via --slots argument
    let mut provided_slots = Vec::new();
    if let Some(ref slots_str) = args.slots {
        // Parse comma-separated list of slots
        info!("Processing specific slots provided via command line.");
        for part in slots_str.split(',') {
            let slot_str = part.trim();
            if let Ok(slot) = slot_str.parse::<u64>() {
                provided_slots.push(slot);
            } else {
                warn!("Invalid slot number provided: {}", slot_str);
            }
        }

        // Remove duplicates and sort slots
        provided_slots.sort_unstable();
        provided_slots.dedup();

        if provided_slots.is_empty() {
            error!("No valid slots to process. Exiting.");
            return Ok(());
        }

        info!("Total slots to process: {}", provided_slots.len());

        // Proceed to process the provided slots
        process_slots(provided_slots, backfill_source, target_db, &args, shutdown_token.clone())
            .await;
        return Ok(()); // Exit after processing provided slots
    }
    let mut start_slot = start_slot;
    loop {
        if shutdown_token.is_cancelled() {
            info!("Shutdown signal received, exiting main loop...");
            break;
        }

        match rpc_client.get_finalized_slot().await {
            Ok(finalized_slot) => {
                let last_slot_to_check = finalized_slot.saturating_sub(SLOT_COLLECTION_OFFSET);
                info!(
                    "Finalized slot from RPC: {}, offsetting slot collection to: {}",
                    finalized_slot, last_slot_to_check
                );
                let top_collected_slot = slots_collector
                    .collect_slots(
                        &blockbuster::programs::bubblegum::ID,
                        last_slot_to_check,
                        start_slot,
                        &shutdown_rx,
                    )
                    .await;
                if let Some(slot) = top_collected_slot {
                    start_slot = slot;
                }
                let slots = in_mem_dumper.get_sorted_keys().await;
                in_mem_dumper.clear().await;
                info!(
                    "Collected {} slots to persist between {} and {}",
                    slots.len(),
                    start_slot,
                    last_slot_to_check
                );
                // slots has all the slots numbers we need to downlaod and persist. Slots should be downloaded concurrently, but no slot shouold be persisted if the previous slot is not persisted.
                if slots.is_empty() {
                    info!("No new slots to process. Sleeping for {:?}", wait_period);
                    let sleep = tokio::time::sleep(wait_period);

                    tokio::select! {
                        _ = sleep => {},
                        _ = shutdown_token.cancelled() => {
                            info!("Received shutdown signal, stopping loop...");
                            break;
                        },
                    };
                    continue;
                }
                // Process the collected slots
                process_slots(
                    slots,
                    backfill_source.clone(),
                    target_db.clone(),
                    &args,
                    shutdown_token.clone(),
                )
                .await;
            },
            Err(e) => {
                error!("Error getting finalized slot: {}", e);
            },
        }

        let sleep = tokio::time::sleep(wait_period);
        tokio::select! {
            _ = sleep => {},
            _ = shutdown_token.cancelled() => {
                info!("Received shutdown signal, stopping loop...");
                break;
            },
        };
    }
    info!("Slot persister has stopped.");
    Ok(())
}

async fn process_slots(
    slots: Vec<u64>,
    backfill_source: Arc<BackfillSource>,
    target_db: Arc<SlotStorage>,
    args: &Args,
    shutdown_token: CancellationToken,
) {
    // Process slots in "outer" batches of up to `args.chunk_size`.
    for batch in slots.chunks(args.chunk_size) {
        if shutdown_token.is_cancelled() {
            info!("Shutdown signal received during batch processing, exiting...");
            break;
        }

        let mut batch_retries = 0;
        let mut batch_delay_ms = INITIAL_BATCH_DELAY_MS;

        // The set of slots we still need to fetch in this "outer" batch.
        let mut slots_to_fetch: Vec<u64> = batch.to_vec();
        let mut successful_blocks: HashMap<u64, RawBlock> = HashMap::new();

        // Retry loop for the entire "outer" batch, including partial failures.
        loop {
            if shutdown_token.is_cancelled() {
                info!("Shutdown signal received during batch processing, exiting...");
                break;
            }

            // We will fill `new_failed_slots` if any slot(s) or the whole chunk fails
            let mut new_failed_slots = Vec::new();

            match &*backfill_source {
                // ------------------------------------------------------------------
                // 1) Bigtable path: split the batch into sub-chunks, fetch in parallel
                // ------------------------------------------------------------------
                BackfillSource::Bigtable(bigtable_client) => {
                    // We want up to `args.max_concurrency` parallel requests.
                    // We'll split `slots_to_fetch` into that many sub-chunks.
                    // Example: if slots_to_fetch.len()=300 & max_concurrency=10,
                    // each sub-chunk is ~30 slots.
                    let total = slots_to_fetch.len();
                    let sub_chunk_size = std::cmp::max(total / args.max_concurrency, 1);

                    // Force at least a single sub-chunk
                    let sub_chunks: Vec<&[u64]> = slots_to_fetch.chunks(sub_chunk_size).collect();

                    // A semaphore ensures we won't exceed `args.max_concurrency`
                    // *sub-chunk fetches* in parallel. (Though usually sub_chunks.len()
                    // is <= max_concurrency anyway.)
                    let semaphore = Arc::new(Semaphore::new(args.max_concurrency));

                    let fetch_futures = sub_chunks.into_iter().map(|sub_slots| {
                        let bigtable_client = bigtable_client.clone();
                        let semaphore = semaphore.clone();
                        let shutdown_token = shutdown_token.clone();

                        async move {
                            let _permit = semaphore.acquire().await;
                            if shutdown_token.is_cancelled() {
                                return Err((sub_slots.to_vec(), "Shutdown".to_string()));
                            }

                            // Single Bigtable call for this sub-chunk
                            match bigtable_client.get_blocks(sub_slots).await {
                                Ok(blocks_map) => Ok(blocks_map),
                                Err(e) => Err((sub_slots.to_vec(), e.to_string())),
                            }
                        }
                    });

                    // Launch all sub-chunk futures in parallel, then gather results.
                    let sub_results = join_all(fetch_futures).await;

                    for sub_result in sub_results {
                        match sub_result {
                            Ok(blocks) => {
                                for (slot, confirmed_block) in blocks {
                                    successful_blocks
                                        .insert(slot, RawBlock { slot, block: confirmed_block });
                                }
                            },
                            Err((slots_failed, e)) => {
                                error!(
                                    "Failed to fetch sub-chunk from Bigtable ({} slots). Error: {}",
                                    slots_failed.len(),
                                    e
                                );
                                new_failed_slots.extend(slots_failed);
                            },
                        }
                    }
                },

                // ---------------------------------------------------------
                // 2) RPC or other: original slot-by-slot concurrency
                // ---------------------------------------------------------
                _ => {
                    let semaphore = Arc::new(Semaphore::new(args.max_concurrency));

                    let fetch_futures = slots_to_fetch.iter().map(|&slot| {
                        let backfill_source = backfill_source.clone();
                        let semaphore = semaphore.clone();
                        let shutdown_token = shutdown_token.clone();

                        async move {
                            // Acquire concurrency permit
                            let _permit = semaphore.acquire().await;
                            fetch_block_with_retries(backfill_source, slot, shutdown_token).await
                        }
                    });

                    let results = join_all(fetch_futures).await;
                    for result in results {
                        match result {
                            Ok((slot, raw_block)) => {
                                successful_blocks.insert(slot, raw_block);
                            },
                            Err((slot, e)) => {
                                error!("Failed to fetch slot {}: {:?}", slot, e);
                                new_failed_slots.push(slot);
                            },
                        }
                    }
                },
            }

            // -----------------------
            // Attempt to persist
            // -----------------------
            if new_failed_slots.is_empty() {
                // We successfully fetched all requested slots for this batch
                debug!(
                    "All slots fetched for current batch. Saving {} slots to RocksDB.",
                    successful_blocks.len()
                );

                match target_db.raw_blocks_cbor.put_batch(successful_blocks.clone()).await {
                    Ok(_) => {
                        let last_slot = successful_blocks.keys().max().cloned().unwrap_or(0);
                        info!(
                            "Successfully saved batch to RocksDB. Last stored slot: {}",
                            last_slot
                        );
                        break; // Move on to next chunk of `slots`
                    },
                    Err(e) => {
                        // DB write failed
                        error!("Failed to save blocks to RocksDB: {}", e);
                        batch_retries += 1;
                        if batch_retries >= MAX_BATCH_RETRIES {
                            panic!(
                                "Failed to save batch to RocksDB after {} retries. Discarding batch.",
                                MAX_BATCH_RETRIES
                            );
                        } else {
                            warn!(
                                "Retrying batch save {}/{} after {} ms due to error: {}",
                                batch_retries, MAX_BATCH_RETRIES, batch_delay_ms, e
                            );
                            tokio::time::sleep(Duration::from_millis(batch_delay_ms)).await;
                            batch_delay_ms *= 2;
                        }
                    },
                }
            } else {
                // Some slots in the chunk were not fetched
                batch_retries += 1;
                if batch_retries >= MAX_BATCH_RETRIES {
                    panic!(
                        "Failed to fetch all slots in batch after {} retries. Discarding batch. Slots: {:?}",
                        MAX_BATCH_RETRIES,
                        new_failed_slots
                    );
                } else {
                    warn!(
                        "Retrying failed slots {}/{} after {} ms: {:?}",
                        batch_retries, MAX_BATCH_RETRIES, batch_delay_ms, new_failed_slots
                    );
                    slots_to_fetch = new_failed_slots;
                    // Exponential backoff before retrying
                    tokio::time::sleep(Duration::from_millis(batch_delay_ms)).await;
                    batch_delay_ms *= 2;
                }
            }
        }
    }
}
