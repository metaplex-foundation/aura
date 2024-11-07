use async_trait::async_trait;
use backfill_rpc::rpc::BackfillRPC;
use clap::Parser;
use entities::models::RawBlock;
use futures::future::join_all;
use interface::signature_persistence::BlockProducer;
use interface::slot_getter::FinalizedSlotGetter;
use interface::slots_dumper::SlotsDumper;
use metrics_utils::utils::start_metrics;
use metrics_utils::{MetricState, MetricsTrait};
use rocks_db::{column::TypedColumn, Storage};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, Mutex};
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use usecase::bigtable::BigTableClient;
use usecase::slots_collector::SlotsCollector;

const MAX_RETRIES: usize = 5;
const INITIAL_DELAY_MS: u64 = 100;

const MAX_BATCH_RETRIES: usize = 5;
const INITIAL_BATCH_DELAY_MS: u64 = 500;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    long_about = "Slot persister reads the slot data from the BigTable and persists it to the RocksDB."
)]
struct Args {
    /// Path to the target RocksDB instance with slots
    #[arg(short, long)]
    target_db_path: PathBuf,

    /// RPC host
    #[arg(short, long)]
    rpc_host: String,

    /// Optional starting slot number, this will override the last saved slot in the RocksDB
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Big table credentials file path
    #[arg(short, long)]
    big_table_credentials: String,

    /// Optional big table timeout (default: 1000)
    #[arg(short, long, default_value_t = 1000)]
    big_table_timeout: u32,

    /// Metrics port
    /// Default: 9090
    #[arg(short, long, default_value = "9090")]
    metrics_port: u16,

    /// Number of slots to process in each batch
    #[arg(short, long, default_value_t = 100)]
    chunk_size: usize,

    /// Maximum number of concurrent requests
    #[arg(short, long, default_value_t = 10)]
    max_concurrency: usize,
}
pub struct InMemorySlotsDumper {
    slots: Mutex<BTreeSet<u64>>,
}
impl Default for InMemorySlotsDumper {
    fn default() -> Self {
        Self::new()
    }
}
impl InMemorySlotsDumper {
    /// Creates a new instance of `InMemorySlotsDumper`.
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(BTreeSet::new()),
        }
    }

    /// Retrieves the sorted keys in ascending order.
    pub async fn get_sorted_keys(&self) -> Vec<u64> {
        let slots = self.slots.lock().await;
        slots.iter().cloned().collect()
    }

    /// Clears the internal storage to reuse it.
    pub async fn clear(&self) {
        let mut slots = self.slots.lock().await;
        slots.clear();
    }
}

#[async_trait]
impl SlotsDumper for InMemorySlotsDumper {
    async fn dump_slots(&self, slots: &[u64]) {
        let mut storage = self.slots.lock().await;
        for &slot in slots {
            storage.insert(slot);
        }
    }
}

pub fn get_last_persisted_slot(rocks_db: Arc<Storage>) -> u64 {
    let mut it = rocks_db
        .db
        .raw_iterator_cf(&rocks_db.db.cf_handle(RawBlock::NAME).unwrap());
    it.seek_to_last();
    if !it.valid() {
        return 0;
    }
    it.key()
        .map(|b| RawBlock::decode_key(b.to_vec()).unwrap_or_default())
        .unwrap_or_default()
}

#[derive(Debug)]
enum FetchError {
    Cancelled,
    Other(String),
}

async fn fetch_block_with_retries(
    bt_connection: Arc<BigTableClient>,
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
            let bt_connection = bt_connection.clone();
            let shutdown_token = shutdown_token.clone();
            async move {
                if shutdown_token.is_cancelled() {
                    info!("Fetch cancelled for slot {} due to shutdown signal.", slot);
                    Err((slot, FetchError::Cancelled))
                } else {
                    match bt_connection
                        .get_block(slot, None::<Arc<BigTableClient>>)
                        .await
                    {
                        Ok(block_data) => Ok((
                            slot,
                            RawBlock {
                                slot,
                                block: block_data,
                            },
                        )),
                        Err(e) => {
                            error!("Error fetching block for slot {}: {}", slot, e);
                            Err((slot, FetchError::Other(e.to_string())))
                        }
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
        Storage::open_cfs(
            &args.target_db_path,
            vec![RawBlock::NAME],
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
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    let rpc_client = Arc::new(BackfillRPC::connect(args.rpc_host));

    let bt_connection = Arc::new(
        BigTableClient::connect_new_with(args.big_table_credentials, args.big_table_timeout)
            .await
            .expect("expected to connect to big table"),
    );

    let in_mem_dumper = Arc::new(InMemorySlotsDumper::new());
    let slots_collector = SlotsCollector::new(
        in_mem_dumper.clone(),
        bt_connection.big_table_inner_client.clone(),
        metrics_state.backfiller_metrics.clone(),
    );
    let wait_period = Duration::from_secs(5);
    let mut start_slot = start_slot;
    loop {
        if shutdown_token.is_cancelled() {
            info!("Shutdown signal received, exiting loop...");
            break;
        }

        match rpc_client.get_finalized_slot().await {
            Ok(finalized_slot) => {
                let top_collected_slot = slots_collector
                    .collect_slots(
                        &blockbuster::programs::bubblegum::ID,
                        finalized_slot,
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
                    finalized_slot
                );
                // slots has all the slots numbers we need to downlaod and persist. Slots should be downloaded concurrently, but no slot shouold be persisted if the previous slot is not persisted.

                // Process slots in batches
                for batch in slots.chunks(args.chunk_size) {
                    if shutdown_token.is_cancelled() {
                        info!("Shutdown signal received during batch processing, exiting...");
                        break;
                    }

                    let mut batch_retries = 0;
                    let mut batch_delay_ms = INITIAL_BATCH_DELAY_MS;

                    // Initialize the list of slots to fetch and the map of successful blocks
                    let mut slots_to_fetch: Vec<u64> = batch.to_vec();
                    let mut successful_blocks: HashMap<u64, RawBlock> = HashMap::new();

                    // Retry loop for the batch
                    loop {
                        if shutdown_token.is_cancelled() {
                            info!("Shutdown signal received during batch processing, exiting...");
                            break;
                        }

                        let semaphore = Arc::new(Semaphore::new(args.max_concurrency));

                        let fetch_futures = slots_to_fetch.iter().map(|&slot| {
                            let bt_connection = bt_connection.clone();
                            let semaphore = semaphore.clone();
                            let shutdown_token = shutdown_token.clone();

                            async move {
                                let _permit = semaphore.acquire().await;
                                fetch_block_with_retries(bt_connection, slot, shutdown_token).await
                            }
                        });

                        let results = join_all(fetch_futures).await;

                        let mut new_failed_slots = Vec::new();

                        for result in results {
                            match result {
                                Ok((slot, raw_block)) => {
                                    successful_blocks.insert(slot, raw_block);
                                }
                                Err((slot, e)) => {
                                    new_failed_slots.push(slot);
                                    error!("Failed to fetch slot {}: {:?}", slot, e);
                                }
                            }
                        }

                        if new_failed_slots.is_empty() {
                            // All slots fetched successfully, save to database
                            if let Err(e) = target_db
                                .raw_blocks_cbor
                                .put_batch_cbor(successful_blocks.clone())
                                .await
                            {
                                error!("Failed to save blocks to RocksDB: {}", e);
                                // Handle error or retry saving as needed
                                batch_retries += 1;
                                if batch_retries >= MAX_BATCH_RETRIES {
                                    panic!(
                                        "Failed to save batch to RocksDB after {} retries. Discarding batch.",
                                        MAX_BATCH_RETRIES
                                    );
                                } else {
                                    warn!(
                                        "Retrying batch save {}/{} after {} ms due to error.",
                                        batch_retries, MAX_BATCH_RETRIES, batch_delay_ms
                                    );
                                    tokio::time::sleep(Duration::from_millis(batch_delay_ms)).await;
                                    batch_delay_ms *= 2;
                                }
                            } else {
                                // Successfully saved, proceed to next batch
                                break;
                            }
                        } else {
                            batch_retries += 1;
                            if batch_retries >= MAX_BATCH_RETRIES {
                                panic!(
                                    "Failed to fetch all slots in batch after {} retries. Discarding batch.",
                                    MAX_BATCH_RETRIES
                                );
                            } else {
                                warn!(
                                    "Retrying failed slots {}/{} after {} ms: {:?}",
                                    batch_retries,
                                    MAX_BATCH_RETRIES,
                                    batch_delay_ms,
                                    new_failed_slots
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
            Err(e) => {
                error!("Error getting finalized slot: {}", e);
            }
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
    Ok(())
}
