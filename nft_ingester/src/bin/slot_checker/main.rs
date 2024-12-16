use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use metrics_utils::MetricState;
use rocks_db::column::TypedColumn;
use rocks_db::migrator::MigrationVersions;
use rocks_db::offchain_data::OffChainData;
use rocks_db::Storage;
use tokio::signal;
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tracing::{error, info, warn};

use entities::models::RawBlock;
use interface::slots_dumper::SlotsDumper;
use usecase::bigtable::BigTableClient;
use usecase::slots_collector::SlotsCollector;

use blockbuster::programs::bubblegum::ID as BUBBLEGUM_PROGRAM_ID;

use tokio_util::sync::CancellationToken;

// For InMemorySlotsDumper
use async_trait::async_trait;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the target RocksDB instance with slots
    #[arg(short, long)]
    target_db_path: PathBuf,

    /// Optional big table credentials file path
    /// If not provided, the default credentials file path will be used
    /// Default: ./creds.json
    #[arg(short('c'), long, default_value = "./creds.json")]
    big_table_credentials: String,

    /// Optional big table timeout (default: 1000)
    #[arg(short, long, default_value_t = 1000)]
    big_table_timeout: u32,

    /// Optional comma-separated list of slot numbers to check
    #[arg(short = 's', long)]
    slots: Option<String>,

    /// The first slot to ckeck from
    #[arg(short, long)]
    first_slot: Option<u64>,
}

pub struct InMemorySlotsDumper {
    slots: AsyncMutex<BTreeSet<u64>>,
}

impl InMemorySlotsDumper {
    /// Creates a new instance of `InMemorySlotsDumper`.
    pub fn new() -> Self {
        Self {
            slots: AsyncMutex::new(BTreeSet::new()),
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

// Function to get the last persisted slot from RocksDB
fn get_last_persisted_slots(rocks_db: Arc<Storage>) -> u64 {
    let mut it = rocks_db
        .db
        .raw_iterator_cf(&rocks_db.db.cf_handle(RawBlock::NAME).unwrap());
    it.seek_to_last();
    if !it.valid() {
        return 0;
    }
    it.key()
        .and_then(|b| RawBlock::decode_key(b.to_vec()).ok())
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();
    info!("Starting Slot Checker...");

    let args = Args::parse();
    let metrics_state = MetricState::new();

    // Open target RocksDB in read-only mode
    let db = Arc::new(
        Storage::open_readonly_with_cfs(
            &args.target_db_path,
            vec![RawBlock::NAME, MigrationVersions::NAME, OffChainData::NAME],
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            metrics_state.red_metrics,
        )
        .expect("Failed to open target RocksDB"),
    );

    // Get the last persisted slot from RocksDB
    let last_persisted_slot = get_last_persisted_slots(db.clone());

    info!("Last persisted slot: {}", last_persisted_slot);

    // Initialize BigTableClient
    let bt_connection = Arc::new(
        BigTableClient::connect_new_with(args.big_table_credentials, args.big_table_timeout)
            .await
            .expect("Failed to connect to BigTable"),
    );

    // Initialize the in-memory slots dumper
    let in_mem_dumper = Arc::new(InMemorySlotsDumper::new());

    // Initialize the slots collector
    let slots_collector = SlotsCollector::new(
        in_mem_dumper.clone(),
        bt_connection.big_table_inner_client.clone(),
        metrics_state.backfiller_metrics,
    );

    // Handle Ctrl+C
    let shutdown_token = CancellationToken::new();
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Spawn a task to handle graceful shutdown on Ctrl+C
    {
        let shutdown_token = shutdown_token.clone();
        tokio::spawn(async move {
            if signal::ctrl_c().await.is_ok() {
                info!("Received Ctrl+C, shutting down gracefully...");
                shutdown_token.cancel();
                shutdown_tx.send(()).unwrap();
            } else {
                error!("Unable to listen for shutdown signal");
            }
        });
    }
    // Check if slots or slots_file is provided
    let mut slots_to_check = Vec::new();

    if let Some(slots_str) = args.slots {
        // Parse comma-separated list of slots
        info!("Checking specific slots provided via command line.");
        for part in slots_str.split(',') {
            let slot_str = part.trim();
            if let Ok(slot) = slot_str.parse::<u64>() {
                slots_to_check.push(slot);
            } else {
                warn!("Invalid slot number provided: {}", slot_str);
            }
        }
    }
    if !slots_to_check.is_empty() {
        // Remove duplicates
        let slots_to_check: Vec<u64> = {
            let mut set = HashSet::new();
            slots_to_check
                .into_iter()
                .filter(|x| set.insert(*x))
                .collect()
        };

        let total_slots_to_check = slots_to_check.len();

        info!("Total slots to check: {}", total_slots_to_check);

        // Initialize progress bar for verification
        let progress_bar = ProgressBar::new(total_slots_to_check as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{msg} {bar:40.cyan/blue} {pos}/{len} [{eta_precise}]")
                .unwrap()
                .progress_chars("##-"),
        );
        progress_bar.set_message("Verifying slots");

        let cf_handle = db.db.cf_handle(RawBlock::NAME).unwrap();

        let mut present_slots = Vec::new();
        let mut missing_slots = Vec::new();

        // Sort slots to check for consistent batching
        let mut slots_to_check = slots_to_check;
        slots_to_check.sort_unstable();

        // Prepare keys
        let keys: Vec<_> = slots_to_check
            .iter()
            .map(|&slot| RawBlock::encode_key(slot))
            .collect();

        // Batch get
        let results = db.db.batched_multi_get_cf(&cf_handle, keys, true);

        for (i, result) in results.into_iter().enumerate() {
            let slot = slots_to_check[i];
            match result {
                Ok(Some(_)) => {
                    present_slots.push(slot);
                }
                Ok(None) => {
                    missing_slots.push(slot);
                }
                Err(e) => {
                    error!("Error fetching slot {}: {}", slot, e);
                    missing_slots.push(slot); // Consider as missing on error
                }
            }
            progress_bar.inc(1);
        }

        progress_bar.finish_with_message("Verification complete.");

        // Output results
        info!("Slots present in RocksDB: {:?}", present_slots);

        info!("Slots missing from RocksDB: {:?}", missing_slots);

        return Ok(()); // Exit after processing
    }

    // Store missing slots
    let missing_slots = Arc::new(Mutex::new(Vec::new()));

    info!(
        "Starting to collect slots from {} to {}",
        0, last_persisted_slot
    );

    // Initialize progress bar spinner for collection
    let progress_bar = ProgressBar::new_spinner();
    progress_bar.set_message("Collecting slots...");
    progress_bar.enable_steady_tick(Duration::from_millis(100)); // Update every 100ms

    // Collect slots from last persisted slot down to 0
    in_mem_dumper.clear().await;

    // Start slot collection
    let _ = slots_collector
        .collect_slots(
            &BUBBLEGUM_PROGRAM_ID,
            last_persisted_slot,
            args.first_slot.unwrap_or_default(),
            &shutdown_rx,
        )
        .await;

    // Collection done, stop the spinner
    progress_bar.finish_with_message("Slot collection complete.");

    // Get the collected slots
    let collected_slots = in_mem_dumper.get_sorted_keys().await;
    in_mem_dumper.clear().await;

    if shutdown_token.is_cancelled() {
        info!("Shutdown signal received, stopping...");
        return Ok(());
    }

    let total_slots_to_check = collected_slots.len() as u64;

    info!(
        "Collected {} slots in range {} to {}",
        total_slots_to_check, 0, last_persisted_slot
    );

    // Initialize progress bar for verification
    let progress_bar = ProgressBar::new(total_slots_to_check);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] {msg} {bar:40.cyan/blue} {percent}% ({pos}/{len}) [{eta_precise}]")
            .unwrap()
            .progress_chars("##-"),
    );
    progress_bar.set_message("Verifying slots");

    // Prepare iterators for collected slots and RocksDB keys
    let mut slots_iter = collected_slots.into_iter();
    let mut next_slot = slots_iter.next();

    let cf_handle = db.db.cf_handle(RawBlock::NAME).unwrap();
    let mut db_iter = db.db.raw_iterator_cf(&cf_handle);
    db_iter.seek_to_first();

    let mut current_db_slot = if db_iter.valid() {
        if let Some(key_bytes) = db_iter.key() {
            RawBlock::decode_key(key_bytes.to_vec()).ok()
        } else {
            None
        }
    } else {
        None
    };

    // Verification loop
    while let Some(slot) = next_slot {
        if shutdown_token.is_cancelled() {
            info!("Shutdown signal received, stopping...");
            break;
        }
        if let Some(first_slot) = args.first_slot {
            if slot < first_slot {
                break;
            }
        }

        if let Some(db_slot) = current_db_slot {
            if slot == db_slot {
                // Slot exists in RocksDB
                // Advance both iterators
                next_slot = slots_iter.next();
                db_iter.next();
                current_db_slot = if db_iter.valid() {
                    if let Some(key_bytes) = db_iter.key() {
                        RawBlock::decode_key(key_bytes.to_vec()).ok()
                    } else {
                        None
                    }
                } else {
                    None
                };
            } else if slot < db_slot {
                // Slot is missing in RocksDB
                {
                    let mut missing_slots_lock = missing_slots.lock().unwrap();
                    missing_slots_lock.push(slot);
                }
                // Advance slots iterator
                next_slot = slots_iter.next();
            } else {
                // slot > db_slot
                // Advance RocksDB iterator
                db_iter.next();
                current_db_slot = if db_iter.valid() {
                    if let Some(key_bytes) = db_iter.key() {
                        RawBlock::decode_key(key_bytes.to_vec()).ok()
                    } else {
                        None
                    }
                } else {
                    None
                };
            }
        } else {
            // No more slots in RocksDB, remaining slots are missing
            {
                let mut missing_slots_lock = missing_slots.lock().unwrap();
                missing_slots_lock.push(slot);
                missing_slots_lock.extend(slots_iter);
            }
            break;
        }

        // Update progress bar
        progress_bar.inc(1);
    }

    progress_bar.finish_with_message("Verification complete.");

    // Print missing slots
    let missing_slots = missing_slots.lock().unwrap();
    if !missing_slots.is_empty() {
        info!("Missing slots: {:?}", missing_slots);
    } else {
        println!("All collected slots are present in the RocksDB.");
    }

    Ok(())
}
