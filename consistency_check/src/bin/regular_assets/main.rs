use std::{
    collections::HashSet,
    fs::File,
    path::PathBuf,
    rc::Rc,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use clap::Parser;
use consistency_check::update_rate;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use rocks_db::{migrator::MigrationState, Storage};
use snapshot_reader::{append_vec_iter, ArchiveSnapshotExtractor};
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;
use tokio::{
    sync::{mpsc::Receiver, Mutex, Semaphore},
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use usecase::graceful_stop::graceful_stop;

mod snapshot_reader;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_path: String,

    #[arg(long)]
    snapshot_path: String,

    #[arg(long)]
    inner_workers: usize,

    #[arg(long, default_value = "./missed_asset_data.csv")]
    missed_asset_file: PathBuf,

    #[arg(long, default_value = "./missed_mint_info.csv")]
    missed_mint_file: PathBuf,

    #[arg(long, default_value = "./missed_token_acc.csv")]
    missed_token_file: PathBuf,
}

lazy_static! {
    static ref CORE_KEY: Pubkey =
        Pubkey::from_str("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d").unwrap();
    static ref SPL_KEY: Pubkey =
        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    static ref SPL_2022_KEY: Pubkey =
        Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap();
}

const MINT_ACC_DATA_SIZE: usize = 82;

const CHANNEL_SIZE: usize = 100_000_000;

enum AccountType {
    Core,
    Mint,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    // Parse command-line arguments
    let config = Args::parse();

    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let temp_path = temp_dir.path().to_str().unwrap().to_string();
    info!("Opening DB...");
    let db_client = Arc::new(
        Storage::open_secondary(
            config.db_path,
            temp_path,
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            red_metrics,
            MigrationState::Last,
        )
        .unwrap(),
    );
    info!("DB opened");

    let shutdown_token = CancellationToken::new();

    let spinner_style =
        ProgressStyle::with_template("{prefix:>10.bold.dim} {spinner} total={human_pos} {msg}")
            .unwrap();
    let accounts_spinner =
        Arc::new(ProgressBar::new_spinner().with_style(spinner_style).with_prefix("accs"));
    let assets_processed = Arc::new(AtomicU64::new(0));
    let rate = Arc::new(Mutex::new(0.0));

    let counter_missed_asset = Arc::new(AtomicU64::new(0));
    let counter_missed_mint = Arc::new(AtomicU64::new(0));
    let counter_missed_token = Arc::new(AtomicU64::new(0));

    let missed_asset_data: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let missed_mint_info: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let missed_token_acc: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let (nfts_channel_tx, nfts_channel_rx) =
        tokio::sync::mpsc::channel::<(AccountType, Pubkey)>(CHANNEL_SIZE);
    let (fungibles_channel_tx, fungibles_channel_rx) =
        tokio::sync::mpsc::channel::<Pubkey>(CHANNEL_SIZE);

    let mut tasks = JoinSet::new();

    tasks.spawn(process_nfts(
        config.inner_workers,
        db_client.clone(),
        shutdown_token.clone(),
        nfts_channel_rx,
        missed_asset_data.clone(),
        missed_mint_info.clone(),
        assets_processed.clone(),
        rate.clone(),
        accounts_spinner.clone(),
        counter_missed_asset.clone(),
        counter_missed_mint.clone(),
        counter_missed_token.clone(),
    ));

    tasks.spawn(process_fungibles(
        config.inner_workers,
        db_client.clone(),
        shutdown_token.clone(),
        fungibles_channel_rx,
        missed_token_acc.clone(),
        assets_processed.clone(),
        rate.clone(),
        accounts_spinner.clone(),
        counter_missed_asset.clone(),
        counter_missed_mint.clone(),
        counter_missed_token.clone(),
    ));

    info!("Opening snapshot file...");
    let source = File::open(config.snapshot_path).unwrap();
    let mut snapshot_loader = ArchiveSnapshotExtractor::open(source).unwrap();
    info!("Snapshot file opened");

    let assets_processed_clone = assets_processed.clone();
    let shutdown_token_clone = shutdown_token.clone();
    let rate_clone = rate.clone();
    tokio::spawn(update_rate(shutdown_token_clone, assets_processed_clone, rate_clone));

    'outer: for append_vec in snapshot_loader.iter() {
        match append_vec {
            Ok(v) => {
                for account in append_vec_iter(Rc::new(v)) {
                    if shutdown_token.is_cancelled() {
                        break 'outer;
                    }

                    let account = account.access().unwrap();

                    if account.account_meta.owner == *CORE_KEY {
                        if let Err(e) =
                            nfts_channel_tx.send((AccountType::Core, account.meta.pubkey)).await
                        {
                            error!("Could not send core key to the channel: {}", e.to_string());
                        }
                    } else if account.account_meta.owner == *SPL_KEY
                        || account.account_meta.owner == *SPL_2022_KEY
                    {
                        // there only 2 types of accounts for that programs, so if it's not mint it's token account
                        if account.data.len() == MINT_ACC_DATA_SIZE {
                            if let Err(e) =
                                nfts_channel_tx.send((AccountType::Mint, account.meta.pubkey)).await
                            {
                                error!("Could not send mint key to the channel: {}", e.to_string());
                            }
                        } else if let Err(e) = fungibles_channel_tx.send(account.meta.pubkey).await
                        {
                            error!(
                                "Could not send token account to the channel: {}",
                                e.to_string()
                            );
                        }
                    }
                }
            },
            Err(error) => error!("append_vec: {:?}", error),
        };
    }

    // close channels
    drop(nfts_channel_tx);
    drop(fungibles_channel_tx);

    graceful_stop(&mut tasks).await;

    if let Err(e) = write_data_to_file(config.missed_asset_file, &missed_asset_data).await {
        error!("Could not save keys with missed asset data: {}", e);
    }

    if let Err(e) = write_data_to_file(config.missed_mint_file, &missed_mint_info).await {
        error!("Could not save keys with missed mint data: {}", e);
    }

    if let Err(e) = write_data_to_file(config.missed_token_file, &missed_token_acc).await {
        error!("Could not save keys with missed token account data: {}", e);
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_nfts(
    inner_workers: usize,
    rocks_db: Arc<Storage>,
    shutdown_token: CancellationToken,
    mut nfts_channel_rx: Receiver<(AccountType, Pubkey)>,
    missed_asset_data: Arc<Mutex<HashSet<String>>>,
    missed_mint_info: Arc<Mutex<HashSet<String>>>,
    assets_processed: Arc<AtomicU64>,
    rate: Arc<Mutex<f64>>,
    progress_bar: Arc<ProgressBar>,
    counter_missed_asset: Arc<AtomicU64>,
    counter_missed_mint: Arc<AtomicU64>,
    counter_missed_token: Arc<AtomicU64>,
) -> Result<(), JoinError> {
    // spawn not more then N threads
    let semaphore = Arc::new(Semaphore::new(inner_workers));

    loop {
        if shutdown_token.is_cancelled() {
            break;
        }

        match nfts_channel_rx.recv().await {
            Some((acc_type, key)) => {
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                let rocks_db_cloned = rocks_db.clone();
                let missed_asset_data_cloned = missed_asset_data.clone();
                let missed_mint_info_cloned = missed_mint_info.clone();

                let assets_processed_cloned = assets_processed.clone();
                let rate_cloned = rate.clone();
                let progress_bar_cloned = progress_bar.clone();
                let counter_missed_asset_cloned = counter_missed_asset.clone();
                let counter_missed_mint_cloned = counter_missed_mint.clone();
                let counter_missed_token_cloned = counter_missed_token.clone();

                tokio::spawn(async move {
                    match rocks_db_cloned.asset_data.has_key(key).await {
                        Ok(exist) => {
                            if !exist {
                                let _ = counter_missed_asset_cloned.fetch_add(1, Ordering::Relaxed);
                                let mut m_d = missed_asset_data_cloned.lock().await;
                                m_d.insert(key.to_string());
                                drop(m_d);
                            }
                        },
                        Err(e) => {
                            error!(
                                "Error during checking asset data key existence: {}",
                                e.to_string()
                            );
                        },
                    }
                    match acc_type {
                        AccountType::Core => {}, // already checked above
                        // if we've got mint account we also should check spl_mints column
                        AccountType::Mint => match rocks_db_cloned.spl_mints.has_key(key).await {
                            Ok(exist) => {
                                if !exist {
                                    let _ =
                                        counter_missed_mint_cloned.fetch_add(1, Ordering::Relaxed);
                                    let mut m_d = missed_mint_info_cloned.lock().await;
                                    m_d.insert(key.to_string());
                                    drop(m_d);
                                }
                            },
                            Err(e) => {
                                error!(
                                    "Error during checking mint key existence: {}",
                                    e.to_string()
                                );
                            },
                        },
                    }

                    let current_assets_processed =
                        assets_processed_cloned.fetch_add(1, Ordering::Relaxed) + 1;
                    let current_rate = {
                        let rate_guard = rate_cloned.lock().await;
                        *rate_guard
                    };

                    if current_assets_processed % 1024 == 0 {
                        progress_bar_cloned.set_position(current_assets_processed);
                    }

                    let m_d = counter_missed_asset_cloned.load(Ordering::Relaxed);
                    let m_m = counter_missed_mint_cloned.load(Ordering::Relaxed);
                    let m_t = counter_missed_token_cloned.load(Ordering::Relaxed);

                    progress_bar_cloned.set_message(format!(
                        "Rate: {:.2}/s Missed asset data: {}, Missed mint data: {}, Missed token data: {}",
                        current_rate, m_d, m_m, m_t
                    ));

                    drop(permit);
                });
            },
            None => {
                // if None is received - channel was closed
                break;
            },
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_fungibles(
    inner_workers: usize,
    rocks_db: Arc<Storage>,
    shutdown_token: CancellationToken,
    mut fungibles_channel_rx: Receiver<Pubkey>,
    missed_token_acc: Arc<Mutex<HashSet<String>>>,
    assets_processed: Arc<AtomicU64>,
    rate: Arc<Mutex<f64>>,
    progress_bar: Arc<ProgressBar>,
    counter_missed_asset: Arc<AtomicU64>,
    counter_missed_mint: Arc<AtomicU64>,
    counter_missed_token: Arc<AtomicU64>,
) -> Result<(), JoinError> {
    // spawn not more then N threads
    let semaphore = Arc::new(Semaphore::new(inner_workers));

    loop {
        if shutdown_token.is_cancelled() {
            break;
        }

        match fungibles_channel_rx.recv().await {
            Some(key) => {
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                let rocks_db_cloned = rocks_db.clone();
                let missed_token_acc_cloned = missed_token_acc.clone();

                let assets_processed_cloned = assets_processed.clone();
                let rate_cloned = rate.clone();
                let progress_bar_cloned = progress_bar.clone();
                let counter_missed_asset_cloned = counter_missed_asset.clone();
                let counter_missed_mint_cloned = counter_missed_mint.clone();
                let counter_missed_token_cloned = counter_missed_token.clone();

                tokio::spawn(async move {
                    match rocks_db_cloned.token_accounts.has_key(key).await {
                        Ok(exist) => {
                            if !exist {
                                let _ = counter_missed_token_cloned.fetch_add(1, Ordering::Relaxed);
                                let mut m_d = missed_token_acc_cloned.lock().await;
                                m_d.insert(key.to_string());
                                drop(m_d);
                            }
                        },
                        Err(e) => {
                            error!(
                                "Error during checking token accounts key existence: {}",
                                e.to_string()
                            );
                        },
                    }

                    let current_assets_processed =
                        assets_processed_cloned.fetch_add(1, Ordering::Relaxed) + 1;
                    let current_rate = {
                        let rate_guard = rate_cloned.lock().await;
                        *rate_guard
                    };

                    if current_assets_processed % 1024 == 0 {
                        progress_bar_cloned.set_position(current_assets_processed);
                    }

                    let m_d = counter_missed_asset_cloned.load(Ordering::Relaxed);
                    let m_m = counter_missed_mint_cloned.load(Ordering::Relaxed);
                    let m_t = counter_missed_token_cloned.load(Ordering::Relaxed);

                    progress_bar_cloned.set_message(format!(
                        "Rate: {:.2}/s Missed asset data: {}, Missed mint data: {}, Missed token data: {}",
                        current_rate, m_d, m_m, m_t
                    ));

                    drop(permit);
                });
            },
            None => {
                // if None is received - channel was closed
                break;
            },
        }
    }
    Ok(())
}

async fn write_data_to_file(
    file_name: PathBuf,
    keys: &Arc<tokio::sync::Mutex<HashSet<String>>>,
) -> Result<(), String> {
    let file = File::create(file_name).map_err(|e| e.to_string())?;
    let mut wrt = csv::Writer::from_writer(file);

    let keys = keys.lock().await;
    for key in keys.iter() {
        wrt.write_record([key]).map_err(|e| e.to_string())?;
    }
    wrt.flush().map_err(|e| e.to_string())?;
    Ok(())
}
