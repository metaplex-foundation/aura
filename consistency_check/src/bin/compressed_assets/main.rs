use std::{
    collections::{HashMap, HashSet},
    fs::File,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use clap::{command, Parser};
use consistency_check::update_rate;
use indicatif::{ProgressBar, ProgressStyle};
use nft_ingester::api::dapi::get_proof_for_assets;
use rocks_db::{migrator::MigrationState, Storage};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::{ParsePubkeyError, Pubkey};
use spl_concurrent_merkle_tree::hash::recompute;
use tempfile::TempDir;
use tokio::{
    sync::{broadcast, Mutex, Semaphore},
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use usecase::{graceful_stop::graceful_stop, proofs::MaybeProofChecker};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    rpc_endpoint: String,

    #[arg(long)]
    db_path: String,

    #[arg(long)]
    trees_file_path: String,

    #[arg(long)]
    workers: usize,

    #[arg(long)]
    inner_workers: usize,
}

const WRITER_SLEEP_TIME: Duration = Duration::from_secs(30);
const FAILED_CHECKS_FILE: &str = "./failed_checks.csv";
const FAILED_PROOFS_FILE: &str = "./failed_proofs.csv";

#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    // Parse command-line arguments
    let config = Args::parse();

    let file = File::open(config.trees_file_path).unwrap();
    let mut rdr = csv::Reader::from_reader(file);

    let keys: Vec<String> = rdr
        .records()
        .filter_map(Result::ok)
        .map(|r| r.as_slice().to_string())
        .collect();

    let rpc_client = Arc::new(RpcClient::new(config.rpc_endpoint));

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

    let progress_bar = Arc::new(ProgressBar::new(keys.len() as u64));
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent}% \
                    ({pos}/{len}) {msg}",
            )
            .expect("Failed to set progress bar style")
            .progress_chars("#>-"),
    );
    let assets_processed = Arc::new(AtomicU64::new(0));
    let rate = Arc::new(Mutex::new(0.0));
    let assets_with_failed_proofs = Arc::new(AtomicU64::new(0));
    let assets_with_missed_proofs = Arc::new(AtomicU64::new(0));

    let shutdown_token = CancellationToken::new();
    let (shutdown_for_file_writer_tx, shutdown_for_file_writer_rx) = broadcast::channel::<()>(1);

    let mut tasks = JoinSet::new();
    let mut writers = JoinSet::new();

    let failed_proofs: Arc<Mutex<HashMap<String, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let failed_check: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let chunk_size = (keys.len() + config.workers - 1) / config.workers;

    for chunk in keys.chunks(chunk_size) {
        tasks.spawn(verify_tree_batch(
            progress_bar.clone(),
            assets_processed.clone(),
            assets_with_failed_proofs.clone(),
            assets_with_missed_proofs.clone(),
            rate.clone(),
            shutdown_token.clone(),
            chunk.to_vec(),
            rpc_client.clone(),
            db_client.clone(),
            failed_proofs.clone(),
            failed_check.clone(),
            config.inner_workers,
        ));
    }

    let assets_processed_clone = assets_processed.clone();
    let rate_clone = rate.clone();
    let shutdown_token_clone = shutdown_token.clone();

    // update rate on the background
    tokio::spawn(update_rate(
        shutdown_token_clone,
        assets_processed_clone,
        rate_clone,
    ));

    // write found problematic assets to the files
    writers.spawn(async move {
        let failed_checks_file =
            File::create(FAILED_CHECKS_FILE).expect("Failed to create file for failed check trees");
        let failed_proofs_file =
            File::create(FAILED_PROOFS_FILE).expect("Failed to create file for failed proofs");

        let mut failed_checks_wrt = csv::Writer::from_writer(failed_checks_file);
        let mut failed_proofs_wrt = csv::Writer::from_writer(failed_proofs_file);

        loop {
            if let Err(e) = process_failed_checks(&mut failed_checks_wrt, &failed_check).await {
                error!("Error writing failed checks: {}", e);
            }

            if let Err(e) = process_failed_proofs(&mut failed_proofs_wrt, &failed_proofs).await {
                error!("Error writing failed proofs: {}", e);
            }

            if !shutdown_for_file_writer_rx.is_empty() {
                break;
            }

            tokio::time::sleep(WRITER_SLEEP_TIME).await;
        }

        Ok(())
    });

    graceful_stop(&mut tasks).await;

    shutdown_for_file_writer_tx.send(()).unwrap();

    graceful_stop(&mut writers).await;

    progress_bar.finish_with_message("Processing complete");
}

async fn verify_tree_batch(
    progress_bar: Arc<ProgressBar>,
    assets_processed: Arc<AtomicU64>,
    assets_with_failed_proofs: Arc<AtomicU64>,
    assets_with_missed_proofs: Arc<AtomicU64>,
    rate: Arc<Mutex<f64>>,
    shutdown_token: CancellationToken,
    trees: Vec<String>,
    rpc: Arc<RpcClient>,
    rocks: Arc<Storage>,
    failed_proofs: Arc<Mutex<HashMap<String, Vec<String>>>>,
    failed_check: Arc<Mutex<HashSet<String>>>,
    inner_workers: usize,
) -> Result<(), JoinError> {
    let api_metrics = Arc::new(metrics_utils::ApiMetricsConfig::new());

    for tree in trees {
        let t_key = match Pubkey::from_str(&tree) {
            Ok(t) => t,
            Err(e) => {
                error!("Could not convert tree string into Pubkey: {}", e);
                let mut f_ch = failed_check.lock().await;
                f_ch.insert(tree.clone());
                continue;
            }
        };

        let tree_config_key = Pubkey::find_program_address(&[t_key.as_ref()], &mpl_bubblegum::ID).0;

        if let Ok(tree_data) = rpc.get_account_data(&tree_config_key).await {
            if let Ok(des_data) = mpl_bubblegum::accounts::TreeConfig::from_bytes(&tree_data) {
                // spawn not more then N threads
                let semaphore = Arc::new(Semaphore::new(inner_workers));

                for asset_index in 0..des_data.num_minted {
                    if shutdown_token.is_cancelled() {
                        info!("Received shutdown signal");
                        return Ok(());
                    }

                    let permit = semaphore.clone().acquire_owned().await.unwrap();

                    let rocks_cloned = rocks.clone();
                    let api_metrics_cloned = api_metrics.clone();
                    let failed_proofs_cloned = failed_proofs.clone();
                    let tree_cloned = tree.clone();
                    let assets_processed_cloned = assets_processed.clone();
                    let assets_with_failed_proofs_cloned = assets_with_failed_proofs.clone();
                    let assets_with_missed_proofs_cloned = assets_with_missed_proofs.clone();
                    let rate_cloned = rate.clone();
                    let progress_bar_cloned = progress_bar.clone();
                    tokio::spawn(async move {
                        let asset_id = mpl_bubblegum::utils::get_asset_id(&t_key, asset_index);

                        if let Ok(proofs) = get_proof_for_assets::<MaybeProofChecker, Storage>(
                            rocks_cloned,
                            vec![asset_id],
                            None,
                            &None,
                            api_metrics_cloned,
                        )
                        .await
                        {
                            for (asset, asset_proof_resp) in proofs {
                                if let Some(asset_proof_resp) = asset_proof_resp {
                                    let asset_proofs: Result<Vec<[u8; 32]>, ParsePubkeyError> =
                                        asset_proof_resp
                                            .proof
                                            .iter()
                                            .map(|p| {
                                                Pubkey::from_str(p).map(|pubkey| pubkey.to_bytes())
                                            })
                                            .collect();

                                    match asset_proofs {
                                        Ok(asset_proofs) => {
                                            let leaf_pubkey = match Pubkey::from_str(
                                                asset_proof_resp.leaf.as_ref(),
                                            ) {
                                                Ok(l) => l.to_bytes(),
                                                Err(e) => {
                                                    save_asset_w_inv_proofs(
                                                        assets_with_failed_proofs_cloned.clone(),
                                                        failed_proofs_cloned.clone(),
                                                        tree_cloned.clone(),
                                                        asset.clone(),
                                                        Some(e.to_string()),
                                                    )
                                                    .await;
                                                    continue;
                                                }
                                            };

                                            let root_from_api =
                                                match Pubkey::from_str(&asset_proof_resp.root) {
                                                    Ok(r) => r.to_bytes(),
                                                    Err(e) => {
                                                        save_asset_w_inv_proofs(
                                                            assets_with_failed_proofs_cloned
                                                                .clone(),
                                                            failed_proofs_cloned.clone(),
                                                            tree_cloned.clone(),
                                                            asset.clone(),
                                                            Some(e.to_string()),
                                                        )
                                                        .await;
                                                        continue;
                                                    }
                                                };

                                            let recomputed_root = recompute(
                                                leaf_pubkey,
                                                asset_proofs.as_ref(),
                                                asset_proof_resp.node_index as u32,
                                            );

                                            if recomputed_root != root_from_api {
                                                save_asset_w_inv_proofs(
                                                    assets_with_failed_proofs_cloned.clone(),
                                                    failed_proofs_cloned.clone(),
                                                    tree_cloned.clone(),
                                                    asset.clone(),
                                                    None,
                                                )
                                                .await;
                                            }
                                        }
                                        Err(_) => {
                                            save_asset_w_inv_proofs(
                                                assets_with_failed_proofs_cloned.clone(),
                                                failed_proofs_cloned.clone(),
                                                tree_cloned.clone(),
                                                asset.clone(),
                                                Some("Could not convert all the received proofs to the Pubkey".to_string()),
                                            ).await;
                                        }
                                    }
                                } else {
                                    save_asset_w_inv_proofs(
                                        assets_with_missed_proofs_cloned.clone(),
                                        failed_proofs_cloned.clone(),
                                        tree_cloned.clone(),
                                        asset.clone(),
                                        Some(format!(
                                            "API did not return any proofs for asset: {:?}",
                                            asset
                                        )),
                                    )
                                    .await;
                                }
                            }
                        } else {
                            save_asset_w_inv_proofs(
                                assets_with_missed_proofs_cloned.clone(),
                                failed_proofs_cloned.clone(),
                                tree_cloned.clone(),
                                asset_id.to_string(),
                                Some(
                                    "Got an error during selecting data from the rocks".to_string(),
                                ),
                            )
                            .await;
                        }

                        let current_assets_processed =
                            assets_processed_cloned.fetch_add(1, Ordering::Relaxed) + 1;
                        let current_rate = {
                            let rate_guard = rate_cloned.lock().await;
                            *rate_guard
                        };
                        let current_assets_with_failed_proofs =
                            assets_with_failed_proofs_cloned.load(Ordering::Relaxed);
                        let current_assets_with_missed_proofs =
                            assets_with_missed_proofs_cloned.load(Ordering::Relaxed);
                        progress_bar_cloned.set_message(format!(
                            "Assets with failed proofs: {} Assets with missed proofs: {} Assets Processed: {} Rate: {:.2}/s",
                            current_assets_with_failed_proofs, current_assets_with_missed_proofs, current_assets_processed, current_rate
                        ));

                        drop(permit);
                    });
                }
            } else {
                error!("Could not deserialise TreeConfig account");
                let mut f_ch = failed_check.lock().await;
                f_ch.insert(tree.clone());
            }
        } else {
            error!("Could not get account data from the RPC");
            let mut f_ch = failed_check.lock().await;
            f_ch.insert(tree.clone());
        }

        progress_bar.inc(1);
    }

    Ok(())
}

async fn write_asset_to_h_map(
    h_map: Arc<Mutex<HashMap<String, Vec<String>>>>,
    tree: String,
    asset: String,
) {
    let mut f_ch = h_map.lock().await;
    if let Some(arr_k) = f_ch.get_mut(&tree) {
        arr_k.push(asset.to_string());
    } else {
        f_ch.insert(tree.clone(), vec![asset.to_string()]);
    }
}

async fn save_asset_w_inv_proofs(
    counter: Arc<AtomicU64>,
    failed_proofs: Arc<Mutex<HashMap<String, Vec<String>>>>,
    tree: String,
    asset: String,
    error: Option<String>,
) {
    if let Some(e) = error {
        error!(e);
    }
    let _ = counter.fetch_add(1, Ordering::Relaxed);
    write_asset_to_h_map(failed_proofs.clone(), tree.clone(), asset).await;
}

async fn process_failed_checks(
    writer: &mut csv::Writer<File>,
    failed_check: &Arc<tokio::sync::Mutex<HashSet<String>>>,
) -> Result<(), csv::Error> {
    let mut f_ch = failed_check.lock().await;
    for t in f_ch.iter() {
        writer.write_record(&[t])?;
    }
    writer.flush()?;
    f_ch.clear();
    Ok(())
}

async fn process_failed_proofs(
    writer: &mut csv::Writer<File>,
    failed_proofs: &Arc<tokio::sync::Mutex<std::collections::HashMap<String, Vec<String>>>>,
) -> Result<(), csv::Error> {
    let mut f_ch = failed_proofs.lock().await;
    for (t, assets) in f_ch.iter() {
        for a in assets {
            writer.write_record(&[t, a])?;
        }
    }
    writer.flush()?;
    f_ch.clear();
    Ok(())
}
