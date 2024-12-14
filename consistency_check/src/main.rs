use std::{
    collections::{HashMap, HashSet},
    fs::File,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use clap::{command, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use nft_ingester::api::dapi::get_proof_for_assets;
use rocks_db::{migrator::MigrationState, Storage};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_concurrent_merkle_tree::hash::recompute;
use tempfile::TempDir;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use usecase::proofs::MaybeProofChecker;

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
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
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

    let shutdown_token = CancellationToken::new();

    let mut tasks = JoinSet::new();

    let failed_proofs: Arc<Mutex<HashMap<String, Vec<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let failed_check: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    let chunk_size = (keys.len() + config.workers - 1) / config.workers;

    for chunk in keys.chunks(chunk_size) {
        tasks.spawn(verify_tree_batch(
            progress_bar.clone(),
            assets_processed.clone(),
            rate.clone(),
            shutdown_token.clone(),
            chunk.to_vec(),
            rpc_client.clone(),
            db_client.clone(),
            failed_proofs.clone(),
            failed_check.clone(),
        ));
    }

    let assets_processed_clone = assets_processed.clone();
    let rate_clone = rate.clone();
    let shutdown_token_clone = shutdown_token.clone();

    tokio::spawn(async move {
        let mut last_time = std::time::Instant::now();
        let mut last_count = assets_processed_clone.load(Ordering::Relaxed);

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            if shutdown_token_clone.is_cancelled() {
                break;
            }

            let current_time = std::time::Instant::now();
            let current_count = assets_processed_clone.load(Ordering::Relaxed);

            let elapsed = current_time.duration_since(last_time).as_secs_f64();
            let count = current_count - last_count;

            let current_rate = if elapsed > 0.0 {
                (count as f64) / elapsed
            } else {
                0.0
            };

            // Update rate
            {
                let mut rate_guard = rate_clone.lock().await;
                *rate_guard = current_rate;
            }

            // Update for next iteration
            last_time = current_time;
            last_count = current_count;
        }
    });

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(_) => {
                println!("One of the tasks was finished")
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                println!("Task panic: {:?}", err);
            }
            Err(err) => {
                let err = err.to_string();
                println!("Task error: {}", err);
            }
        }
    }

    let file = File::create("./failed_checks.csv").unwrap();
    let mut wrt = csv::Writer::from_writer(file);
    let f_ch = failed_check.lock().await;
    for t in f_ch.iter() {
        wrt.write_record(&[t]).unwrap();
    }
    wrt.flush().unwrap();

    let file = File::create("./failed_proofs.csv").unwrap();
    let mut wrt = csv::Writer::from_writer(file);
    let f_ch = failed_proofs.lock().await;
    for (t, assets) in f_ch.iter() {
        for a in assets {
            wrt.write_record(&[t, a]).unwrap();
        }
    }
    wrt.flush().unwrap();

    progress_bar.finish_with_message("Processing complete");
}

async fn verify_tree_batch(
    progress_bar: Arc<ProgressBar>,
    assets_processed: Arc<AtomicU64>,
    rate: Arc<Mutex<f64>>,
    shutdown_token: CancellationToken,
    trees: Vec<String>,
    rpc: Arc<RpcClient>,
    rocks: Arc<Storage>,
    failed_proofs: Arc<Mutex<HashMap<String, Vec<String>>>>,
    failed_check: Arc<Mutex<HashSet<String>>>,
) -> Result<(), JoinError> {
    let api_metrics = Arc::new(metrics_utils::ApiMetricsConfig::new());
    for tree in trees {
        let t_key = Pubkey::from_str(&tree).unwrap();
        let tree_config_key = Pubkey::find_program_address(
            &[t_key.as_ref()],
            &Pubkey::from_str("BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY").unwrap(),
        )
        .0;

        if let Ok(tree_data) = rpc.get_account_data(&tree_config_key).await {
            if let Ok(des_data) = mpl_bubblegum::accounts::TreeConfig::from_bytes(&tree_data) {
                for asset_index in 0..des_data.num_minted {
                    if shutdown_token.is_cancelled() {
                        println!("Received shutdown signal. Bye");
                        return Ok(());
                    }

                    let asset_id = mpl_bubblegum::utils::get_asset_id(&t_key, asset_index);

                    // TODO: create butch of assets

                    if let Ok(proofs) = get_proof_for_assets::<MaybeProofChecker, Storage>(
                        rocks.clone(),
                        vec![asset_id],
                        None,
                        &None,
                        api_metrics.clone(),
                    )
                    .await
                    {
                        for (_, pr) in proofs {
                            if let Some(pr) = pr {
                                let ar_pr: Vec<[u8; 32]> = pr
                                    .proof
                                    .iter()
                                    .map(|p| Pubkey::from_str(p).unwrap().to_bytes())
                                    .collect();
                                let recomputed_root = recompute(
                                    Pubkey::from_str(pr.leaf.as_ref()).unwrap().to_bytes(),
                                    ar_pr.as_ref(),
                                    pr.node_index as u32,
                                );

                                if recomputed_root != Pubkey::from_str(&pr.root).unwrap().to_bytes()
                                {
                                    write_asset_to_h_map(
                                        failed_proofs.clone(),
                                        tree.clone(),
                                        asset_id.to_string(),
                                    )
                                    .await;
                                }
                            } else {
                                println!("API did not return any proofs for asset");
                                write_asset_to_h_map(
                                    failed_proofs.clone(),
                                    tree.clone(),
                                    asset_id.to_string(),
                                )
                                .await;
                            }
                        }
                    } else {
                        println!("Got an error during selecting data from the rocks");
                        write_asset_to_h_map(
                            failed_proofs.clone(),
                            tree.clone(),
                            asset_id.to_string(),
                        )
                        .await;
                    }

                    let current_assets_processed =
                        assets_processed.fetch_add(1, Ordering::Relaxed) + 1;
                    let current_rate = {
                        let rate_guard = rate.lock().await;
                        *rate_guard
                    };
                    progress_bar.set_message(format!(
                        "Assets Processed: {} Rate: {:.2}/s",
                        current_assets_processed, current_rate
                    ));
                }
            } else {
                println!("Could not deserialise TreeConfig account");
                let mut f_ch = failed_check.lock().await;
                f_ch.insert(tree.clone());
            }
        } else {
            println!("Could not get account data from the RPC");
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
