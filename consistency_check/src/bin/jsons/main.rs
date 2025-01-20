use std::{
    collections::HashSet,
    fs::File,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use clap::{Parser, Subcommand};
use consistency_check::update_rate;
use entities::enums::TaskStatus;
use indicatif::{ProgressBar, ProgressStyle};
use nft_ingester::init::init_index_storage_with_migration;
use rocks_db::{migrator::MigrationState, Storage};
use tempfile::TempDir;
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use usecase::graceful_stop::graceful_stop;

const WRITER_SLEEP_TIME: Duration = Duration::from_secs(30);
const SLEEP_AFTER_ERROR: Duration = Duration::from_secs(3);

const PG_MAX_CONNECTIONS: u32 = 10;
const PG_MIN_CONNECTIONS: u32 = 1;
const PG_MIGRATIONS_PATH: &str = "../migrations";

const MISSED_JSONS_FILE: &str = "./missed_jsons.csv";

const LINKS_BATCH: usize = 1;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    CheckConsistency {
        #[arg(long)]
        rocks_path: String,
        #[arg(long)]
        postgre_creds: String,
        #[arg(long)]
        batch: usize,
    },
    ChangeStatus {
        #[arg(long)]
        postgre_creds: String,
        #[arg(long)]
        file_path: String,
    },
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    match args.cmd {
        Commands::CheckConsistency { rocks_path, postgre_creds, batch } => {
            check_jsons_consistency(rocks_path, postgre_creds, batch).await;
        },
        Commands::ChangeStatus { postgre_creds, file_path } => {
            change_jsons_status(postgre_creds, file_path).await;
        },
    }
}

async fn change_jsons_status(postgre_creds: String, file_path: String) {
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());

    let index_pg_storage = Arc::new(
        init_index_storage_with_migration(
            &postgre_creds,
            PG_MAX_CONNECTIONS,
            red_metrics.clone(),
            PG_MIN_CONNECTIONS,
            PG_MIGRATIONS_PATH,
            None,
            None,
        )
        .await
        .unwrap(),
    );

    let spinner_style =
        ProgressStyle::with_template("{prefix:>10.bold.dim} {spinner} total={human_pos} {msg}")
            .unwrap();
    let links_spinner =
        Arc::new(ProgressBar::new_spinner().with_style(spinner_style).with_prefix("links"));
    let mut links_processed = 0;

    let mut missed_jsons = csv::Reader::from_path(file_path).unwrap();

    let mut batch = Vec::new();

    info!("Start changing link statuses...");

    for l in missed_jsons.deserialize() {
        let link: String = l.unwrap();

        batch.push(link);

        if batch.len() >= LINKS_BATCH {
            let links_num = batch.len() as u64;
            if let Err(e) = index_pg_storage
                .change_task_status(std::mem::take(&mut batch), TaskStatus::Pending)
                .await
            {
                error!("Could not change statuses for batch: {}", e.to_string());
                tokio::time::sleep(SLEEP_AFTER_ERROR).await;
            } else {
                links_processed += links_num;
                links_spinner.set_position(links_processed);
            }
        }
    }

    info!("All the links are processed");
}

async fn check_jsons_consistency(rocks_path: String, postgre_creds: String, batch: usize) {
    let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let temp_path = temp_dir.path().to_str().unwrap().to_string();
    info!("Opening DB...");
    let db_client = Arc::new(
        Storage::open_secondary(
            rocks_path,
            temp_path,
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap(),
    );
    info!("DB opened");

    let index_pg_storage = Arc::new(
        init_index_storage_with_migration(
            &postgre_creds,
            PG_MAX_CONNECTIONS,
            red_metrics.clone(),
            PG_MIN_CONNECTIONS,
            PG_MIGRATIONS_PATH,
            None,
            None,
        )
        .await
        .unwrap(),
    );

    info!("Selecting count of tasks from the Postgre...");
    let links_count = index_pg_storage.get_tasks_count().await.unwrap() as u64;
    info!("Done");
    let progress_bar = Arc::new(ProgressBar::new(links_count));
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
    let count_of_missed_jsons = Arc::new(AtomicU64::new(0));

    let shutdown_token = CancellationToken::new();
    let (shutdown_for_file_writer_tx, shutdown_for_file_writer_rx) = broadcast::channel::<()>(1);

    let mut writers = JoinSet::new();

    let missed_jsons: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    info!("Launching writer job...");
    let missed_jsons_cloned = missed_jsons.clone();
    writers.spawn(async move {
        let missed_jsons_file =
            File::create(MISSED_JSONS_FILE).expect("Failed to create file for missed jsons");

        let mut missed_jsons_wrt = csv::Writer::from_writer(missed_jsons_file);

        loop {
            let mut f_ch = missed_jsons_cloned.lock().await;
            for t in f_ch.iter() {
                missed_jsons_wrt.write_record([t]).unwrap();
            }
            missed_jsons_wrt.flush().unwrap();
            f_ch.clear();

            drop(f_ch);

            if !shutdown_for_file_writer_rx.is_empty() {
                break;
            }

            tokio::time::sleep(WRITER_SLEEP_TIME).await;
        }

        Ok(())
    });

    info!("Launching rate updater...");
    let assets_processed_clone = assets_processed.clone();
    let shutdown_token_clone = shutdown_token.clone();
    let rate_clone = rate.clone();
    tokio::spawn(update_rate(shutdown_token_clone, assets_processed_clone, rate_clone));

    let mut last_key_in_batch = None;

    info!("Launching main loop...");
    loop {
        match index_pg_storage.get_tasks(batch as i64, last_key_in_batch.clone()).await {
            Ok(tasks) => {
                if tasks.is_empty() {
                    info!(
                        "Got empty list from the PG. Last key in the batch - {:?}",
                        last_key_in_batch.clone()
                    );
                    break;
                }

                progress_bar.inc(tasks.len() as u64);

                last_key_in_batch = Some(tasks.last().unwrap().tsk_id.clone());

                let keys_to_check: Vec<String> = tasks
                    .iter()
                    .filter(|t| t.status == TaskStatus::Success)
                    .map(|t| t.metadata_url.clone())
                    .collect();

                match db_client.asset_offchain_data.batch_get(keys_to_check.clone()).await {
                    Ok(jsons) => {
                        let mut ms_jn = missed_jsons.lock().await;

                        for (i, json) in jsons.iter().enumerate() {
                            if let Some(j) = json {
                                if let Some(m) = &j.metadata {
                                    if m.is_empty() {
                                        ms_jn.insert(keys_to_check.get(i).unwrap().clone());
                                        count_of_missed_jsons.fetch_add(1, Ordering::Relaxed);
                                    }
                                } else {
                                    ms_jn.insert(keys_to_check.get(i).unwrap().clone());
                                    count_of_missed_jsons.fetch_add(1, Ordering::Relaxed);
                                }
                            } else {
                                ms_jn.insert(keys_to_check.get(i).unwrap().clone());
                                count_of_missed_jsons.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    },
                    Err(e) => {
                        error!("Error during selecting data from the Rocks: {}", e.to_string());
                        count_of_missed_jsons
                            .fetch_add(keys_to_check.len() as u64, Ordering::Relaxed);
                        let mut ms_jn = missed_jsons.lock().await;
                        ms_jn.extend(keys_to_check);
                    },
                }

                assets_processed.fetch_add(tasks.len() as u64, Ordering::Relaxed);

                if tasks.len() < batch {
                    assets_processed.fetch_add(tasks.len() as u64, Ordering::Relaxed);
                    info!("Selected from the Postgre less jSONs that expected - meaning it's finished");
                    break;
                }
            },
            Err(e) => {
                error!("Error during selecting data from the Postgre: {}", e.to_string());
                tokio::time::sleep(Duration::from_secs(5)).await;
            },
        }

        let current_missed_jsons = count_of_missed_jsons.load(Ordering::Relaxed);
        let current_processed_jsons = assets_processed.load(Ordering::Relaxed);
        let current_rate = {
            let rate_guard = rate.lock().await;
            *rate_guard
        };
        progress_bar.set_message(format!(
            "Missed JSONs: {}, JSONs processed: {}, Rate: {:.2}/s",
            current_missed_jsons, current_processed_jsons, current_rate
        ));
    }

    info!("Main loop finished its job");

    shutdown_for_file_writer_tx.send(()).unwrap();

    graceful_stop(&mut writers).await;
}
