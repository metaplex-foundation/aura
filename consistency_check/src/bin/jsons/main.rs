use std::{
    collections::HashSet,
    fs::File,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use clap::Parser;
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    rocks_path: String,

    #[arg(long)]
    postgre_creds: String,

    #[arg(long)]
    batch: usize,
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
            config.rocks_path,
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
            &config.postgre_creds,
            10,
            red_metrics.clone(),
            1,
            "../migrations",
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
            File::create("./missed_jsons.csv").expect("Failed to create file for missed jsons");

        let mut missed_jsons_wrt = csv::Writer::from_writer(missed_jsons_file);

        loop {
            let mut f_ch = missed_jsons_cloned.lock().await;
            for t in f_ch.iter() {
                missed_jsons_wrt.write_record(&[t]).unwrap();
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
    tokio::spawn(update_rate(
        shutdown_token_clone,
        assets_processed_clone,
        rate_clone,
    ));

    let mut last_key_in_batch = None;

    info!("Launching main loop...");
    loop {
        match index_pg_storage
            .get_tasks(config.batch as i64, last_key_in_batch.clone())
            .await
        {
            Ok(tasks) => {
                last_key_in_batch = Some(tasks.last().unwrap().tsk_id.clone());

                let keys_to_check: Vec<String> = tasks
                    .iter()
                    .filter(|t| t.status == TaskStatus::Success)
                    .map(|t| t.metadata_url.clone())
                    .collect();

                match db_client
                    .asset_offchain_data
                    .batch_get(keys_to_check.clone())
                    .await
                {
                    Ok(jsons) => {
                        let mut ms_jn = missed_jsons.lock().await;

                        for (i, json) in jsons.iter().enumerate() {
                            if let Some(j) = json {
                                if j.metadata.is_empty() {
                                    ms_jn.insert(j.url.clone());
                                    count_of_missed_jsons.fetch_add(1, Ordering::Relaxed);
                                }
                            } else {
                                ms_jn.insert(keys_to_check.get(i).unwrap().clone());
                                count_of_missed_jsons.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error during selecting data from the Rocks: {}",
                            e.to_string()
                        );
                        count_of_missed_jsons
                            .fetch_add(keys_to_check.len() as u64, Ordering::Relaxed);
                        let mut ms_jn = missed_jsons.lock().await;
                        ms_jn.extend(keys_to_check);
                    }
                }

                assets_processed.fetch_add(tasks.len() as u64, Ordering::Relaxed);

                if tasks.len() < config.batch {
                    info!("Selected from the Postgre less jSONs that expected - meaning it's finished");
                    break;
                }
            }
            Err(e) => {
                error!(
                    "Error during selecting data from the Postgre: {}",
                    e.to_string()
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
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

    graceful_stop(&mut writers).await;
}
