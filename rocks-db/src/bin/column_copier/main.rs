use entities::models::{OffChainData, RawBlock};
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::column::TypedColumn;
use rocks_db::migrator::MigrationState;
use rocks_db::Storage;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const BATCH_SIZE: usize = 10_000;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), String> {
    // Retrieve the database paths from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} <source_db_path> <destination_db_path>", args[0]);
        std::process::exit(1);
    }
    let source_db_path = &args[1];
    let destination_db_path = &args[2];

    println!("Starting data migration...");

    // Specify the column families you plan to copy
    let columns_to_copy = [RawBlock::NAME, OffChainData::NAME];

    println!(
        "Columns to be copied from {} to {}: {:?}",
        source_db_path, destination_db_path, columns_to_copy
    );
    let secondary_rocks_dir = TempDir::new().unwrap();
    // Copy specified column families
    if let Err(e) = copy_column_families(
        source_db_path,
        secondary_rocks_dir.path().to_str().unwrap(),
        destination_db_path,
        &columns_to_copy,
    )
    .await
    {
        println!("Failed to copy data: {}.", e);
    } else {
        println!("Data copied successfully.");
    }

    Ok(())
}

async fn copy_column_families(
    source_path: &str,
    secondary_source_path: &str,
    destination_path: &str,
    columns_to_copy: &[&'static str],
) -> Result<(), String> {
    let start = Instant::now();
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    // Open source and destination databases
    let source_db = Storage::open_secondary(
        source_path,
        secondary_source_path,
        Arc::new(Mutex::new(JoinSet::new())),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .map_err(|e| e.to_string())?;
    let destination_db = Storage::open(
        destination_path,
        Arc::new(Mutex::new(JoinSet::new())),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .map_err(|e| e.to_string())?;
    let mut set = JoinSet::new();

    // Create a MultiProgress to manage multiple progress bars
    let m = Arc::new(MultiProgress::new());

    columns_to_copy.iter().cloned().for_each(|cf_name| {
        let sdb = source_db.db.clone();
        let ddb = destination_db.db.clone();
        let m_clone = m.clone();

        set.spawn(tokio::task::spawn_blocking(move || {
            // Create a progress bar for this column
            let pb = m_clone.add(ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .unwrap(),
            );
            pb.set_message(format!("Copying column {}", cf_name));
            pb.enable_steady_tick(Duration::from_millis(100));
            let start_column = Instant::now();
            let mut iter = sdb.raw_iterator_cf(&sdb.cf_handle(&cf_name).unwrap());
            iter.seek_to_first();
            let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
            let dest_handle = &ddb.cf_handle(&cf_name).unwrap();
            let mut batch_count = 0;
            while iter.valid() {
                batch.put_cf(dest_handle, iter.key().unwrap(), iter.value().unwrap());
                if batch.len() >= BATCH_SIZE {
                    if let Err(e) = ddb.write(batch) {
                        tracing::error!("Error copying {}: {}", cf_name, e);
                    }
                    batch = rocksdb::WriteBatchWithTransaction::<false>::default();
                    batch_count += 1;
                    // Update the progress bar
                    pb.set_message(format!(
                        "Copying column {} - batches copied: {}",
                        cf_name, batch_count
                    ));
                }
                iter.next();
            }
            if let Err(e) = ddb.write(batch) {
                tracing::error!("Error copying {}: {}", cf_name, e);
            }
            pb.finish_with_message(format!(
                "Finished copying column {} in {} seconds",
                cf_name,
                start_column.elapsed().as_secs()
            ));
        }));
    });
    for _ in 0..columns_to_copy.len() {
        set.join_next().await.unwrap().unwrap().unwrap();
    }

    println!(
        "Migrated all columns in {} seconds",
        start.elapsed().as_secs()
    );

    Ok(())
}
