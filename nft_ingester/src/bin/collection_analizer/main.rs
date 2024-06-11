use std::collections::HashMap;
use std::sync::Arc;

use bincode::deserialize;
use clap::{arg, Parser};
use csv::Writer;
use itertools::Itertools;
use metrics_utils::red::RequestErrorDurationMetrics;
use nft_ingester::config::init_logger;
use rocks_db::asset::AssetLeaf;
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::info;

use nft_ingester::error::IngesterError;
use rocks_db::Storage;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, required = true)]
    pub source_db: String,
    #[arg(short, long, required = true)]
    pub target_csv: String,
    #[arg(short, long, default_value_t = String::from("info"))]
    log_level: String,
}

/// This binary is used to analize trees and collections. It collects all the trees and collections and calculates the number of assets in each collection-tree pair. A special zeroed collection key is used to identify a no-collection.
/// The iteration is done over the leafs first, from where we get the tree and the results are written to a csv file with the tree, collection, number_of_assets structure.
#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config = Args::parse();
    init_logger(config.log_level.as_str());

    info!("Started...");

    let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));
    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    let secondary_rocks_dir = TempDir::new().unwrap();
    let source_storage = Storage::open_secondary(
        &config.source_db,
        secondary_rocks_dir.path().to_str().unwrap(),
        mutexed_tasks.clone(),
        red_metrics.clone(),
        rocks_db::migrator::MigrationState::Last,
    )
    .unwrap();

    info!("starting processing assets over chunks...");
    let results = process_assets(&source_storage).await.unwrap();
    info!("Writing results to csv...");
    write_results_to_csv(results, &config.target_csv).unwrap();
    info!("Done!");

    Ok(())
}

async fn process_assets(
    source_storage: &Storage,
) -> Result<HashMap<Pubkey, HashMap<Pubkey, u64>>, String> {
    let zero_pubkey = Pubkey::default();
    let mut results: HashMap<Pubkey, HashMap<Pubkey, u64>> = HashMap::new();

    let chunk_size = 10_000;
    let chunks = source_storage
        .asset_leaf_data
        .iter_start()
        .filter_map(|k| k.ok())
        .filter_map(|(_, bytes)| deserialize::<AssetLeaf>(&bytes).ok())
        .map(|v| (v.pubkey, v.tree_id))
        .chunks(chunk_size);
    let mut cnt = 0;
    for chunk in chunks.into_iter() {
        let chunk: Vec<_> = chunk.collect();
        let pubkeys: Vec<Pubkey> = chunk.iter().map(|(pubkey, _)| *pubkey).collect();

        let collections = source_storage
            .asset_collection_data
            .batch_get(pubkeys)
            .await
            .map_err(|e| e.to_string())?;
        let collection_map: HashMap<Pubkey, Pubkey> = collections
            .into_iter()
            .enumerate()
            .filter_map(|(_, collection_opt)| {
                collection_opt
                    .filter(|collection| collection.is_collection_verified.value)
                    .map(|collection| (collection.pubkey, collection.collection.value))
            })
            .collect();

        for (asset_pubkey, tree_id) in chunk {
            let collection_pubkey = collection_map
                .get(&asset_pubkey)
                .copied()
                .unwrap_or(zero_pubkey);

            let tree_entry = results.entry(tree_id).or_insert_with(HashMap::new);
            let count = tree_entry.entry(collection_pubkey).or_insert(0);
            *count += 1;
        }
        // log the progress every handred's chunk
        cnt += 1;
        if cnt % 100 == 0 {
            info!("Processed {} chunks", cnt);
        }
    }

    Ok(results)
}

fn write_results_to_csv(
    results: HashMap<Pubkey, HashMap<Pubkey, u64>>,
    file_path: &str,
) -> std::io::Result<()> {
    let mut wtr = Writer::from_path(file_path)?;

    wtr.write_record(&["tree", "collection", "number_of_assets"])?;

    for (tree, collections) in results {
        for (collection, count) in collections {
            wtr.write_record(&[tree.to_string(), collection.to_string(), count.to_string()])?;
        }
    }

    wtr.flush()?;
    Ok(())
}
