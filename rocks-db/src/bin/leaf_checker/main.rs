use std::{env, str::FromStr, sync::Arc, time::Instant};

use bincode::deserialize;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::{
    cl_items::{ClItemKey, ClLeaf},
    migrator::MigrationState,
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use tokio::{sync::Mutex, task::JoinSet};

pub const BATCH_SIZE: usize = 1000;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
    // Retrieve the database paths from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} <source_db_path>", args[0]);
        std::process::exit(1);
    }
    let source_db_path = &args[1];

    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    // Open source and destination databases
    let start = Instant::now();

    println!("Opening DB...");

    let source_db = Storage::open(
        source_db_path,
        Arc::new(Mutex::new(JoinSet::new())),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .unwrap();

    println!("Opened in {:?}", start.elapsed());

    let mut assets_with_missed_cl_items = 0;

    let start = Instant::now();

    println!("Start looking for a missed CL items...");

    let mut batch = Vec::new();

    for cl_leaf in source_db.cl_leafs.iter_start() {
        match cl_leaf {
            Ok((_, value)) => {
                let cl_leaf_data = deserialize::<ClLeaf>(value.to_vec().as_ref()).unwrap();

                let key = ClItemKey::new(cl_leaf_data.cli_node_idx, cl_leaf_data.cli_tree_key);

                let cl_item_row = source_db.cl_items.get(key).unwrap();
                if cl_item_row.is_none() {
                    assets_with_missed_cl_items += 1;

                    let asset_id =
                        get_asset_id(&cl_leaf_data.cli_tree_key, &cl_leaf_data.cli_leaf_idx).await;
                    println!("Found missed CL item for: {}", asset_id);

                    // todo: change to get over complete assets
                    let asset_dynamic_data = source_db
                        .asset_dynamic_data
                        .get(asset_id)
                        .ok()
                        .and_then(|opt| opt)
                        .and_then(|a| a.seq.map(|s| s.value))
                        .unwrap_or_default();

                    let asset_leaf_data = source_db
                        .asset_leaf_data
                        .get(asset_id)
                        .ok()
                        .and_then(|opt| opt)
                        .and_then(|a| a.leaf_seq)
                        .unwrap_or_default();

                    let max_asset_sequence = std::cmp::max(asset_dynamic_data, asset_leaf_data);

                    // if seq is 0 means asset does not exist at all
                    // found a few such assets during testing, not sure how it happened yet
                    if max_asset_sequence == 0 {
                        continue;
                    }

                    batch.push((cl_leaf_data.cli_tree_key, max_asset_sequence));

                    if batch.len() >= BATCH_SIZE {
                        source_db
                            .tree_seq_idx
                            .delete_batch(std::mem::take(&mut batch))
                            .await
                            .unwrap();
                    }
                }
            }
            Err(e) => {
                println!("Could not start iterator over cl_leafs: {}", e);
            }
        }
    }

    if !batch.is_empty() {
        source_db
            .tree_seq_idx
            .delete_batch(std::mem::take(&mut batch))
            .await
            .unwrap();
    }

    println!("Done for {:?}", start.elapsed());
    println!(
        "Found {} assets with missed CL items.",
        assets_with_missed_cl_items
    );
}

async fn get_asset_id(tree_id: &Pubkey, nonce: &u64) -> Pubkey {
    Pubkey::find_program_address(
        &["asset".as_ref(), tree_id.as_ref(), &nonce.to_le_bytes()],
        // TODO: use imported constant instead
        &Pubkey::from_str("BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY").unwrap(),
    )
    .0
}
