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

    let mut delete_batch = Vec::new();

    let mut select_batch_keys = Vec::new();
    let mut select_batch_data = Vec::new();

    for cl_leaf in source_db.cl_leafs.iter_start() {
        match cl_leaf {
            Ok((_, value)) => {
                let cl_leaf_data = deserialize::<ClLeaf>(value.to_vec().as_ref()).unwrap();

                let key = ClItemKey::new(cl_leaf_data.cli_node_idx, cl_leaf_data.cli_tree_key);

                select_batch_keys.push(key);
                select_batch_data.push(cl_leaf_data);

                if select_batch_keys.len() >= 5000 {
                    let cl_item_rows = source_db
                        .cl_items
                        .batch_get(std::mem::take(&mut select_batch_keys))
                        .await
                        .unwrap();

                    for (i, value) in cl_item_rows.iter().enumerate() {
                        if value.is_none() {
                            assets_with_missed_cl_items += 1;

                            let data = select_batch_data.get(i).unwrap();

                            let asset_id =
                                get_asset_id(&data.cli_tree_key, &data.cli_leaf_idx).await;

                            let (asset_dynamic_data, asset_leaf_data) = tokio::join!(
                                source_db.asset_dynamic_data.batch_get(vec![asset_id]),
                                source_db.asset_leaf_data.batch_get(vec![asset_id])
                            );

                            let asset_dynamic_data = asset_dynamic_data
                                .ok()
                                .and_then(|vec| vec.into_iter().next())
                                .and_then(|data_opt| data_opt)
                                .and_then(|data| data.seq.map(|s| s.value))
                                .unwrap_or_default();

                            let asset_leaf_data = asset_leaf_data
                                .ok()
                                .and_then(|vec| vec.into_iter().next())
                                .and_then(|data_opt| data_opt)
                                .and_then(|data| data.leaf_seq)
                                .unwrap_or_default();

                            let max_asset_sequence =
                                std::cmp::max(asset_dynamic_data, asset_leaf_data);

                            // if seq is 0 means asset does not exist at all
                            // found a few such assets during testing, not sure how it happened yet
                            if max_asset_sequence == 0 {
                                continue;
                            }

                            delete_batch.push((data.cli_tree_key, max_asset_sequence));

                            if delete_batch.len() >= BATCH_SIZE {
                                source_db
                                    .tree_seq_idx
                                    .delete_batch(std::mem::take(&mut delete_batch))
                                    .await
                                    .unwrap();
                            }
                        }
                    }

                    select_batch_data.clear();
                }
            }
            Err(e) => {
                println!("Could not start iterator over cl_leafs: {}", e);
            }
        }
    }

    if !delete_batch.is_empty() {
        source_db
            .tree_seq_idx
            .delete_batch(std::mem::take(&mut delete_batch))
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
