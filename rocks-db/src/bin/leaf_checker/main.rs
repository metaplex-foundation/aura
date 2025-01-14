use std::{env, str::FromStr, sync::Arc, time::Instant};

use bincode::deserialize;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::{
    columns::cl_items::{ClItemKey, ClLeaf},
    migrator::MigrationState,
    Storage,
};
use solana_sdk::pubkey::Pubkey;
use tokio::{sync::Mutex, task::JoinSet};

pub const NUM_OF_THREADS: usize = 2500;

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

    let source_db = Arc::new(
        Storage::open(
            source_db_path,
            Arc::new(Mutex::new(JoinSet::new())),
            red_metrics.clone(),
            MigrationState::Last,
        )
        .unwrap(),
    );

    println!("Opened in {:?}", start.elapsed());

    let mut assets_with_missed_cl_items = 0;

    let start = Instant::now();

    println!("Start looking for a missed CL items...");

    let mut delete_batch = Vec::new();

    let mut select_batch = Vec::new();

    for cl_leaf in source_db.cl_leafs.iter_start() {
        match cl_leaf {
            Ok((_, value)) => {
                select_batch.push(value.to_vec());

                if select_batch.len() >= NUM_OF_THREADS {
                    process_batch(source_db.clone(), &mut select_batch, &mut delete_batch).await;

                    if !delete_batch.is_empty() {
                        assets_with_missed_cl_items += delete_batch.len();

                        source_db
                            .tree_seq_idx
                            .delete_batch(std::mem::take(&mut delete_batch))
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

    if !select_batch.is_empty() {
        process_batch(source_db.clone(), &mut select_batch, &mut delete_batch).await;
    }

    if !delete_batch.is_empty() {
        assets_with_missed_cl_items += delete_batch.len();

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

async fn process_batch(
    storage: Arc<Storage>,
    batch_data: &mut Vec<Vec<u8>>,
    delete_batch: &mut Vec<(Pubkey, u64)>,
) {
    let mut tasks = JoinSet::new();

    for data in batch_data.iter() {
        tasks.spawn(process_leaf(storage.clone(), data.clone()));
    }

    while let Some(task_result) = tasks.join_next().await {
        match task_result {
            Ok(key) => {
                if let Some(key) = key {
                    delete_batch.push(key);
                }
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

    batch_data.clear();
}

async fn process_leaf(storage: Arc<Storage>, data: Vec<u8>) -> Option<(Pubkey, u64)> {
    let cl_leaf_data = deserialize::<ClLeaf>(data.as_ref()).unwrap();

    let key = ClItemKey::new(cl_leaf_data.cli_node_idx, cl_leaf_data.cli_tree_key);

    if !storage.cl_items.has_key(key).await.unwrap() {
        let asset_id = get_asset_id(&cl_leaf_data.cli_tree_key, &cl_leaf_data.cli_leaf_idx).await;
        let asset_leaf_data_fut = storage.asset_leaf_data.batch_get(vec![asset_id]);
        let asset_complete_data = storage.get_complete_asset_details(asset_id);
        let asset_leaf_data = asset_leaf_data_fut.await;

        let asset_dynamic_seq = asset_complete_data
            .ok()
            .and_then(|data_opt| data_opt)
            .and_then(|acd| acd.dynamic_details)
            .and_then(|data| data.seq.map(|s| s.value))
            .unwrap_or_default();

        let asset_leaf_seq = asset_leaf_data
            .ok()
            .and_then(|vec| vec.into_iter().next())
            .and_then(|data_opt| data_opt)
            .and_then(|data| data.leaf_seq)
            .unwrap_or_default();

        let max_asset_sequence = std::cmp::max(asset_dynamic_seq, asset_leaf_seq);

        // if seq is 0 means asset does not exist at all
        // found a few such assets during testing, not sure how it happened yet
        if max_asset_sequence == 0 {
            return None;
        }

        return Some((cl_leaf_data.cli_tree_key, max_asset_sequence));
    }

    None
}

async fn get_asset_id(tree_id: &Pubkey, nonce: &u64) -> Pubkey {
    Pubkey::find_program_address(
        &["asset".as_ref(), tree_id.as_ref(), &nonce.to_le_bytes()],
        // TODO: use imported constant instead
        &Pubkey::from_str("BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY").unwrap(),
    )
    .0
}
