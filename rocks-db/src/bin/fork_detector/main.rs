use entities::models::AssetSignature;
use metrics_utils::red::RequestErrorDurationMetrics;
use rocks_db::cl_items::{ClItemKey, ClLeafKey};
use rocks_db::column::TypedColumn;
use rocks_db::migrator::MigrationState;
use rocks_db::{SlotStorage, Storage};
use solana_sdk::pubkey::Pubkey;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const BATCH_TO_DROP: usize = 1000;

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), String> {
    // Retrieve the database paths from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} <source_db_path>", args[0]);
        std::process::exit(1);
    }
    let source_db_path = &args[1];

    println!("Start looking for forks...");

    // Copy specified column families
    if let Err(e) = find_forks(source_db_path).await {
        println!("Failed to find and delete forks: {}.", e);
    } else {
        println!("Forks dropped successfully.");
    }

    Ok(())
}

async fn find_forks(source_path: &str) -> Result<(), String> {
    use std::path::PathBuf;

    let red_metrics = Arc::new(RequestErrorDurationMetrics::new());
    // Open source and destination databases
    let start = Instant::now();

    println!("Opening DB...");

    let js = Arc::new(Mutex::new(JoinSet::new()));
    let source_db = Storage::open(
        source_path,
        js.clone(),
        red_metrics.clone(),
        MigrationState::Last,
    )
    .map_err(|e| e.to_string())?;

    println!("Opened in {:?}", start.elapsed());

    let primary_path: PathBuf = todo!();
    let secondary_path: PathBuf = todo!();
    let slots_db = Arc::new(
        SlotStorage::open_secondary(primary_path, secondary_path, js.clone(), red_metrics.clone())
            .expect("should open slots db"),
    );

    println!("Iterating over column family...");

    let start = Instant::now();
    let all_assets_signatures = source_db.asset_signature.iter_start();

    // vec[(signature, sequence, slot)]
    let mut signatures: Vec<(String, u64, u64)> = Vec::new();
    // (treeId, leaf_idx)
    let mut current_asset = ("".to_string(), 0);

    // vec[(treeId, sequence)]
    let mut sequences_to_delete = Vec::new();

    for sig in all_assets_signatures {
        match sig {
            Ok((k, v)) => {
                let key = AssetSignature::decode_key(k.to_vec()).unwrap();
                let value = bincode::deserialize::<AssetSignature>(v.as_ref()).unwrap();

                let asset_identifier = (key.tree.to_string(), key.leaf_idx);

                // once got new asset check signatures for previous one
                if asset_identifier != current_asset {
                    // more than 2 actions with asset
                    if signatures.len() >= 2 {
                        check_assets_signatures(
                            &source_db,
                            &slots_db,
                            &mut signatures,
                            &mut sequences_to_delete,
                            &current_asset,
                        )
                        .await;
                    }

                    current_asset = asset_identifier;
                    signatures.clear();

                    signatures.push((value.tx, key.seq, value.slot));
                } else {
                    signatures.push((value.tx, key.seq, value.slot));
                }
            }
            Err(e) => {
                println!("Error: {:?}", e.to_string());
            }
        }
    }

    // check last asset signatures
    if signatures.len() >= 2 {
        check_assets_signatures(
            &source_db,
            &slots_db,
            &mut signatures,
            &mut sequences_to_delete,
            &current_asset,
        )
        .await;
    }

    if !sequences_to_delete.is_empty() {
        if let Err(e) = source_db
            .tree_seq_idx
            .delete_batch(sequences_to_delete)
            .await
        {
            println!("Could not drop sequences: {:?}", e.to_string());
        }
    }

    println!(
        "Found forks and deleted sequences for {:?}",
        start.elapsed()
    );

    Ok(())
}

async fn check_assets_signatures(
    source_db: &Storage,
    slots_db: &SlotStorage,
    signatures: &mut Vec<(String, u64, u64)>,
    sequences_to_delete: &mut Vec<(Pubkey, u64)>,
    current_asset: &(String, u64),
) {
    // check only last two because if there was some forked tx
    // somewhere in the middle is doesn't matter such as latest tx brings correct asset's state
    let last_sig = signatures.pop().unwrap();
    let before_last_sig = signatures.pop().unwrap();

    // if fork happened - two same signatures in different blocks
    if last_sig.0 == before_last_sig.0 {
        // take slot with highest seq because we merge data in CL_Items column family by sequence
        // meaning even if there was a fork but we got an update with higher sequence from NOT forked slot
        // everything is fine
        let higher_seq_slot = if last_sig.1 > before_last_sig.1 {
            last_sig.2
        } else {
            before_last_sig.2
        };

        match slots_db.raw_blocks_cbor.has_key(higher_seq_slot).await {
            Ok(has_block) => {
                if !has_block {
                    // only block check is not enough because was found out that during forks
                    // in CLItems may be saved data from not forked block even if sequence was higher in forked block
                    // still not figured out how could it happen
                    match source_db.cl_leafs.get(ClLeafKey::new(
                        current_asset.1,
                        Pubkey::from_str(&current_asset.0).unwrap(),
                    )) {
                        Ok(leaf_data) => {
                            if let Some(leaf) = leaf_data {
                                match source_db
                                    .cl_items
                                    .get(ClItemKey::new(leaf.cli_node_idx, leaf.cli_tree_key))
                                {
                                    Ok(cl_item) => {
                                        if cl_item.is_none() {
                                            let tree_pubkey =
                                                Pubkey::from_str(&current_asset.0).unwrap();

                                            delete_sequence(
                                                source_db,
                                                sequences_to_delete,
                                                tree_pubkey,
                                                last_sig.1,
                                            )
                                            .await;

                                            delete_sequence(
                                                source_db,
                                                sequences_to_delete,
                                                tree_pubkey,
                                                before_last_sig.1,
                                            )
                                            .await;
                                        }
                                    }
                                    Err(e) => {
                                        println!(
                                            "Error during cl_items selecting: {:?}",
                                            e.to_string()
                                        );
                                    }
                                }
                            } else {
                                let tree_pubkey = Pubkey::from_str(&current_asset.0).unwrap();

                                delete_sequence(
                                    source_db,
                                    sequences_to_delete,
                                    tree_pubkey,
                                    last_sig.1,
                                )
                                .await;

                                delete_sequence(
                                    source_db,
                                    sequences_to_delete,
                                    tree_pubkey,
                                    before_last_sig.1,
                                )
                                .await;
                            }
                        }
                        Err(e) => {
                            println!("Error during leaf selecting: {:?}", e.to_string());
                        }
                    }
                }
            }
            Err(e) => {
                println!(
                    "Error during block({:?}) selecting: {:?}",
                    higher_seq_slot,
                    e.to_string()
                );
            }
        }
    }
}

async fn delete_sequence(
    source_db: &Storage,
    sequences_to_delete: &mut Vec<(Pubkey, u64)>,
    tree_pubkey: Pubkey,
    sequence: u64,
) {
    sequences_to_delete.push((tree_pubkey, sequence));

    if sequences_to_delete.len() >= BATCH_TO_DROP {
        // clone vec instead of move to try delete data again if error happened
        if let Err(e) = source_db
            .tree_seq_idx
            .delete_batch(sequences_to_delete.clone())
            .await
        {
            println!("Could not drop sequences: {:?}", e.to_string());
        } else {
            sequences_to_delete.clear();
        }
    }
}
