use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use log::info;
use solana_program::pubkey::Pubkey;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use nft_ingester::config::{setup_config, TreeBackfillerConfig, COMPARER_CONFIG_PREFIX};
use nft_ingester::{config::init_logger, error::IngesterError};
use rocks_db::Storage;

pub const DEFAULT_ROCKSDB_PATH: &str = "./my_rocksdb";

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    restore_rocks_db: bool,
}

#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<(), IngesterError> {
    let config: TreeBackfillerConfig = setup_config(COMPARER_CONFIG_PREFIX);
    init_logger(&config.get_log_level());
    tracing::info!("Starting comparer for target tree key: {}", config.target_tree_key);

    let tasks = JoinSet::new();
    let mutexed_tasks = Arc::new(Mutex::new(tasks));

    let first_rocks = Storage::open_as_secondary(
        &config.source_rocks.clone(),
        "./secondary1",
        mutexed_tasks.clone(),
    )
    .unwrap();

    let second_rocks = Storage::open_as_secondary(
        &config.source_rocks.clone(),
        "./secondary2",
        mutexed_tasks.clone(),
    )
    .unwrap();

    let target_tree_key = Pubkey::from_str(&config.target_tree_key).unwrap();
    compare_cl_items(first_rocks, second_rocks, target_tree_key).await;
    Ok(())
}

fn compare_leaves(first_rocks: Storage, second_rocks: Storage, target_tree_key: Pubkey) {
    let first_map = first_rocks
        .cl_leafs
        .iter_deserialized()
        .filter_map(|v| v.ok())
        .filter(|((_, tree_id), _)| *tree_id == target_tree_key)
        .map(|((node_id, _), value)| (node_id, value))
        .collect::<HashMap<_, _>>();
    tracing::info!(
        "Collected the leaves for the source. Total leaves for the first map: {}",
        first_map.len(),
    );

    let second_map = second_rocks
        .cl_leafs
        .iter_deserialized()
        .filter_map(|v| v.ok())
        .filter(|((_, tree_id), _)| *tree_id == target_tree_key)
        .map(|((node_id, _), value)| (node_id, value))
        .collect::<HashMap<_, _>>();
    tracing::info!(
        "Collected all the leaves. Total leaves for the first map: {}, for the second map: {}",
        first_map.len(),
        second_map.len()
    );

    for (node_id, first_leaf) in first_map.iter() {
        if let Some(second_leaf) = second_map.get(node_id) {
            if first_leaf != second_leaf {
                tracing::info!(
                    "Found a different leaf at node {}: {:?} vs {:?}",
                    node_id,
                    first_leaf,
                    second_leaf
                );
            }
        } else {
            tracing::info!(
                "Found a leaf in the first map that is not in the second map at node {}: {:?}",
                node_id,
                first_leaf
            );
        }
    }

    for (node_id, second_leaf) in second_map.iter() {
        if let Some(first_leaf) = first_map.get(node_id) {
            if first_leaf != second_leaf {
                tracing::info!(
                    "Found a different leaf at node {}: {:?} vs {:?}",
                    node_id,
                    first_leaf,
                    second_leaf
                );
            }
        } else {
            tracing::info!(
                "Found a leaf in the second map that is not in the first map at node {}: {:?}",
                node_id,
                second_leaf
            );
        }
    }

    info!("Finished comparing the leaves");
}

async fn compare_cl_items(first_rocks: Storage, second_rocks: Storage, target_tree_key: Pubkey) {
    let target_key_bytes: [u8; 32] = target_tree_key.to_bytes();

    let first_map_fut = tokio::task::spawn_blocking(move || {
        let first_map = first_rocks
            .cl_items
            .iter_start()
            .filter_map(|k| k.ok())
            .filter_map(|(key, value)| {
                if key[8..] == target_key_bytes {
                    if let Ok((node_id, _)) = ClItem::decode_key(key.to_vec()) {
                        if let Ok(value) = deserialize::<ClItem>(&value) {
                            return Some((node_id, value));
                        }
                    }
                }
                None
            })
            .collect::<HashMap<_, _>>();

        tracing::info!(
            "Collected the items for the source. Total items for the first map: {}",
            first_map.len(),
        );
        first_map
    });

    let second_map_fut = tokio::task::spawn_blocking(move || {
        let second_map = second_rocks
            .cl_items
            .iter_start()
            .filter_map(|k| k.ok())
            .filter_map(|(key, value)| {
                if key[8..] == target_key_bytes {
                    if let Ok((node_id, _)) = ClItem::decode_key(key.to_vec()) {
                        if let Ok(value) = deserialize::<ClItem>(&value) {
                            return Some((node_id, value));
                        }
                    }
                }
                None
            })
            .collect::<HashMap<_, _>>();
        tracing::info!(
            "Collected all the items for the second source. Total items for the for the second map: {}",
            second_map.len()
        );
        second_map
    });

    let first_map = first_map_fut.await.unwrap();
    let second_map = second_map_fut.await.unwrap();
    for (node_id, first_item) in first_map.iter() {
        if let Some(second_item) = second_map.get(node_id) {
            if first_item != second_item {
                tracing::info!(
                    "Found a different item at node {}: {:?} vs {:?}",
                    node_id,
                    first_item,
                    second_item
                );
            }
        } else {
            tracing::info!(
                "Found a item in the first map that is not in the second map at node {}: {:?}",
                node_id,
                first_item
            );
        }
    }

    for (node_id, second_item) in second_map.iter() {
        if let Some(first_item) = first_map.get(node_id) {
            if first_item != second_item {
                tracing::info!(
                    "Found a different item at node {}: {:?} vs {:?}",
                    node_id,
                    first_item,
                    second_item
                );
            }
        } else {
            tracing::info!(
                "Found a item in the second map that is not in the first map at node {}: {:?}",
                node_id,
                second_item
            );
        }
    }

    info!("Finished comparing the items");
}
