use entities::models::RawBlock;
use metrics_utils::utils::start_metrics;
use metrics_utils::{MetricState, MetricsTrait};
use nft_ingester::fork_cleaner::ForkCleaner;
use rocks_db::cl_items::ClItem;
use rocks_db::tree_seq::TreeSeqIdx;
use setup::rocks::RocksTestEnvironment;
use solana_transaction_status::UiConfirmedBlock;
use std::str::FromStr;
use tokio::sync::broadcast;

#[cfg(test)]
#[cfg(feature = "integration_tests")]
#[tracing_test::traced_test]
#[tokio::test]
async fn test_clean_forks() {
    let storage = RocksTestEnvironment::new(&[]).storage;
    let first_tree_key =
        solana_program::pubkey::Pubkey::from_str("5zYdh7eB538fv5Xnjbqg2rZfapY993vwwNYUoP59uz61")
            .unwrap();
    let second_tree_key =
        solana_program::pubkey::Pubkey::from_str("94ZDcq2epe5QqG1egicMXVYmsGkZKBYRmxTH5g4eZxVe")
            .unwrap();

    storage
        .cl_items
        .put_async(
            (100, first_tree_key),
            ClItem {
                cli_node_idx: 100,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10000,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10000,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (101, first_tree_key),
            ClItem {
                cli_node_idx: 101,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10001,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10001,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (102, first_tree_key),
            ClItem {
                cli_node_idx: 102,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10002,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10002,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (103, first_tree_key),
            ClItem {
                cli_node_idx: 103,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10003,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10003,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (104, first_tree_key),
            ClItem {
                cli_node_idx: 104,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10004,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10004,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (105, first_tree_key),
            ClItem {
                cli_node_idx: 105,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10005,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10005,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (106, first_tree_key),
            ClItem {
                cli_node_idx: 106,
                cli_tree_key: first_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10006,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10006,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (100, second_tree_key),
            ClItem {
                cli_node_idx: 100,
                cli_tree_key: second_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10000,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10000,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (101, second_tree_key),
            ClItem {
                cli_node_idx: 101,
                cli_tree_key: second_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10001,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10001,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (104, second_tree_key),
            ClItem {
                cli_node_idx: 104,
                cli_tree_key: second_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10002,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10004,
            },
        )
        .await
        .unwrap();
    storage
        .cl_items
        .put_async(
            (106, second_tree_key),
            ClItem {
                cli_node_idx: 106,
                cli_tree_key: second_tree_key,
                cli_leaf_idx: None,
                cli_seq: 10003,
                cli_level: 1,
                cli_hash: Vec::new(),
                slot_updated: 10006,
            },
        )
        .await
        .unwrap();

    storage
        .raw_blocks_cbor
        .put_async(
            10000,
            RawBlock {
                slot: 10000,
                block: UiConfirmedBlock {
                    previous_blockhash: "".to_string(),
                    blockhash: "".to_string(),
                    parent_slot: 0,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .await
        .unwrap();
    storage
        .raw_blocks_cbor
        .put_async(
            10001,
            RawBlock {
                slot: 10001,
                block: UiConfirmedBlock {
                    previous_blockhash: "".to_string(),
                    blockhash: "".to_string(),
                    parent_slot: 0,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .await
        .unwrap();
    storage
        .raw_blocks_cbor
        .put_async(
            10002,
            RawBlock {
                slot: 10002,
                block: UiConfirmedBlock {
                    previous_blockhash: "".to_string(),
                    blockhash: "".to_string(),
                    parent_slot: 0,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .await
        .unwrap();
    storage
        .raw_blocks_cbor
        .put_async(
            10005,
            RawBlock {
                slot: 10000,
                block: UiConfirmedBlock {
                    previous_blockhash: "".to_string(),
                    blockhash: "".to_string(),
                    parent_slot: 0,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .await
        .unwrap();
    storage
        .raw_blocks_cbor
        .put_async(
            10006,
            RawBlock {
                slot: 10000,
                block: UiConfirmedBlock {
                    previous_blockhash: "".to_string(),
                    blockhash: "".to_string(),
                    parent_slot: 0,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .await
        .unwrap();
    // Need for SLOT_CHECK_OFFSET
    storage
        .raw_blocks_cbor
        .put_async(
            20005,
            RawBlock {
                slot: 20000,
                block: UiConfirmedBlock {
                    previous_blockhash: "".to_string(),
                    blockhash: "".to_string(),
                    parent_slot: 0,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .await
        .unwrap();

    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10000), TreeSeqIdx { slot: 10000 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10001), TreeSeqIdx { slot: 10001 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10002), TreeSeqIdx { slot: 10002 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10003), TreeSeqIdx { slot: 10003 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10004), TreeSeqIdx { slot: 10004 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10005), TreeSeqIdx { slot: 10005 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((first_tree_key, 10006), TreeSeqIdx { slot: 10006 })
        .await
        .unwrap();

    storage
        .tree_seq_idx
        .put_async((second_tree_key, 10000), TreeSeqIdx { slot: 10000 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((second_tree_key, 10001), TreeSeqIdx { slot: 10001 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((second_tree_key, 10002), TreeSeqIdx { slot: 10004 })
        .await
        .unwrap();
    storage
        .tree_seq_idx
        .put_async((second_tree_key, 10003), TreeSeqIdx { slot: 10006 })
        .await
        .unwrap();

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();
    start_metrics(metrics_state.registry, Some(4444)).await;

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let rx = shutdown_rx.resubscribe();
    let fork_cleaner = ForkCleaner::new(
        storage.clone(),
        storage.clone(),
        metrics_state.fork_cleaner_metrics.clone(),
    );
    fork_cleaner.clean_forks(rx.resubscribe()).await;

    let forked_first_key_first_item = storage.cl_items.get((103, first_tree_key)).unwrap();
    let forked_first_key_second_item = storage.cl_items.get((104, first_tree_key)).unwrap();
    let forked_first_key_first_seq = storage.tree_seq_idx.get((first_tree_key, 10003)).unwrap();
    let forked_first_key_second_seq = storage.tree_seq_idx.get((first_tree_key, 10004)).unwrap();
    assert_eq!(forked_first_key_first_item, None);
    assert_eq!(forked_first_key_second_item, None);
    assert_eq!(forked_first_key_first_seq, None);
    assert_eq!(forked_first_key_second_seq, None);

    let forked_second_key_first_item = storage.cl_items.get((104, second_tree_key)).unwrap();
    let forked_second_key_first_seq = storage.tree_seq_idx.get((second_tree_key, 10002)).unwrap();
    assert_eq!(forked_second_key_first_item, None);
    assert_eq!(forked_second_key_first_seq, None);

    let non_forked_first_key_item = storage.cl_items.get((106, first_tree_key)).unwrap();
    let non_forked_first_key_seq = storage.tree_seq_idx.get((first_tree_key, 10006)).unwrap();
    assert_eq!(
        non_forked_first_key_item,
        Some(ClItem {
            cli_node_idx: 106,
            cli_tree_key: first_tree_key,
            cli_leaf_idx: None,
            cli_seq: 10006,
            cli_level: 1,
            cli_hash: Vec::new(),
            slot_updated: 10006,
        })
    );
    assert_eq!(non_forked_first_key_seq, Some(TreeSeqIdx { slot: 10006 }));

    let non_forked_second_key_item = storage.cl_items.get((106, second_tree_key)).unwrap();
    let non_forked_second_key_seq = storage.tree_seq_idx.get((second_tree_key, 10003)).unwrap();
    assert_eq!(
        non_forked_second_key_item,
        Some(ClItem {
            cli_node_idx: 106,
            cli_tree_key: second_tree_key,
            cli_leaf_idx: None,
            cli_seq: 10003,
            cli_level: 1,
            cli_hash: Vec::new(),
            slot_updated: 10006,
        })
    );
    assert_eq!(non_forked_second_key_seq, Some(TreeSeqIdx { slot: 10006 }));
}
