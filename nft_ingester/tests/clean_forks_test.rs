use bincode::deserialize;
use blockbuster::{instruction::InstructionBundle, programs::bubblegum::BubblegumInstruction};
use entities::models::{RawBlock, SignatureWithSlot};
use metrics_utils::MetricState;
use mpl_bubblegum::{
    types::{BubblegumEventType, LeafSchema, Version},
    InstructionName, LeafSchemaEvent,
};
use nft_ingester::{
    cleaners::fork_cleaner::ForkCleaner,
    processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
};
use rocks_db::{
    column::TypedColumn,
    columns::cl_items::ClItem,
    transaction::{InstructionResult, TransactionResult, TreeUpdate},
    tree_seq::TreeSeqIdx,
};
use setup::rocks::RocksTestEnvironment;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use spl_account_compression::{events::ChangeLogEventV1, state::PathNode};
use tokio::sync::broadcast;

#[cfg(test)]
#[cfg(feature = "integration_tests")]
#[tracing_test::traced_test]
#[tokio::test]
async fn test_clean_forks() {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    };

    use metrics_utils::{utils::start_metrics, MetricsTrait};
    use rocks_db::columns::{cl_items::ClItemKey, leaf_signatures::LeafSignature};
    use solana_transaction_status::UiConfirmedBlock;

    let RocksTestEnvironment { storage, slot_storage, .. } = RocksTestEnvironment::new(&[]);
    let first_tree_key =
        solana_program::pubkey::Pubkey::from_str("5zYdh7eB538fv5Xnjbqg2rZfapY993vwwNYUoP59uz61")
            .unwrap();
    let second_tree_key =
        solana_program::pubkey::Pubkey::from_str("94ZDcq2epe5QqG1egicMXVYmsGkZKBYRmxTH5g4eZxVe")
            .unwrap();

    let leaf_idx_from_first_tree = 5;
    let leaf_idx_from_second_tree = 10;

    storage
        .cl_items
        .put_async(
            ClItemKey::new(100, first_tree_key),
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
            ClItemKey::new(101, first_tree_key),
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
            ClItemKey::new(102, first_tree_key),
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
            ClItemKey::new(103, first_tree_key),
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
            ClItemKey::new(104, first_tree_key),
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
            ClItemKey::new(105, first_tree_key),
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
            ClItemKey::new(106, first_tree_key),
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
            ClItemKey::new(100, second_tree_key),
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
            ClItemKey::new(101, second_tree_key),
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
            ClItemKey::new(104, second_tree_key),
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
            ClItemKey::new(106, second_tree_key),
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

    // save leaf signatures for updates for first tree
    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10000);
    slot_sequence_map.insert(10000, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10001);
    slot_sequence_map.insert(10001, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10002);
    slot_sequence_map.insert(10002, seq_set.clone());
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10003);
    slot_sequence_map.insert(10003, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10004);
    slot_sequence_map.insert(10004, seq_set.clone());
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10005);
    slot_sequence_map.insert(10005, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10006);
    slot_sequence_map.insert(10006, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), first_tree_key, leaf_idx_from_first_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    // save leaf signatures for updates for second tree
    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10000);
    slot_sequence_map.insert(10000, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), second_tree_key, leaf_idx_from_second_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10001);
    slot_sequence_map.insert(10001, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), second_tree_key, leaf_idx_from_second_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10002);
    slot_sequence_map.insert(10004, seq_set.clone());
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), second_tree_key, leaf_idx_from_second_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10003);
    slot_sequence_map.insert(10006, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), second_tree_key, leaf_idx_from_second_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    let mut slot_sequence_map = HashMap::new();
    let mut seq_set = HashSet::new();
    seq_set.insert(10004);
    slot_sequence_map.insert(10007, seq_set);
    storage
        .leaf_signature
        .put_async(
            (Signature::new_unique(), second_tree_key, leaf_idx_from_second_tree),
            LeafSignature { data: slot_sequence_map },
        )
        .await
        .unwrap();

    slot_storage
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
    slot_storage
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
    slot_storage
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
    slot_storage
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
    slot_storage
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
    slot_storage
        .raw_blocks_cbor
        .put_async(
            30000,
            RawBlock {
                slot: 30000,
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
    // intentionally save data only to tree_seq_idx
    // to check if fork cleaner detect it and drop
    storage
        .tree_seq_idx
        .put_async((second_tree_key, 10004), TreeSeqIdx { slot: 10007 })
        .await
        .unwrap();

    let mut metrics_state = MetricState::new();
    metrics_state.register_metrics();
    start_metrics(metrics_state.registry, Some(4444)).await;

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let rx = shutdown_rx.resubscribe();
    let fork_cleaner = ForkCleaner::new(
        storage.clone(),
        slot_storage.clone(),
        metrics_state.fork_cleaner_metrics.clone(),
    );
    fork_cleaner.clean_forks(rx.resubscribe()).await;

    let forked_first_key_first_item =
        storage.cl_items.get(ClItemKey::new(103, first_tree_key)).unwrap();
    let forked_first_key_second_item =
        storage.cl_items.get(ClItemKey::new(104, first_tree_key)).unwrap();
    let forked_first_key_first_seq = storage.tree_seq_idx.get((first_tree_key, 10003)).unwrap();
    let forked_first_key_second_seq = storage.tree_seq_idx.get((first_tree_key, 10004)).unwrap();
    assert_eq!(forked_first_key_first_item, None);
    assert_eq!(forked_first_key_second_item, None);
    assert_eq!(forked_first_key_first_seq, None);
    assert_eq!(forked_first_key_second_seq, None);

    let forked_second_key_first_item =
        storage.cl_items.get(ClItemKey::new(104, second_tree_key)).unwrap();
    let forked_second_key_first_seq = storage.tree_seq_idx.get((second_tree_key, 10002)).unwrap();
    let forked_second_key_fourth_seq = storage.tree_seq_idx.get((second_tree_key, 10004)).unwrap();
    assert_eq!(forked_second_key_first_item, None);
    assert_eq!(forked_second_key_first_seq, None);
    assert_eq!(forked_second_key_fourth_seq, None);

    let non_forked_first_key_item =
        storage.cl_items.get(ClItemKey::new(106, first_tree_key)).unwrap();
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

    let non_forked_second_key_item =
        storage.cl_items.get(ClItemKey::new(106, second_tree_key)).unwrap();
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

#[tracing_test::traced_test]
#[tokio::test]
async fn test_process_forked_transaction() {
    let metrics_state = MetricState::new();
    let RocksTestEnvironment { storage, slot_storage, .. } = RocksTestEnvironment::new(&[]);

    let tree = Pubkey::new_unique();

    let slot_normal_tx = 100;
    let slot_forked_tx = 95;

    let normal_sequence = 1;
    let forked_sequence = 2;

    let owner = Pubkey::new_unique();
    let delegate = Pubkey::new_unique();
    let data_hash = Pubkey::new_unique().to_bytes();
    let creator_hash = Pubkey::new_unique().to_bytes();
    let leaf_hash = Pubkey::new_unique().to_bytes();

    let signature = Signature::new_unique();

    // create two transactions which change data for same asset
    // so there are different nodes' hashes and sequences
    // one transaction is accepted by majority of validator and another one is left in a fork
    // fork cleaner has to detect this situation and clean the DB so sequence consistency cheker could download and parse correct tx
    let normal_tx = BubblegumInstruction {
        instruction: InstructionName::Transfer,
        tree_update: Some(ChangeLogEventV1 {
            id: tree,
            path: vec![
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 32 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 16 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 8 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 4 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 2 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 1 },
            ],
            seq: normal_sequence,
            index: 5,
        }),
        leaf_update: Some(LeafSchemaEvent {
            event_type: BubblegumEventType::LeafSchemaEvent,
            version: Version::V1,
            schema: LeafSchema::V1 { id: tree, owner, delegate, nonce: 5, data_hash, creator_hash },
            leaf_hash,
        }),
        payload: None,
    };

    let forked_tx = BubblegumInstruction {
        instruction: InstructionName::Transfer,
        tree_update: Some(ChangeLogEventV1 {
            id: tree,
            path: vec![
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 32 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 16 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 8 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 4 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 2 },
                PathNode { node: Pubkey::new_unique().to_bytes(), index: 1 },
            ],
            seq: forked_sequence,
            index: 5,
        }),
        leaf_update: Some(LeafSchemaEvent {
            event_type: BubblegumEventType::LeafSchemaEvent,
            version: Version::V1,
            schema: LeafSchema::V1 { id: tree, owner, delegate, nonce: 5, data_hash, creator_hash },
            leaf_hash,
        }),
        payload: None,
    };

    // in this InstructionBundle-s all we need for this test is slot
    // so other data can be arbitrary
    let tx_bundle = InstructionBundle {
        txn_id: &signature.to_string(),
        program: Pubkey::new_unique(),
        instruction: None,
        inner_ix: None,
        keys: &[],
        slot: slot_normal_tx,
    };

    let mut tx_1_resp: InstructionResult =
        BubblegumTxProcessor::get_update_owner_update(&normal_tx, &tx_bundle)
            .map(From::from)
            .unwrap();

    if let Some(cl) = &normal_tx.tree_update {
        tx_1_resp.tree_update = Some(TreeUpdate {
            tree: cl.id,
            seq: cl.seq,
            slot: tx_bundle.slot,
            event: cl.into(),
            instruction: "instruction".to_string(),
            tx: signature.to_string(),
        });
    };

    let tx_bundle = InstructionBundle {
        txn_id: &signature.to_string(),
        program: Pubkey::new_unique(),
        instruction: None,
        inner_ix: None,
        keys: &[],
        slot: slot_forked_tx,
    };

    let mut tx_2_resp: InstructionResult =
        BubblegumTxProcessor::get_update_owner_update(&forked_tx, &tx_bundle)
            .map(From::from)
            .unwrap();

    if let Some(cl) = &forked_tx.tree_update {
        tx_2_resp.tree_update = Some(TreeUpdate {
            tree: cl.id,
            seq: cl.seq,
            slot: tx_bundle.slot,
            event: cl.into(),
            instruction: "instruction".to_string(),
            tx: signature.to_string(),
        });
    };

    let normal_transaction = TransactionResult {
        instruction_results: vec![tx_1_resp],
        transaction_signature: Some((
            mpl_bubblegum::programs::MPL_BUBBLEGUM_ID,
            SignatureWithSlot { signature, slot: slot_normal_tx },
        )),
    };

    storage.store_transaction_result(&normal_transaction, true, true).await.unwrap();

    let forked_transaction = TransactionResult {
        instruction_results: vec![tx_2_resp],
        transaction_signature: Some((
            mpl_bubblegum::programs::MPL_BUBBLEGUM_ID,
            SignatureWithSlot { signature, slot: slot_forked_tx },
        )),
    };

    storage.store_transaction_result(&forked_transaction, true, true).await.unwrap();

    // save only one slot to the raw_blocks_cbor because this is how we detect that fork happened
    // if some slot is not in raw_blocks_cbor - it's forked one
    //
    // for this test all we need is key from Rocks raw_blocks_cbor column family, so RawBlock data could be arbitrary
    slot_storage
        .raw_blocks_cbor
        .put(
            slot_normal_tx,
            RawBlock {
                slot: slot_normal_tx,
                block: solana_transaction_status::UiConfirmedBlock {
                    previous_blockhash: "previousBlockHash".to_string(),
                    blockhash: "blockHash".to_string(),
                    parent_slot: slot_normal_tx,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .unwrap();

    // Required for SLOT_CHECK_OFFSET
    // 16000 is arbitrary number
    slot_storage
        .raw_blocks_cbor
        .put(
            slot_normal_tx + 16000,
            RawBlock {
                slot: slot_normal_tx + 16000,
                block: solana_transaction_status::UiConfirmedBlock {
                    previous_blockhash: "previousBlockHash".to_string(),
                    blockhash: "blockHash".to_string(),
                    parent_slot: slot_normal_tx + 16000,
                    transactions: None,
                    signatures: None,
                    rewards: None,
                    block_time: None,
                    block_height: None,
                },
            },
        )
        .unwrap();

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    let fork_cleaner = ForkCleaner::new(
        storage.clone(),
        slot_storage.clone(),
        metrics_state.fork_cleaner_metrics.clone(),
    );
    fork_cleaner.clean_forks(shutdown_rx.resubscribe()).await;

    for cl_item in storage.cl_items.iter_start() {
        let (_, value) = cl_item.unwrap();

        let value = deserialize::<ClItem>(&value).unwrap();

        // make sure that there should not be either of slots because normal slot is overwritten with forked one such as
        // in forked slot sequence is higher. Merge function for CLItems checks only sequence numbers
        let n = value.slot_updated != slot_normal_tx && value.slot_updated != slot_forked_tx;

        assert!(n);
    }

    for asset in storage.tree_seq_idx.iter_start() {
        let (key, _) = asset.unwrap();

        let (_, seq) = TreeSeqIdx::decode_key(key.to_vec()).unwrap();

        // fork cleaner should drop both sequences
        // usually there saved only one of them but it's better to double parse transactions which were in fork
        let n = seq != forked_sequence && seq != normal_sequence;

        assert!(n);
    }

    let signatures_to_check: Vec<_> = storage.leaf_signature.iter_start().collect();

    // once fork cleaner processed signatures it has to delete that record
    assert_eq!(signatures_to_check.len(), 0);
}
