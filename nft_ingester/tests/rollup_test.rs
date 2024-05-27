use std::collections::HashMap;
#[allow(unused_imports)]
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use anchor_lang::prelude::*;
use async_trait::async_trait;
use mpl_bubblegum::types::{LeafSchema, MetadataArgs};

use entities::enums::RollupState;
use entities::models::RollupWithState;
use entities::rollup::{
    BatchMintInstruction, ChangeLogEventV1, PathNode, RolledMintInstruction, Rollup,
};
use interface::error::UsecaseError;
use interface::rollup::RollupDownloader;
use metrics_utils::RollupProcessorMetricsConfig;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::error::{IngesterError, RollupValidationError};
use nft_ingester::rollup_processor::{MockPermanentStorageClient, RollupProcessor};
use postgre_client::PgClient;
use rand::{thread_rng, Rng};
use serde_json::json;
use solana_sdk::keccak;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_account_compression::ConcurrentMerkleTree;
use tempfile::TempDir;
use testcontainers::clients::Cli;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use uuid::Uuid;

fn generate_rollup(size: usize) -> Rollup {
    let authority = Pubkey::from_str("3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM").unwrap();
    let tree = Pubkey::from_str("HxhCw9g3kZvrdg9zZvctmh6qpSDg1FfsBXfFvRkbCHB7").unwrap();
    let mut mints = Vec::new();
    let mut merkle = ConcurrentMerkleTree::<10, 32>::new();
    merkle.initialize().unwrap();

    let mut last_leaf_hash = [0u8; 32];
    for i in 0..size {
        let mint_args = MetadataArgs {
            name: thread_rng()
                .sample_iter(rand::distributions::Alphanumeric)
                .take(15)
                .map(char::from)
                .collect(),
            symbol: thread_rng()
                .sample_iter(rand::distributions::Alphanumeric)
                .take(5)
                .map(char::from)
                .collect(),
            uri: format!(
                "https://arweave.net/{}",
                thread_rng()
                    .sample_iter(rand::distributions::Alphanumeric)
                    .take(43)
                    .map(char::from)
                    .collect::<String>()
            ),
            seller_fee_basis_points: thread_rng()
                .sample(rand::distributions::Uniform::new(0, 10000)),
            primary_sale_happened: thread_rng().gen_bool(0.5),
            is_mutable: thread_rng().gen_bool(0.5),
            edition_nonce: if thread_rng().gen_bool(0.5) {
                None
            } else {
                Some(thread_rng().sample(rand::distributions::Uniform::new(0, 255)))
            },
            token_standard: if thread_rng().gen_bool(0.5) {
                None
            } else {
                Some(mpl_bubblegum::types::TokenStandard::NonFungible)
            },
            collection: if thread_rng().gen_bool(0.5) {
                None
            } else {
                Some(mpl_bubblegum::types::Collection {
                    verified: false,
                    key: Pubkey::new_unique(),
                })
            },
            uses: None, // todo
            token_program_version: mpl_bubblegum::types::TokenProgramVersion::Original,
            creators: (0..thread_rng().sample(rand::distributions::Uniform::new(1, 5)))
                .map(|_| mpl_bubblegum::types::Creator {
                    address: Pubkey::new_unique(),
                    verified: false,
                    share: thread_rng().sample(rand::distributions::Uniform::new(0, 100)),
                })
                .collect(),
        };
        let nonce = i as u64;
        let id = mpl_bubblegum::utils::get_asset_id(&tree, nonce);
        let owner = authority.clone();
        let delegate = authority.clone();

        let metadata_args_hash = keccak::hashv(&[mint_args.try_to_vec().unwrap().as_slice()]);
        let data_hash = keccak::hashv(&[
            &metadata_args_hash.to_bytes(),
            &mint_args.seller_fee_basis_points.to_le_bytes(),
        ]);
        let creator_data = mint_args
            .creators
            .iter()
            .map(|c| [c.address.as_ref(), &[c.verified as u8], &[c.share]].concat())
            .collect::<Vec<_>>();
        let creator_hash = keccak::hashv(
            creator_data
                .iter()
                .map(|c| c.as_slice())
                .collect::<Vec<&[u8]>>()
                .as_ref(),
        );

        let hashed_leaf = keccak::hashv(&[
            &[1], //self.version().to_bytes()
            id.as_ref(),
            owner.as_ref(),
            delegate.as_ref(),
            nonce.to_le_bytes().as_ref(),
            data_hash.as_ref(),
            creator_hash.as_ref(),
        ])
        .to_bytes();
        merkle.append(hashed_leaf).unwrap();
        last_leaf_hash = hashed_leaf;
        let changelog = merkle.change_logs[merkle.active_index as usize];
        let path_len = changelog.path.len() as u32;
        let mut path: Vec<spl_account_compression::state::PathNode> = changelog
            .path
            .iter()
            .enumerate()
            .map(|(lvl, n)| {
                spl_account_compression::state::PathNode::new(
                    *n,
                    (1 << (path_len - lvl as u32)) + (changelog.index >> lvl),
                )
            })
            .collect();
        path.push(spl_account_compression::state::PathNode::new(
            changelog.root,
            1,
        ));

        let rolled_mint = RolledMintInstruction {
            tree_update: ChangeLogEventV1 {
                id: tree,
                path: path.into_iter().map(Into::into).collect::<Vec<_>>(),
                seq: merkle.sequence_number,
                index: changelog.index,
            },
            leaf_update: LeafSchema::V1 {
                id,
                owner,
                delegate,
                nonce,
                data_hash: data_hash.to_bytes(),
                creator_hash: creator_hash.to_bytes(),
            },
            mint_args,
            authority,
        };
        mints.push(rolled_mint);
    }
    let rollup = Rollup {
        tree_id: tree,
        raw_metadata_map: HashMap::new(),
        max_depth: 10,
        rolled_mints: mints,
        merkle_root: merkle.get_root(),
        last_leaf_hash,
        max_buffer_size: 32,
    };

    rollup
}

#[test]
#[cfg(feature = "rollup_tests")]
fn test_generate_1_000_rollup() {
    let rollup = generate_rollup(1000);
    assert_eq!(rollup.rolled_mints.len(), 1000);
    let file = File::create("rollup-1000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
#[cfg(feature = "rollup_tests")]
fn test_generate_10_000_rollup() {
    let rollup = generate_rollup(10_000);
    assert_eq!(rollup.rolled_mints.len(), 10_000);
    println!("rollup of {:?} assets created", rollup.rolled_mints.len());
    let file = File::create("rollup-10_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
#[cfg(feature = "rollup_tests")]
fn test_generate_100_000_rollup() {
    let rollup = generate_rollup(100_000);
    assert_eq!(rollup.rolled_mints.len(), 100_000);
    println!("rollup of {:?} assets created", rollup.rolled_mints.len());
    let file = File::create("rollup-100_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
#[cfg(feature = "rollup_tests")]
fn test_generate_1_000_000_rollup() {
    let rollup = generate_rollup(1_000_000);
    assert_eq!(rollup.rolled_mints.len(), 1_000_000);
    let file = File::create("rollup-1_000_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
#[cfg(feature = "rollup_tests")]
fn test_generate_10_000_000_rollup() {
    let rollup = generate_rollup(10_000_000);
    assert_eq!(rollup.rolled_mints.len(), 10_000_000);
    let file = File::create("rollup-10_000_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

const ROLLUP_ASSETS_TO_SAVE: usize = 1_000;
struct TestRollupCreator;
#[async_trait]
impl RollupDownloader for TestRollupCreator {
    async fn download_rollup(&self, _url: &str) -> std::result::Result<Box<Rollup>, UsecaseError> {
        // let json_file = std::fs::read_to_string("../rollup-1000.json").unwrap();
        // let rollup: Rollup = serde_json::from_str(&json_file).unwrap();

        Ok(Box::new(generate_rollup(ROLLUP_ASSETS_TO_SAVE)))
    }

    async fn download_rollup_and_check_checksum(
        &self,
        _url: &str,
        _checksum: &str,
    ) -> std::result::Result<Box<Rollup>, UsecaseError> {
        Ok(Box::new(generate_rollup(ROLLUP_ASSETS_TO_SAVE)))
    }
}

fn _generate() -> ConcurrentMerkleTree<10, 32> {
    let json_file = std::fs::read_to_string("../rollup-1000.json").unwrap();
    let rollup: Rollup = serde_json::from_str(&json_file).unwrap();

    let mut merkle_tree = ConcurrentMerkleTree::<10, 32>::new();
    merkle_tree.initialize().unwrap();

    for (nonce, asset) in rollup.rolled_mints.iter().enumerate() {
        let metadata_args_hash = keccak::hashv(&[asset.mint_args.try_to_vec().unwrap().as_slice()]);
        let data_hash = keccak::hashv(&[
            &metadata_args_hash.to_bytes(),
            &asset.mint_args.seller_fee_basis_points.to_le_bytes(),
        ]);

        let creator_data = asset
            .mint_args
            .creators
            .iter()
            .map(|c| [c.address.as_ref(), &[c.verified as u8], &[c.share]].concat())
            .collect::<Vec<_>>();

        let creator_hash = keccak::hashv(
            creator_data
                .iter()
                .map(|c| c.as_slice())
                .collect::<Vec<&[u8]>>()
                .as_ref(),
        );

        let id = mpl_bubblegum::utils::get_asset_id(&rollup.tree_id, nonce as u64);

        let leaf = LeafSchema::V1 {
            id,
            owner: Pubkey::from_str("3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM").unwrap(),
            delegate: Pubkey::from_str("3VvLDXqJbw3heyRwFxv8MmurPznmDVUJS9gPMX2BDqfM").unwrap(),
            nonce: nonce as u64,
            data_hash: data_hash.to_bytes(),
            creator_hash: creator_hash.to_bytes(),
        };
        merkle_tree.append(leaf.hash()).unwrap();
    }
    merkle_tree
}

#[tokio::test]
async fn store_rollup_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
    BubblegumTxProcessor::store_rollup_update(
        100,
        &BatchMintInstruction {
            max_depth: 10,
            max_buffer_size: 32,
            num_minted: 0,
            root: [0u8; 32],
            leaf: [0u8; 32],
            index: 0,
            metadata_url: "ff".to_string(),
            file_checksum: "ff".to_string(),
        },
        TestRollupCreator {},
        env.rocks_env.storage.clone(),
        Signature::default(),
    )
    .await
    .unwrap();

    let static_iter = env.rocks_env.storage.asset_static_data.iter_start();
    assert_eq!(static_iter.count(), ROLLUP_ASSETS_TO_SAVE);
}

#[tokio::test]
async fn xxhash_test() {
    let file_data = vec![43, 2, 5, 4, 76, 34, 123, 42, 73, 81, 47];

    let file_hash = xxhash_rust::xxh3::xxh3_128(&file_data);

    let hash_hex = hex::encode(file_hash.to_be_bytes());

    assert_eq!(&hash_hex, "4f299160368d57dccbb6deec075d5083");
}

async fn save_temp_rollup(dir: &TempDir, client: Arc<PgClient>, rollup: &Rollup) -> String {
    let file_name = format!("{}.json", Uuid::new_v4());
    let full_file_path = format!("{}/{}", dir.path().to_str().unwrap(), &file_name);
    let mut file = tokio::fs::File::create(full_file_path).await.unwrap();
    file.write_all(json!(rollup).to_string().as_bytes())
        .await
        .unwrap();
    client.insert_new_rollup(&file_name).await.unwrap();
    file_name
}

#[tokio::test]
async fn rollup_processing_validation_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
    let mut rollup = generate_rollup(1000);
    let mut permanent_storage_client = MockPermanentStorageClient::new();
    permanent_storage_client
        .expect_upload_file()
        .returning(|_, _| Box::pin(async { Ok(("tx_id".to_string(), 100u64)) }));
    permanent_storage_client
        .expect_get_metadata_url()
        .returning(|_| "".to_string());
    let dir = TempDir::new().unwrap();
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let rollup_processor = RollupProcessor::new(
        env.pg_env.client.clone(),
        Arc::new(nft_ingester::rollup_processor::NoopRollupTxSender {}),
        Arc::new(permanent_storage_client),
        dir.path().to_str().unwrap().to_string(),
        Arc::new(RollupProcessorMetricsConfig::new()),
    );
    let (_, shutdown_rx) = broadcast::channel::<()>(1);
    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(processing_result, Ok(()));

    let old_root = rollup.merkle_root;
    let new_root = Pubkey::new_unique();
    rollup.merkle_root = new_root.to_bytes();
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::InvalidRoot(
                Pubkey::from(old_root).to_string(),
                new_root.to_string()
            )
        ))
    );

    rollup.merkle_root = old_root;
    let leaf_idx = 111;
    let old_leaf_data_hash = rollup.rolled_mints[leaf_idx].leaf_update.data_hash();
    let new_leaf_data_hash = Pubkey::new_unique();
    rollup.rolled_mints[leaf_idx].leaf_update = LeafSchema::V1 {
        id: rollup.rolled_mints[leaf_idx].leaf_update.id(),
        owner: rollup.rolled_mints[leaf_idx].leaf_update.owner(),
        delegate: rollup.rolled_mints[leaf_idx].leaf_update.delegate(),
        nonce: rollup.rolled_mints[leaf_idx].leaf_update.nonce(),
        data_hash: new_leaf_data_hash.to_bytes(),
        creator_hash: rollup.rolled_mints[leaf_idx].leaf_update.creator_hash(),
    };
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::InvalidDataHash(
                Pubkey::from(old_leaf_data_hash).to_string(),
                new_leaf_data_hash.to_string()
            )
        ))
    );

    rollup.rolled_mints[leaf_idx].leaf_update = LeafSchema::V1 {
        id: rollup.rolled_mints[leaf_idx].leaf_update.id(),
        owner: rollup.rolled_mints[leaf_idx].leaf_update.owner(),
        delegate: rollup.rolled_mints[leaf_idx].leaf_update.delegate(),
        nonce: rollup.rolled_mints[leaf_idx].leaf_update.nonce(),
        data_hash: old_leaf_data_hash,
        creator_hash: rollup.rolled_mints[leaf_idx].leaf_update.creator_hash(),
    };
    let old_tree_depth = rollup.max_depth;
    let new_tree_depth = 100;
    rollup.max_depth = new_tree_depth;
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::CannotCreateMerkleTree(new_tree_depth, rollup.max_buffer_size)
        ))
    );

    rollup.max_depth = old_tree_depth;
    let new_asset_id = Pubkey::new_unique();
    let old_asset_id = rollup.rolled_mints[leaf_idx].leaf_update.id();
    rollup.rolled_mints[leaf_idx].leaf_update = LeafSchema::V1 {
        id: new_asset_id,
        owner: rollup.rolled_mints[leaf_idx].leaf_update.owner(),
        delegate: rollup.rolled_mints[leaf_idx].leaf_update.delegate(),
        nonce: rollup.rolled_mints[leaf_idx].leaf_update.nonce(),
        data_hash: rollup.rolled_mints[leaf_idx].leaf_update.data_hash(),
        creator_hash: rollup.rolled_mints[leaf_idx].leaf_update.creator_hash(),
    };
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::PDACheckFail(old_asset_id.to_string(), new_asset_id.to_string())
        ))
    );

    rollup.rolled_mints[leaf_idx].leaf_update = LeafSchema::V1 {
        id: old_asset_id,
        owner: rollup.rolled_mints[leaf_idx].leaf_update.owner(),
        delegate: rollup.rolled_mints[leaf_idx].leaf_update.delegate(),
        nonce: rollup.rolled_mints[leaf_idx].leaf_update.nonce(),
        data_hash: rollup.rolled_mints[leaf_idx].leaf_update.data_hash(),
        creator_hash: rollup.rolled_mints[leaf_idx].leaf_update.creator_hash(),
    };
    let old_path = rollup.rolled_mints[leaf_idx]
        .tree_update
        .path
        .iter()
        .map(|path| PathNode {
            node: path.node,
            index: path.index,
        })
        .collect::<Vec<_>>();
    let new_path = Vec::new();
    rollup.rolled_mints[leaf_idx].tree_update.path = new_path;
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::WrongAssetPath(
                rollup.rolled_mints[leaf_idx].leaf_update.id().to_string()
            )
        ))
    );

    rollup.rolled_mints[leaf_idx].tree_update.path = old_path;
    let old_tree_id = rollup.rolled_mints[leaf_idx].tree_update.id;
    let new_tree_id = Pubkey::new_unique();
    rollup.rolled_mints[leaf_idx].tree_update.id = new_tree_id;
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::WrongTreeIdForChangeLog(
                rollup.rolled_mints[leaf_idx].leaf_update.id().to_string(),
                old_tree_id.to_string(),
                new_tree_id.to_string()
            )
        ))
    );

    rollup.rolled_mints[leaf_idx].tree_update.id = old_tree_id;
    let old_index = rollup.rolled_mints[leaf_idx].tree_update.index;
    let new_index = 1;
    rollup.rolled_mints[leaf_idx].tree_update.index = new_index;
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;

    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    assert_eq!(
        processing_result,
        Err(IngesterError::RollupValidation(
            RollupValidationError::WrongChangeLogIndex(
                rollup.rolled_mints[leaf_idx].leaf_update.id().to_string(),
                old_index,
                new_index
            )
        ))
    );
}

#[tokio::test]
async fn rollup_upload_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
    let rollup = generate_rollup(1000);
    let mut permanent_storage_client = MockPermanentStorageClient::new();
    permanent_storage_client
        .expect_upload_file()
        .times(nft_ingester::rollup_processor::MAX_ROLLUP_RETRIES - 1)
        .returning(|_, _| {
            Box::pin(async { Err(IngesterError::Arweave("test error".to_string())) })
        });
    permanent_storage_client
        .expect_upload_file()
        .times(1)
        .returning(|_, _| Box::pin(async { Ok(("tx_id".to_string(), 100u64)) }));
    permanent_storage_client
        .expect_get_metadata_url()
        .returning(|_| "".to_string());
    let dir = TempDir::new().unwrap();
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;
    let rollup_processor = RollupProcessor::new(
        env.pg_env.client.clone(),
        Arc::new(nft_ingester::rollup_processor::NoopRollupTxSender {}),
        Arc::new(permanent_storage_client),
        dir.path().to_str().unwrap().to_string(),
        Arc::new(RollupProcessorMetricsConfig::new()),
    );

    let (_, shutdown_rx) = broadcast::channel::<()>(1);
    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    // Retries covered previous errors
    assert_eq!(processing_result, Ok(()));
    let mut permanent_storage_client = MockPermanentStorageClient::new();
    permanent_storage_client
        .expect_upload_file()
        .times(nft_ingester::rollup_processor::MAX_ROLLUP_RETRIES)
        .returning(|_, _| {
            Box::pin(async { Err(IngesterError::Arweave("test error".to_string())) })
        });
    permanent_storage_client
        .expect_get_metadata_url()
        .returning(|_| "".to_string());
    let rollup_processor = RollupProcessor::new(
        env.pg_env.client.clone(),
        Arc::new(nft_ingester::rollup_processor::NoopRollupTxSender {}),
        Arc::new(permanent_storage_client),
        dir.path().to_str().unwrap().to_string(),
        Arc::new(RollupProcessorMetricsConfig::new()),
    );
    let file_name = save_temp_rollup(&dir, env.pg_env.client.clone(), &rollup).await;
    let processing_result = rollup_processor
        .process_rollup(
            shutdown_rx.resubscribe(),
            RollupWithState {
                file_name,
                state: RollupState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    // Retries are exhausted
    assert_eq!(
        processing_result,
        Err(IngesterError::Arweave("Arweave: test error".to_string()))
    );
}
