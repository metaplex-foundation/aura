use std::collections::HashMap;
#[allow(unused_imports)]
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use anchor_lang::prelude::*;
use async_trait::async_trait;
use mpl_bubblegum::types::{LeafSchema, MetadataArgs};

use digital_asset_types::rpc::AssetProof;
use entities::api_req_params::GetAssetProof;
use entities::enums::{FailedRollupState, PersistingRollupState, RollupState};
use entities::models::{BufferedTransaction, FailedRollupKey};
use entities::models::{RollupToVerify, RollupWithState};
use entities::rollup::{ChangeLogEventV1, PathNode, RolledMintInstruction, Rollup};
use flatbuffers::FlatBufferBuilder;
use interface::error::UsecaseError;
use interface::rollup::MockRollupDownloader;
use interface::rollup::RollupDownloader;
use metrics_utils::ApiMetricsConfig;
use metrics_utils::IngesterMetricsConfig;
use metrics_utils::RollupPersisterMetricsConfig;
use metrics_utils::RollupProcessorMetricsConfig;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::config::JsonMiddlewareConfig;
use nft_ingester::error::IngesterError;
use nft_ingester::json_worker::JsonWorker;
use nft_ingester::rollup::rollup_persister::{RollupPersister, MAX_ROLLUP_DOWNLOAD_ATTEMPTS};
use nft_ingester::rollup::rollup_processor::{MockPermanentStorageClient, RollupProcessor};
use plerkle_serialization::serializer::serialize_transaction;
use postgre_client::PgClient;
use rand::{thread_rng, Rng};
use serde_json::json;
use solana_program::instruction::CompiledInstruction;
use solana_program::message::Message;
use solana_program::message::MessageHeader;
use solana_sdk::keccak;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::SanitizedTransaction;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::TransactionStatusMeta;
use solana_transaction_status::{InnerInstruction, InnerInstructions};
use spl_account_compression::ConcurrentMerkleTree;
use std::collections::VecDeque;
use tempfile::TempDir;
use testcontainers::clients::Cli;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use usecase::error::RollupValidationError;
use usecase::proofs::MaybeProofChecker;
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

fn generate_merkle_tree_from_rollup(rollup: &Rollup) -> ConcurrentMerkleTree<10, 32> {
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
async fn save_rollup_to_queue_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let tasks = Arc::new(Mutex::new(VecDeque::new()));

    let bubblegum_updates_processor = BubblegumTxProcessor::new(
        env.rocks_env.storage.clone(),
        Arc::new(IngesterMetricsConfig::new()),
        tasks.clone(),
    );

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();

    // arbitrary data
    let rollup_instruction_data =
        mpl_bubblegum::instructions::FinalizeTreeWithRootInstructionArgs {
            rightmost_root: [1; 32],
            rightmost_leaf: [1; 32],
            rightmost_index: 99,
            metadata_url: metadata_url.clone(),
            metadata_hash: metadata_hash.clone(),
        };

    // took it from Bubblegum client
    // this value is generated by Anchor library, it's instruction identifier
    let mut instruction_data = vec![101, 214, 253, 135, 176, 170, 11, 235];
    instruction_data.extend(rollup_instruction_data.try_to_vec().unwrap().iter());

    let transaction = SanitizedTransaction::from_transaction_for_tests(Transaction {
        signatures: vec![Signature::new_unique()],
        message: Message {
            header: MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![
                Pubkey::new_unique(),
                Pubkey::from_str("BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY").unwrap(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
            ],
            recent_blockhash: [1; 32].into(),
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![2, 3],
                data: instruction_data,
            }],
        },
    });

    // inner instruction is useless here but required by transaction parser
    let transaction_status_meta = TransactionStatusMeta {
        inner_instructions: Some(vec![InnerInstructions {
            index: 0,
            instructions: vec![InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![],
                    data: vec![],
                },
                stack_height: None,
            }],
        }]),
        ..Default::default()
    };

    let transaction_info =
        plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaTransactionInfoV2 {
            signature: &Signature::new_unique(),
            is_vote: false,
            transaction: &transaction,
            transaction_status_meta: &transaction_status_meta,
            index: 0,
        };
    let builder = FlatBufferBuilder::new();
    let builder = serialize_transaction(builder, &transaction_info, 10);

    let buffered_transaction = BufferedTransaction {
        transaction: builder.finished_data().to_vec(),
        map_flatbuffer: false,
    };

    bubblegum_updates_processor
        .process_transaction(buffered_transaction)
        .await
        .unwrap();

    let r = env
        .rocks_env
        .storage
        .rollup_to_verify
        .get(metadata_hash.clone())
        .unwrap()
        .unwrap();

    assert_eq!(r.file_hash, metadata_hash);
    assert_eq!(r.url, metadata_url);
}

#[tokio::test]
async fn rollup_persister_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let test_rollup = generate_rollup(10);

    let tmp_dir = tempfile::TempDir::new().unwrap();

    let tmp_file = File::create(tmp_dir.path().join("rollup-10.json")).unwrap();
    serde_json::to_writer(tmp_file, &test_rollup).unwrap();

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let rollup_to_verify = RollupToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts: 0,
        persisting_state: PersistingRollupState::ReceivedTransaction,
    };

    env.rocks_env
        .storage
        .rollup_to_verify
        .put(metadata_hash.clone(), rollup_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockRollupDownloader::new();
    mocked_downloader
        .expect_download_rollup_and_check_checksum()
        .returning(move |_, _| {
            let json_file = std::fs::read_to_string(tmp_dir.path().join("rollup-10.json")).unwrap();
            Ok(Box::new(serde_json::from_str(&json_file).unwrap()))
        });

    let rollup_persister = RollupPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(RollupPersisterMetricsConfig::new()),
    );

    let (rollup_to_verify, _) = env
        .rocks_env
        .storage
        .fetch_rollup_for_verifying()
        .await
        .unwrap();

    let (_, rx) = broadcast::channel::<()>(1);
    rollup_persister
        .persist_rollup(&rx, rollup_to_verify.unwrap(), None)
        .await
        .unwrap();

    let merkle_tree = generate_merkle_tree_from_rollup(&test_rollup);

    let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker, JsonWorker, JsonWorker>::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(ApiMetricsConfig::new()),
        None,
        50,
        None,
        None,
        JsonMiddlewareConfig::default(),
    );

    let leaf_index = 4u32;

    let payload = GetAssetProof {
        id: test_rollup
            .rolled_mints
            .get(leaf_index as usize)
            .unwrap()
            .leaf_update
            .id()
            .to_string(),
    };
    let proof_result = api.get_asset_proof(payload).await.unwrap();
    let asset_proof: AssetProof = serde_json::from_value(proof_result).unwrap();

    let mut proofs: [[u8; 32]; 10] = [[0; 32]; 10];

    for (i, s) in asset_proof.proof.iter().enumerate() {
        proofs[i] = Pubkey::from_str(s).unwrap().to_bytes();
    }

    assert_eq!(
        merkle_tree.check_valid_proof(
            Pubkey::from_str(asset_proof.leaf.as_str())
                .unwrap()
                .to_bytes(),
            &proofs,
            leaf_index
        ),
        true
    );
    assert_eq!(
        merkle_tree.check_valid_proof(
            Pubkey::from_str(asset_proof.leaf.as_str())
                .unwrap()
                .to_bytes(),
            &proofs,
            leaf_index + 1
        ),
        false
    );

    assert_eq!(
        env.rocks_env
            .storage
            .rollup_to_verify
            .get(metadata_hash.clone())
            .unwrap()
            .is_none(),
        true
    );

    assert_eq!(
        env.rocks_env
            .storage
            .rollups
            .get(metadata_hash.clone())
            .unwrap()
            .is_some(),
        true
    );
}

#[tokio::test]
async fn rollup_persister_download_fail_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let test_rollup = generate_rollup(10);

    let tmp_dir = tempfile::TempDir::new().unwrap();

    let tmp_file = File::create(tmp_dir.path().join("rollup-10.json")).unwrap();
    serde_json::to_writer(tmp_file, &test_rollup).unwrap();

    let download_attempts = 0;

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let rollup_to_verify = RollupToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts,
        persisting_state: PersistingRollupState::ReceivedTransaction,
    };

    env.rocks_env
        .storage
        .rollup_to_verify
        .put(metadata_hash.clone(), rollup_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockRollupDownloader::new();
    mocked_downloader
        .expect_download_rollup_and_check_checksum()
        .returning(move |_, _| Err(UsecaseError::Reqwest("Could not download file".to_string())));

    let rollup_persister = RollupPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(RollupPersisterMetricsConfig::new()),
    );

    let (rollup_to_verify, _) = env
        .rocks_env
        .storage
        .fetch_rollup_for_verifying()
        .await
        .unwrap();

    // ignoring error here because check the result later
    let (_, rx) = broadcast::channel::<()>(1);
    rollup_persister
        .persist_rollup(&rx, rollup_to_verify.unwrap(), None)
        .await
        .unwrap();


    let r = env
        .rocks_env
        .storage
        .rollup_to_verify
        .get(metadata_hash.clone())
        .unwrap()
        .unwrap();

    assert_eq!(r.download_attempts, download_attempts + 1);
}

#[tokio::test]
async fn rollup_persister_drop_from_queue_after_download_fail_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let test_rollup = generate_rollup(10);

    let tmp_dir = tempfile::TempDir::new().unwrap();

    let tmp_file = File::create(tmp_dir.path().join("rollup-10.json")).unwrap();
    serde_json::to_writer(tmp_file, &test_rollup).unwrap();

    let download_attempts = MAX_ROLLUP_DOWNLOAD_ATTEMPTS;

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let rollup_to_verify = RollupToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts,
        persisting_state: PersistingRollupState::ReceivedTransaction,
    };

    env.rocks_env
        .storage
        .rollup_to_verify
        .put(metadata_hash.clone(), rollup_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockRollupDownloader::new();
    mocked_downloader
        .expect_download_rollup_and_check_checksum()
        .returning(move |_, _| Err(UsecaseError::Reqwest("Could not download file".to_string())));

    let rollup_persister = RollupPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(RollupPersisterMetricsConfig::new()),
    );

    let (rollup_to_verify, _) = env
        .rocks_env
        .storage
        .fetch_rollup_for_verifying()
        .await
        .unwrap();

    // ignoring error here because check the result later
    let (_, rx) = broadcast::channel::<()>(1);
    rollup_persister
        .persist_rollup(&rx, rollup_to_verify.unwrap(), None)
        .await
        .unwrap();


    let r = env
        .rocks_env
        .storage
        .rollup_to_verify
        .get(metadata_hash.clone())
        .unwrap();

    assert_eq!(r.is_none(), true);

    let key = FailedRollupKey {
        status: FailedRollupState::DownloadFailed,
        hash: metadata_hash.clone(),
    };
    let failed_rollup = env
        .rocks_env
        .storage
        .failed_rollups
        .get(key)
        .unwrap()
        .unwrap();

    assert_eq!(failed_rollup.file_hash, metadata_hash.clone());
    assert_eq!(failed_rollup.download_attempts, download_attempts + 1);
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
        env.rocks_env.storage.clone(),
        Arc::new(nft_ingester::rollup::rollup_processor::NoopRollupTxSender {}),
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
        .times(nft_ingester::rollup::rollup_processor::MAX_ROLLUP_RETRIES - 1)
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
        env.rocks_env.storage.clone(),
        Arc::new(nft_ingester::rollup::rollup_processor::NoopRollupTxSender {}),
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
        .times(nft_ingester::rollup::rollup_processor::MAX_ROLLUP_RETRIES)
        .returning(|_, _| {
            Box::pin(async { Err(IngesterError::Arweave("test error".to_string())) })
        });
    permanent_storage_client
        .expect_get_metadata_url()
        .returning(|_| "".to_string());
    let rollup_processor = RollupProcessor::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(nft_ingester::rollup::rollup_processor::NoopRollupTxSender {}),
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
