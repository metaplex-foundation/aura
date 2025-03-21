use std::{collections::HashMap, fs::File, str::FromStr, sync::Arc, time::Duration};

use anchor_lang::prelude::*;
#[cfg(feature = "batch_mint_tests")]
use async_trait::async_trait;
use bubblegum_batch_sdk::{
    batch_mint_client::BatchMintClient, batch_mint_validations::generate_batch_mint,
    model::BatchMint,
};
use entities::{
    api_req_params::{GetAssetProof, GetAssetProofBatch},
    enums::{BatchMintState, FailedBatchMintState, PersistingBatchMintState},
    models::{BatchMintToVerify, BatchMintWithState, BufferedTransaction},
};
use flatbuffers::FlatBufferBuilder;
#[cfg(feature = "batch_mint_tests")]
use interface::batch_mint::BatchMintDownloader;
use interface::{
    account_balance::MockAccountBalanceGetter, batch_mint::MockBatchMintDownloader,
    error::UsecaseError,
};
use metrics_utils::{
    ApiMetricsConfig, BatchMintPersisterMetricsConfig, BatchMintProcessorMetricsConfig,
    IngesterMetricsConfig,
};
use mockall::predicate;
use mpl_bubblegum::types::{Creator, LeafSchema, MetadataArgs};
use nft_ingester::{
    api::{dapi::rpc_asset_models::AssetProof, error::DasApiError},
    batch_mint::{
        batch_mint_persister::{BatchMintPersister, MAX_BATCH_MINT_DOWNLOAD_ATTEMPTS},
        batch_mint_processor::{BatchMintProcessor, MockPermanentStorageClient},
    },
    config::{HealthCheckInfo, JsonMiddlewareConfig},
    error::IngesterError,
    json_worker::JsonWorker,
    processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
    raydium_price_fetcher::RaydiumTokenPriceFetcher,
};
use plerkle_serialization::serializer::serialize_transaction;
use postgre_client::PgClient;
use rocks_db::{columns::batch_mint::FailedBatchMintKey, Storage};
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::{
    instruction::CompiledInstruction,
    message::{Message, MessageHeader},
};
use solana_sdk::{
    keccak,
    pubkey::Pubkey,
    signature::{Signature, Signer},
    signer::keypair::Keypair,
    transaction::{SanitizedTransaction, Transaction},
};
use solana_transaction_status::{InnerInstruction, InnerInstructions, TransactionStatusMeta};
use spl_account_compression::ConcurrentMerkleTree;
use tempfile::TempDir;
use testcontainers::clients::Cli;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use usecase::proofs::MaybeProofChecker;
use uuid::Uuid;

#[test]
#[cfg(feature = "batch_mint_tests")]
fn test_generate_1_000_batch_mint() {
    let batch_mint = generate_batch_mint(1000);
    assert_eq!(batch_mint.batch_mints.len(), 1000);
    let file = File::create("batch_mint-1000.json").unwrap();
    serde_json::to_writer(file, &batch_mint).unwrap()
}

#[test]
#[cfg(feature = "batch_mint_tests")]
fn test_generate_10_000_batch_mint() {
    let batch_mint = generate_batch_mint(10_000);
    assert_eq!(batch_mint.batch_mints.len(), 10_000);
    println!("batch_mint of {:?} assets created", batch_mint.batch_mints.len());
    let file = File::create("batch_mint-10_000.json").unwrap();
    serde_json::to_writer(file, &batch_mint).unwrap()
}

#[test]
#[cfg(feature = "batch_mint_tests")]
fn test_generate_100_000_batch_mint() {
    let batch_mint = generate_batch_mint(100_000);
    assert_eq!(batch_mint.batch_mints.len(), 100_000);
    println!("batch_mint of {:?} assets created", batch_mint.batch_mints.len());
    let file = File::create("batch_mint-100_000.json").unwrap();
    serde_json::to_writer(file, &batch_mint).unwrap()
}

#[test]
#[cfg(feature = "batch_mint_tests")]
fn test_generate_1_000_000_batch_mint() {
    let batch_mint = generate_batch_mint(1_000_000);
    assert_eq!(batch_mint.batch_mints.len(), 1_000_000);
    let file = File::create("batch_mint-1_000_000.json").unwrap();
    serde_json::to_writer(file, &batch_mint).unwrap()
}

#[test]
#[cfg(feature = "batch_mint_tests")]
fn test_generate_10_000_000_batch_mint() {
    let batch_mint = generate_batch_mint(10_000_000);
    assert_eq!(batch_mint.batch_mints.len(), 10_000_000);
    let file = File::create("batch_mint-10_000_000.json").unwrap();
    serde_json::to_writer(file, &batch_mint).unwrap()
}

#[cfg(feature = "batch_mint_tests")]
const BATCH_MINT_ASSETS_TO_SAVE: usize = 1_000;

#[cfg(feature = "batch_mint_tests")]
struct TestBatchMintCreator;

#[cfg(feature = "batch_mint_tests")]
#[async_trait]
impl BatchMintDownloader for TestBatchMintCreator {
    async fn download_batch_mint(
        &self,
        _url: &str,
    ) -> std::result::Result<Box<BatchMint>, UsecaseError> {
        // let json_file = std::fs::read_to_string("../batch_mint-1000.json").unwrap();
        // let batch_mint: BatchMint = serde_json::from_str(&json_file).unwrap();

        Ok(Box::new(generate_batch_mint(BATCH_MINT_ASSETS_TO_SAVE)))
    }

    async fn download_batch_mint_and_check_checksum(
        &self,
        _url: &str,
        _checksum: &str,
    ) -> std::result::Result<Box<BatchMint>, UsecaseError> {
        Ok(Box::new(generate_batch_mint(BATCH_MINT_ASSETS_TO_SAVE)))
    }
}

fn generate_merkle_tree_from_batch_mint(batch_mint: &BatchMint) -> ConcurrentMerkleTree<10, 32> {
    let mut merkle_tree = ConcurrentMerkleTree::<10, 32>::new();
    merkle_tree.initialize().unwrap();

    for (nonce, asset) in batch_mint.batch_mints.iter().enumerate() {
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
            creator_data.iter().map(|c| c.as_slice()).collect::<Vec<&[u8]>>().as_ref(),
        );

        let id = mpl_bubblegum::utils::get_asset_id(&batch_mint.tree_id, nonce as u64);

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

#[tokio::test(flavor = "multi_thread")]
async fn save_batch_mint_to_queue_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let bubblegum_updates_processor = BubblegumTxProcessor::new(
        env.rocks_env.storage.clone(),
        Arc::new(IngesterMetricsConfig::new()),
    );

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();

    // arbitrary data
    let batch_mint_instruction_data =
        mpl_bubblegum::instructions::FinalizeTreeWithRootInstructionArgs {
            root: [1; 32],
            rightmost_leaf: [1; 32],
            rightmost_index: 99,
            metadata_url: metadata_url.clone(),
            metadata_hash: metadata_hash.clone(),
        };

    // took it from Bubblegum client
    // this value is generated by Anchor library, it's instruction identifier
    let mut instruction_data = vec![77, 73, 220, 153, 126, 225, 64, 204];
    instruction_data.extend(batch_mint_instruction_data.try_to_vec().unwrap().iter());

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
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
            ],
            recent_blockhash: [1; 32].into(),
            instructions: vec![CompiledInstruction {
                program_id_index: 1,
                accounts: vec![2, 3, 4, 5, 6],
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

    bubblegum_updates_processor.process_transaction(buffered_transaction, true).await.unwrap();

    let r = env.rocks_env.storage.batch_mint_to_verify.get(metadata_hash.clone()).unwrap().unwrap();

    assert_eq!(r.file_hash, metadata_hash);
    assert_eq!(r.url, metadata_url);
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_mint_with_verified_creators_test() {
    // For this test it's necessary to use Solana mainnet RPC
    let url = "https://api.mainnet-beta.solana.com".to_string();
    let solana_client = Arc::new(RpcClient::new_with_timeout(url, Duration::from_secs(3)));
    // Merkle tree created in mainnet for testing purposes
    let tree_key = Pubkey::from_str("AGMiLKtXX7PiVneM8S1KkTmCnF7X5zh6bKq4t1Mhrwpb").unwrap();

    // First we have to create offchain Merkle tree with SDK

    let batch_mint_client = BatchMintClient::new(solana_client);
    let mut batch_mint_builder =
        batch_mint_client.create_batch_mint_builder(&tree_key).await.unwrap();

    let asset_creator = Keypair::new();
    let owner = Keypair::new();
    let delegate = Keypair::new();

    let asset = MetadataArgs {
        name: "Name".to_string(),
        symbol: "Symbol".to_string(),
        uri: "https://immutable-storage/asset/".to_string(),
        seller_fee_basis_points: 0,
        primary_sale_happened: false,
        is_mutable: false,
        edition_nonce: None,
        token_standard: Some(mpl_bubblegum::types::TokenStandard::NonFungible),
        collection: None,
        uses: None,
        token_program_version: mpl_bubblegum::types::TokenProgramVersion::Original,
        creators: vec![Creator { address: asset_creator.pubkey(), verified: true, share: 100 }],
    };

    let metadata_hash_arg =
        batch_mint_builder.add_asset(&owner.pubkey(), &delegate.pubkey(), &asset).unwrap();

    let signature = asset_creator.sign_message(&metadata_hash_arg.get_message());

    let mut creators_signatures = HashMap::new();
    creators_signatures.insert(asset_creator.pubkey(), signature);

    let mut message_and_signatures = HashMap::new();
    message_and_signatures.insert(metadata_hash_arg.get_nonce(), creators_signatures);

    batch_mint_builder.add_signatures_for_verified_creators(message_and_signatures).unwrap();

    let finalized_batch_mint = batch_mint_builder.build_batch_mint().unwrap();

    // Offchain Merkle tree creation is finished
    // Start to process it

    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let tmp_dir = tempfile::TempDir::new().unwrap();

    // dump batch_mint into .json file not to cast one BatchMint struct into another one
    let tmp_file = File::create(tmp_dir.path().join("batch_mint-1.json")).unwrap();
    serde_json::to_writer(tmp_file, &finalized_batch_mint).unwrap();

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let batch_mint_to_verify = BatchMintToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts: 0,
        persisting_state: PersistingBatchMintState::ReceivedTransaction,
        staker: Default::default(),
        collection_mint: None,
    };

    env.rocks_env
        .storage
        .batch_mint_to_verify
        .put(metadata_hash.clone(), batch_mint_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockBatchMintDownloader::new();
    mocked_downloader
        .expect_download_batch_mint_and_check_checksum()
        .with(predicate::eq("url".to_string()), predicate::eq("hash".to_string()))
        .returning(move |_, _| {
            let json_file =
                std::fs::read_to_string(tmp_dir.path().join("batch_mint-1.json")).unwrap();
            Ok(Box::new(serde_json::from_str(&json_file).unwrap()))
        });

    let batch_mint_persister = BatchMintPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(BatchMintPersisterMetricsConfig::new()),
    );

    let (batch_mint_to_verify, _) =
        env.rocks_env.storage.fetch_batch_mint_for_verifying().await.unwrap();

    batch_mint_persister
        .persist_batch_mint(CancellationToken::new(), batch_mint_to_verify.unwrap(), None)
        .await;

    let api = nft_ingester::api::api_impl::DasApi::<
        MaybeProofChecker,
        JsonWorker,
        JsonWorker,
        MockAccountBalanceGetter,
        RaydiumTokenPriceFetcher,
        Storage,
    >::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        HealthCheckInfo {
            node_name: Some("test".to_string()),
            app_version: "1.0".to_string(),
            image_info: None,
        },
        Arc::new(ApiMetricsConfig::new()),
        None,
        None,
        50,
        None,
        None,
        JsonMiddlewareConfig::default(),
        Arc::new(MockAccountBalanceGetter::new()),
        None,
        Arc::new(RaydiumTokenPriceFetcher::default()),
        "".to_string(),
    );

    let payload = GetAssetProof { id: metadata_hash_arg.get_asset_id().to_string() };
    let proof_result = api.get_asset_proof(payload).await.unwrap();
    let asset_proof: AssetProof = serde_json::from_value(proof_result).unwrap();

    assert_eq!(asset_proof.proof.is_empty(), false);
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_mint_with_unverified_creators_test() {
    // For this test it's necessary to use Solana mainnet RPC
    let url = "https://api.mainnet-beta.solana.com".to_string();
    let solana_client = Arc::new(RpcClient::new_with_timeout(url, Duration::from_secs(3)));
    // Merkle tree created in mainnet for testing purposes
    let tree_key = Pubkey::from_str("AGMiLKtXX7PiVneM8S1KkTmCnF7X5zh6bKq4t1Mhrwpb").unwrap();

    // First we have to create offchain Merkle tree with SDK

    let batch_mint_client = BatchMintClient::new(solana_client);
    let mut batch_mint_builder =
        batch_mint_client.create_batch_mint_builder(&tree_key).await.unwrap();

    let asset_creator = Keypair::new();
    let owner = Keypair::new();
    let delegate = Keypair::new();

    let asset = MetadataArgs {
        name: "Name".to_string(),
        symbol: "Symbol".to_string(),
        uri: "https://immutable-storage/asset/".to_string(),
        seller_fee_basis_points: 0,
        primary_sale_happened: false,
        is_mutable: false,
        edition_nonce: None,
        token_standard: Some(mpl_bubblegum::types::TokenStandard::NonFungible),
        collection: None,
        uses: None,
        token_program_version: mpl_bubblegum::types::TokenProgramVersion::Original,
        creators: vec![Creator { address: asset_creator.pubkey(), verified: true, share: 100 }],
    };

    let metadata_hash_arg =
        batch_mint_builder.add_asset(&owner.pubkey(), &delegate.pubkey(), &asset).unwrap();

    let signature = asset_creator.sign_message(&metadata_hash_arg.get_message());

    let mut creators_signatures = HashMap::new();
    creators_signatures.insert(asset_creator.pubkey(), signature);

    let mut message_and_signatures = HashMap::new();
    message_and_signatures.insert(metadata_hash_arg.get_nonce(), creators_signatures);

    batch_mint_builder.add_signatures_for_verified_creators(message_and_signatures).unwrap();

    let mut finalized_batch_mint = batch_mint_builder.build_batch_mint().unwrap();
    // drop signature from the batch_mint to test if verification on indexer side will catch it
    for mint in finalized_batch_mint.batch_mints.iter_mut() {
        mint.creator_signature = None;
    }

    // Offchain Merkle tree creation is finished
    // Start to process it

    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let tmp_dir = tempfile::TempDir::new().unwrap();

    // dump batch_mint into .json file not to cast one BatchMint struct into another one
    let tmp_file = File::create(tmp_dir.path().join("batch_mint-1.json")).unwrap();
    serde_json::to_writer(tmp_file, &finalized_batch_mint).unwrap();

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let batch_mint_to_verify = BatchMintToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts: 0,
        persisting_state: PersistingBatchMintState::ReceivedTransaction,
        staker: Default::default(),
        collection_mint: None,
    };

    env.rocks_env
        .storage
        .batch_mint_to_verify
        .put(metadata_hash.clone(), batch_mint_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockBatchMintDownloader::new();
    mocked_downloader
        .expect_download_batch_mint_and_check_checksum()
        .with(predicate::eq("url".to_string()), predicate::eq("hash".to_string()))
        .returning(move |_, _| {
            let json_file =
                std::fs::read_to_string(tmp_dir.path().join("batch_mint-1.json")).unwrap();
            Ok(Box::new(serde_json::from_str(&json_file).unwrap()))
        });

    let batch_mint_persister = BatchMintPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(BatchMintPersisterMetricsConfig::new()),
    );

    let (batch_mint_to_verify, _) =
        env.rocks_env.storage.fetch_batch_mint_for_verifying().await.unwrap();

    batch_mint_persister
        .persist_batch_mint(CancellationToken::new(), batch_mint_to_verify.unwrap(), None)
        .await;

    let api = nft_ingester::api::api_impl::DasApi::<
        MaybeProofChecker,
        JsonWorker,
        JsonWorker,
        MockAccountBalanceGetter,
        RaydiumTokenPriceFetcher,
        Storage,
    >::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        HealthCheckInfo {
            node_name: Some("test".to_string()),
            app_version: "1.0".to_string(),
            image_info: None,
        },
        Arc::new(ApiMetricsConfig::new()),
        None,
        None,
        50,
        None,
        None,
        JsonMiddlewareConfig::default(),
        Arc::new(MockAccountBalanceGetter::new()),
        None,
        Arc::new(RaydiumTokenPriceFetcher::default()),
        "".to_string(),
    );

    let payload = GetAssetProof { id: metadata_hash_arg.get_asset_id().to_string() };

    // batch_mint update should not be processed
    // and as a result proof for asset cannot be extracted
    match api.get_asset_proof(payload).await {
        Ok(_) => panic!("Request should fail"),
        Err(err) => match err {
            DasApiError::ProofNotFound => {},
            _ => panic!("API returned wrong error"),
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_mint_persister_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let test_batch_mint = generate_batch_mint(10);

    let tmp_dir = tempfile::TempDir::new().unwrap();

    let tmp_file = File::create(tmp_dir.path().join("batch_mint-10.json")).unwrap();
    serde_json::to_writer(tmp_file, &test_batch_mint).unwrap();

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let batch_mint_to_verify = BatchMintToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts: 0,
        persisting_state: PersistingBatchMintState::ReceivedTransaction,
        staker: Default::default(),
        collection_mint: None,
    };

    env.rocks_env
        .storage
        .batch_mint_to_verify
        .put(metadata_hash.clone(), batch_mint_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockBatchMintDownloader::new();
    mocked_downloader.expect_download_batch_mint_and_check_checksum().returning(move |_, _| {
        let json_file = std::fs::read_to_string(tmp_dir.path().join("batch_mint-10.json")).unwrap();
        Ok(Box::new(serde_json::from_str(&json_file).unwrap()))
    });

    let batch_mint_persister = BatchMintPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(BatchMintPersisterMetricsConfig::new()),
    );

    let (batch_mint_to_verify, _) =
        env.rocks_env.storage.fetch_batch_mint_for_verifying().await.unwrap();

    batch_mint_persister
        .persist_batch_mint(CancellationToken::new(), batch_mint_to_verify.unwrap(), None)
        .await;

    let merkle_tree = generate_merkle_tree_from_batch_mint(&test_batch_mint);

    let api = nft_ingester::api::api_impl::DasApi::<
        MaybeProofChecker,
        JsonWorker,
        JsonWorker,
        MockAccountBalanceGetter,
        RaydiumTokenPriceFetcher,
        Storage,
    >::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        HealthCheckInfo {
            node_name: Some("test".to_string()),
            app_version: "1.0".to_string(),
            image_info: None,
        },
        Arc::new(ApiMetricsConfig::new()),
        None,
        None,
        50,
        None,
        None,
        JsonMiddlewareConfig::default(),
        Arc::new(MockAccountBalanceGetter::new()),
        None,
        Arc::new(RaydiumTokenPriceFetcher::default()),
        "".to_string(),
    );

    let leaf_index = 4u32;

    let payload = GetAssetProof {
        id: test_batch_mint
            .batch_mints
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
            Pubkey::from_str(asset_proof.leaf.as_str()).unwrap().to_bytes(),
            &proofs,
            leaf_index
        ),
        true
    );
    assert_eq!(
        merkle_tree.check_valid_proof(
            Pubkey::from_str(asset_proof.leaf.as_str()).unwrap().to_bytes(),
            &proofs,
            leaf_index + 1
        ),
        false
    );

    assert_eq!(
        env.rocks_env.storage.batch_mint_to_verify.get(metadata_hash.clone()).unwrap().is_none(),
        true
    );

    assert_eq!(
        env.rocks_env.storage.batch_mints.get(metadata_hash.clone()).unwrap().is_some(),
        true
    );
    // Test get asset proof batch
    let payload = GetAssetProofBatch {
        ids: test_batch_mint
            .batch_mints
            .into_iter()
            .map(|lu| lu.leaf_update.id().to_string())
            .take(10)
            .collect(),
    };
    let proof_result = api.get_asset_proof_batch(payload).await.unwrap();
    let asset_proofs: HashMap<String, Option<AssetProof>> =
        serde_json::from_value(proof_result).unwrap();
    for (_key, proof) in asset_proofs {
        assert!(proof.is_some())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_mint_persister_download_fail_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let test_batch_mint = generate_batch_mint(10);

    let tmp_dir = tempfile::TempDir::new().unwrap();

    let tmp_file = File::create(tmp_dir.path().join("batch_mint-10.json")).unwrap();
    serde_json::to_writer(tmp_file, &test_batch_mint).unwrap();

    let download_attempts = 0;

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let batch_mint_to_verify = BatchMintToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts,
        persisting_state: PersistingBatchMintState::ReceivedTransaction,
        staker: Default::default(),
        collection_mint: None,
    };

    env.rocks_env
        .storage
        .batch_mint_to_verify
        .put(metadata_hash.clone(), batch_mint_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockBatchMintDownloader::new();
    mocked_downloader
        .expect_download_batch_mint_and_check_checksum()
        .returning(move |_, _| Err(UsecaseError::Reqwest("Could not download file".to_string())));

    let batch_mint_persister = BatchMintPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(BatchMintPersisterMetricsConfig::new()),
    );

    let (batch_mint_to_verify, _) =
        env.rocks_env.storage.fetch_batch_mint_for_verifying().await.unwrap();

    batch_mint_persister
        .persist_batch_mint(CancellationToken::new(), batch_mint_to_verify.unwrap(), None)
        .await;

    let r = env.rocks_env.storage.batch_mint_to_verify.get(metadata_hash.clone()).unwrap();

    assert_eq!(r.is_none(), true);
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_mint_persister_drop_from_queue_after_download_fail_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;

    let test_batch_mint = generate_batch_mint(10);

    let tmp_dir = tempfile::TempDir::new().unwrap();

    let tmp_file = File::create(tmp_dir.path().join("batch_mint-10.json")).unwrap();
    serde_json::to_writer(tmp_file, &test_batch_mint).unwrap();

    let download_attempts = MAX_BATCH_MINT_DOWNLOAD_ATTEMPTS;

    let metadata_url = "url".to_string();
    let metadata_hash = "hash".to_string();
    let batch_mint_to_verify = BatchMintToVerify {
        file_hash: metadata_hash.clone(),
        url: metadata_url.clone(),
        created_at_slot: 10,
        signature: Signature::new_unique(),
        download_attempts,
        persisting_state: PersistingBatchMintState::ReceivedTransaction,
        staker: Default::default(),
        collection_mint: None,
    };

    env.rocks_env
        .storage
        .batch_mint_to_verify
        .put(metadata_hash.clone(), batch_mint_to_verify.clone())
        .unwrap();

    let mut mocked_downloader = MockBatchMintDownloader::new();
    mocked_downloader
        .expect_download_batch_mint_and_check_checksum()
        .returning(move |_, _| Err(UsecaseError::Reqwest("Could not download file".to_string())));

    let batch_mint_persister = BatchMintPersister::new(
        env.rocks_env.storage.clone(),
        mocked_downloader,
        Arc::new(BatchMintPersisterMetricsConfig::new()),
    );

    let (batch_mint_to_verify, _) =
        env.rocks_env.storage.fetch_batch_mint_for_verifying().await.unwrap();

    batch_mint_persister
        .persist_batch_mint(CancellationToken::new(), batch_mint_to_verify.unwrap(), None)
        .await;

    let r = env.rocks_env.storage.batch_mint_to_verify.get(metadata_hash.clone()).unwrap();

    assert_eq!(r.is_none(), true);

    let key = FailedBatchMintKey {
        status: FailedBatchMintState::DownloadFailed,
        hash: metadata_hash.clone(),
    };
    let failed_batch_mint = env.rocks_env.storage.failed_batch_mints.get(key).unwrap().unwrap();

    assert_eq!(failed_batch_mint.file_hash, metadata_hash.clone());
    assert_eq!(failed_batch_mint.download_attempts, download_attempts + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn xxhash_test() {
    let file_data = vec![43, 2, 5, 4, 76, 34, 123, 42, 73, 81, 47];

    let file_hash = xxhash_rust::xxh3::xxh3_128(&file_data);

    let hash_hex = hex::encode(file_hash.to_be_bytes());

    assert_eq!(&hash_hex, "4f299160368d57dccbb6deec075d5083");
}

async fn save_temp_batch_mint(
    dir: &TempDir,
    client: Arc<PgClient>,
    batch_mint: &BatchMint,
) -> String {
    let file_name = format!("{}.json", Uuid::new_v4());
    let full_file_path = format!("{}/{}", dir.path().to_str().unwrap(), &file_name);
    let mut file = tokio::fs::File::create(full_file_path).await.unwrap();
    file.write_all(json!(batch_mint).to_string().as_bytes()).await.unwrap();
    client.insert_new_batch_mint(&file_name).await.unwrap();
    file_name
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_mint_upload_test() {
    let cnt = 0;
    let cli = Cli::default();
    let (env, _) = setup::TestEnvironment::create(&cli, cnt, 100).await;
    let batch_mint = generate_batch_mint(1000);
    let mut permanent_storage_client = MockPermanentStorageClient::new();
    permanent_storage_client
        .expect_upload_file()
        .times(nft_ingester::batch_mint::batch_mint_processor::MAX_BATCH_MINT_RETRIES - 1)
        .returning(|_, _| {
            Box::pin(async { Err(IngesterError::Arweave("test error".to_string())) })
        });
    permanent_storage_client
        .expect_upload_file()
        .times(1)
        .returning(|_, _| Box::pin(async { Ok(("tx_id".to_string(), 100u64)) }));
    permanent_storage_client.expect_get_metadata_url().returning(|_| "".to_string());
    let dir = TempDir::new().unwrap();
    let file_name = save_temp_batch_mint(&dir, env.pg_env.client.clone(), &batch_mint).await;
    let batch_mint_processor = BatchMintProcessor::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(nft_ingester::batch_mint::batch_mint_processor::NoopBatchMintTxSender {}),
        Arc::new(permanent_storage_client),
        dir.path().to_str().unwrap().to_string(),
        Arc::new(BatchMintProcessorMetricsConfig::new()),
    );

    let processing_result = batch_mint_processor
        .process_batch_mint(
            CancellationToken::new(),
            BatchMintWithState {
                file_name,
                state: BatchMintState::Uploaded,
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
        .times(nft_ingester::batch_mint::batch_mint_processor::MAX_BATCH_MINT_RETRIES)
        .returning(|_, _| {
            Box::pin(async { Err(IngesterError::Arweave("test error".to_string())) })
        });
    permanent_storage_client.expect_get_metadata_url().returning(|_| "".to_string());
    let batch_mint_processor = BatchMintProcessor::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(nft_ingester::batch_mint::batch_mint_processor::NoopBatchMintTxSender {}),
        Arc::new(permanent_storage_client),
        dir.path().to_str().unwrap().to_string(),
        Arc::new(BatchMintProcessorMetricsConfig::new()),
    );

    let file_name = save_temp_batch_mint(&dir, env.pg_env.client.clone(), &batch_mint).await;
    let processing_result = batch_mint_processor
        .process_batch_mint(
            CancellationToken::new(),
            BatchMintWithState {
                file_name,
                state: BatchMintState::Uploaded,
                error: None,
                url: None,
                created_at: 0,
            },
        )
        .await;
    // Retries are exhausted
    assert_eq!(processing_result, Err(IngesterError::Arweave("Arweave: test error".to_string())));
}
