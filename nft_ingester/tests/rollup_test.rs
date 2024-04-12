use std::collections::HashMap;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use anchor_lang::prelude::*;
use async_trait::async_trait;
use mpl_bubblegum::types::{LeafSchema, MetadataArgs};

use digital_asset_types::rpc::{Asset, AssetProof};
use entities::api_req_params::{GetAsset, GetAssetProof, Options};
use entities::rollup::{BatchMintInstruction, ChangeLogEventV1, RolledMintInstruction, Rollup};
use interface::error::UsecaseError;
// use interface::proofs::ProofChecker;
use interface::rollup::RollupDownloader;
use metrics_utils::ApiMetricsConfig;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use rand::{thread_rng, Rng};
// use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::keccak;
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::ConcurrentMerkleTree;
use testcontainers::clients::Cli;
use usecase::proofs::MaybeProofChecker;

fn generate_rollup(size: usize) -> (Rollup, ConcurrentMerkleTree<10, 32>) {
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
                seq: nonce + 1,
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
        rolled_mints: mints,
        merkle_root: merkle.get_root(),
        last_leaf_hash,
    };

    (rollup, merkle)
}

#[test]
fn test_generate_1_000_rollup() {
    let (rollup, _) = generate_rollup(1000);
    assert_eq!(rollup.rolled_mints.len(), 1000);
    let file = File::create("rollup-1000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
fn test_generate_10_000_rollup() {
    let (rollup, _) = generate_rollup(10_000);
    assert_eq!(rollup.rolled_mints.len(), 10_000);
    println!("rollup of {:?} assets created", rollup.rolled_mints.len());
    let file = File::create("rollup-10_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
fn test_generate_100_000_rollup() {
    let (rollup, _) = generate_rollup(100_000);
    assert_eq!(rollup.rolled_mints.len(), 100_000);
    println!("rollup of {:?} assets created", rollup.rolled_mints.len());
    let file = File::create("rollup-100_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

// #[test]
fn test_generate_1_000_000_rollup() {
    let (rollup, _) = generate_rollup(1_000_000);
    assert_eq!(rollup.rolled_mints.len(), 1_000_000);
    let file = File::create("rollup-1_000_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

// #[test]
fn test_generate_10_000_000_rollup() {
    let (rollup, _) = generate_rollup(10_000_000);
    assert_eq!(rollup.rolled_mints.len(), 10_000_000);
    let file = File::create("rollup-10_000_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

const ROLLUP_ASSETS_TO_SAVE: usize = 1_000;
struct TestRollupDownloader {}
#[async_trait]
impl RollupDownloader for TestRollupDownloader {
    async fn download_rollup(&self, _url: &str) -> std::result::Result<Box<Rollup>, UsecaseError> {
        let json_file = std::fs::read_to_string(
            "/home/requesco/Desktop/utility-chain/nft_ingester/rollup-1000.json",
        )
        .unwrap();
        let rollup: Rollup = serde_json::from_str(&json_file).unwrap();
        Ok(Box::new(rollup))
    }
}

fn generate() -> ConcurrentMerkleTree<10, 32> {
    let json_file = std::fs::read_to_string(
        "/home/requesco/Desktop/utility-chain/nft_ingester/rollup-1000.json",
    )
    .unwrap();
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
        // println!(
        //     "ID: {:?}, LeafIdx: {}, Hash: {:?}",
        //     id,
        //     nonce,
        //     Pubkey::from(leaf.hash()).to_string()
        // );
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
        },
        TestRollupDownloader {},
        env.rocks_env.storage.clone(),
    )
    .await
    .unwrap();

    let api = nft_ingester::api::api_impl::DasApi::<MaybeProofChecker>::new(
        env.pg_env.client.clone(),
        env.rocks_env.storage.clone(),
        Arc::new(ApiMetricsConfig::new()),
        None,
        50,
    );
    let proofs: AssetProof = serde_json::from_value(
        api.get_asset_proof(GetAssetProof {
            id: "8BxAYGxr5UvhDJpK6C62acfp4CScKZ7cryXeK1G52qTK".to_string(),
        })
        .await
        .unwrap(),
    )
    .unwrap();
    // let asset: Asset = serde_json::from_value(
    //     api.get_asset(GetAsset {
    //         id: "8BxAYGxr5UvhDJpK6C62acfp4CScKZ7cryXeK1G52qTK".to_string(),
    //         options: Some(Options {
    //             show_unverified_collections: true,
    //         }),
    //     })
    //     .await
    //     .unwrap(),
    // )
    // .unwrap();

    // let json_file = std::fs::read_to_string(
    //     "/home/requesco/Desktop/utility-chain/nft_ingester/rollup-1000.json",
    // )
    // .unwrap();
    // let rollup: Rollup = serde_json::from_str(&json_file).unwrap();

    // let generated_merkle_tree = generate();
    // generated_merkle_tree.active_index = 0;
    // let prove_leaf = generated_merkle_tree.check_valid_proof(
    //     Pubkey::from_str(&proofs.leaf).unwrap().to_bytes(),
    //     &proofs
    //         .clone()
    //         .proof
    //         .into_iter()
    //         .map(|proof| Pubkey::from_str(&proof).unwrap().to_bytes())
    //         .collect::<Vec<_>>()
    //         .try_into()
    //         .unwrap(),
    //     851,
    // );
    // println!("from file prove_leaf: {:?}", prove_leaf);
    // println!("from file active index: {}", generated_merkle_tree.active_index);
    // println!("from file sequence_number: {}", generated_merkle_tree.sequence_number);
    // println!("from file rightmost_proof: {:?}", generated_merkle_tree.rightmost_proof);
    // println!("from file change_logs: {:?}", generated_merkle_tree.change_logs);
    // println!("from file buffer_size: {}", generated_merkle_tree.buffer_size);
    // let mut merkle_tree = ConcurrentMerkleTree::<10, 32>::new();
    // merkle_tree
    //     .initialize_with_root(
    //         rollup.merkle_root,
    //         rollup.rolled_mints.last().unwrap().tree_update.path[0].node,
    //         generated_merkle_tree
    //             .rightmost_proof
    //             .proof
    //             .to_vec()
    //             .as_slice(),
    //         rollup.rolled_mints.len().saturating_sub(1) as u32,
    //     )
    //     .unwrap();
    // println!("active index: {}", merkle_tree.active_index);
    // println!("sequence_number: {}", merkle_tree.sequence_number);
    // println!("rightmost_proof: {:?}", merkle_tree.rightmost_proof);
    // println!("change_logs: {:?}", merkle_tree.change_logs);
    // println!("buffer_size: {}", merkle_tree.buffer_size);
    // println!("buffer_size: {}", merkle_tree.rightmost_proof.index);
    // let prove_leaf = merkle_tree.check_valid_proof(
    //     Pubkey::from_str(&proofs.leaf).unwrap().to_bytes(),
    //     &proofs
    //         .clone()
    //         .proof
    //         .into_iter()
    //         .map(|proof| Pubkey::from_str(&proof).unwrap().to_bytes())
    //         .collect::<Vec<_>>()
    //         .try_into()
    //         .unwrap(),
    //     851,
    // );
    // println!("prove_leaf: {:?}", prove_leaf);
    println!(
        "{:?}",
        proofs
            .clone()
            .proof
            .into_iter()
            .map(|proof| Pubkey::from_str(&proof).unwrap().to_bytes())
            .collect::<Vec<_>>()
    );
    // println!("{:?}", proofs);
    // println!("{:?}", asset);
    // println!("{:?}", Pubkey::from_str(&proofs.leaf).unwrap().to_bytes());

    // let proof_checker = MaybeProofChecker::new(
    //     Arc::new(RpcClient::new(
    //         "https://solana-devnet.rpc.extrnode.com/cad7543e-9ad3-4fd4-b637-574f1ba1a847"
    //             .to_string(),
    //     )),
    //     0.0,
    //     Default::default(),
    // );
    // println!(
    //     "check_proof: {}",
    //     proof_checker
    //         .check_proof(
    //             Pubkey::from_str(&proofs.tree_id).unwrap(),
    //             proofs
    //                 .clone()
    //                 .proof
    //                 .into_iter()
    //                 .map(|proof| Pubkey::from_str(&proof).unwrap())
    //                 .collect::<Vec<_>>(),
    //             851,
    //             Pubkey::from_str(&proofs.leaf).unwrap().to_bytes()
    //         )
    //         .await
    //         .unwrap()
    // );
}
