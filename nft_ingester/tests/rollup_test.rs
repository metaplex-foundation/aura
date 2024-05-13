use std::collections::HashMap;
#[allow(unused_imports)]
use std::fs::File;
use std::str::FromStr;

use anchor_lang::prelude::*;
use async_trait::async_trait;
use mpl_bubblegum::types::{LeafSchema, MetadataArgs};

use entities::rollup::{BatchMintInstruction, ChangeLogEventV1, RolledMintInstruction, Rollup};
use interface::error::UsecaseError;
use interface::rollup::RollupDownloader;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use rand::{thread_rng, Rng};
use solana_sdk::keccak;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use spl_account_compression::ConcurrentMerkleTree;
use testcontainers::clients::Cli;

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
