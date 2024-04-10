use std::collections::HashMap;
use std::fs::File;

use anchor_lang::prelude::*;
use async_trait::async_trait;
use mpl_bubblegum::types::MetadataArgs;

use entities::rollup::{BatchMintInstruction, RolledMintInstruction, Rollup};
use interface::error::UsecaseError;
use interface::rollup::RollupDownloader;
use nft_ingester::bubblegum_updates_processor::BubblegumTxProcessor;
use nft_ingester::rollup_processor::find_rollup_pda;
use rand::{thread_rng, Rng};
use solana_sdk::keccak;
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::ConcurrentMerkleTree;
use testcontainers::clients::Cli;

fn generate_rollup(size: usize) -> Rollup {
    let authority = Pubkey::new_unique();
    let nonce: u32 = 1035;

    let (tree, _) = find_rollup_pda(&authority, nonce as u64);
    let mut mints = Vec::new();
    let mut merkle = ConcurrentMerkleTree::<24, 1024>::new();
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
        let owner = Pubkey::new_unique();
        let delegate = Pubkey::new_unique();

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

        // let leaf = LeafSchema::V1 {
        //     id,
        //     owner,
        //     delegate,
        //     nonce,
        //     data_hash: data_hash.to_bytes(),
        //     creator_hash: creator_hash.to_bytes(),
        // };
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
        // merkle.rightmost_proof.proof
        last_leaf_hash = hashed_leaf;

        let rolled_mint = RolledMintInstruction {
            mint_args,
            authority,
            owner,
            delegate,
            nonce,
            id,
        };
        mints.push(rolled_mint);
    }
    let rollup = Rollup {
        tree_authority: authority,
        tree_nonce: nonce as u64,
        tree_id: tree,
        raw_metadata_map: HashMap::new(),
        rolled_mints: mints,
        merkle_root: merkle.get_root(),
        last_leaf_hash,
    };

    rollup
}

#[test]
fn test_generate_1_000_rollup() {
    let rollup = generate_rollup(1000);
    assert_eq!(rollup.rolled_mints.len(), 1000);
    let file = File::create("rollup-1000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
fn test_generate_10_000_rollup() {
    let rollup = generate_rollup(10_000);
    assert_eq!(rollup.rolled_mints.len(), 10_000);
    println!("rollup of {:?} assets created", rollup.rolled_mints.len());
    let file = File::create("rollup-10_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
fn test_generate_100_000_rollup() {
    let rollup = generate_rollup(100_000);
    assert_eq!(rollup.rolled_mints.len(), 100_000);
    println!("rollup of {:?} assets created", rollup.rolled_mints.len());
    let file = File::create("rollup-100_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
fn test_generate_1_000_000_rollup() {
    let rollup = generate_rollup(1_000_000);
    assert_eq!(rollup.rolled_mints.len(), 1_000_000);
    let file = File::create("rollup-1_000_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

#[test]
fn test_generate_10_000_000_rollup() {
    let rollup = generate_rollup(10_000_000);
    assert_eq!(rollup.rolled_mints.len(), 10_000_000);
    let file = File::create("rollup-10_000_000.json").unwrap();
    serde_json::to_writer(file, &rollup).unwrap()
}

const ROLLUP_ASSETS_TO_SAVE: usize = 1_000;
struct TestRollupDownloader {}
#[async_trait]
impl RollupDownloader for TestRollupDownloader {
    async fn download_rollup(&self, _url: &str) -> std::result::Result<Box<Rollup>, UsecaseError> {
        Ok(Box::new(generate_rollup(ROLLUP_ASSETS_TO_SAVE)))
    }
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

    let static_iter = env.rocks_env.storage.asset_static_data.iter_start();
    assert_eq!(static_iter.count(), ROLLUP_ASSETS_TO_SAVE);
}
