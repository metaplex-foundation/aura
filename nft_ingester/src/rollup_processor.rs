use crate::error::RollupValidationError;
use crate::tree_macros::validate_change_logs;
use anchor_lang::AnchorSerialize;
use async_trait::async_trait;
use entities::models::RollupWithState;
use entities::rollup::{RolledMintInstruction, Rollup};
use interface::error::UsecaseError;
use interface::rollup::RollupDownloader;
use mpl_bubblegum::utils::get_asset_id;
use postgre_client::model::RollupState;
use postgre_client::PgClient;
use rocks_db::Storage;
use solana_program::keccak;
use solana_program::keccak::Hash;
use solana_program::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};

pub struct RollupDownloaderImpl;
#[async_trait]
impl RollupDownloader for RollupDownloaderImpl {
    async fn download_rollup(&self, url: &str) -> Result<Box<Rollup>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }

    async fn download_rollup_and_check_checksum(
        &self,
        url: &str,
        checksum: &str,
    ) -> Result<Box<Rollup>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;

        let file_hash = xxhash_rust::xxh3::xxh3_128(&response);

        let hash_hex = hex::encode(file_hash.to_be_bytes());

        if hash_hex != checksum {
            return Err(UsecaseError::InvalidParameters(
                "File checksum mismatch".to_string(),
            ));
        }

        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}

pub struct RollupProcessor {
    pg_client: Arc<PgClient>,
    _rocks: Arc<Storage>,
    file_storage_path: String,
}

impl RollupProcessor {
    pub fn new(pg_client: Arc<PgClient>, rocks: Arc<Storage>, file_storage_path: String) -> Self {
        Self {
            pg_client,
            _rocks: rocks,
            file_storage_path,
        }
    }

    async fn process_rollup(&self, rollup_to_process: RollupWithState) {
        let json_file = match tokio::fs::read_to_string(format!(
            "{}/{}",
            self.file_storage_path, &rollup_to_process.file_name
        ))
        .await
        {
            Ok(json_file) => json_file,
            Err(e) => {
                error!("Failed to read file to string: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                return;
            }
        };
        let rollup = match serde_json::from_str::<Rollup>(&json_file) {
            Ok(rollup) => rollup,
            Err(e) => {
                if let Err(e) = self
                    .pg_client
                    .mark_rollup_as_verification_failed(
                        &rollup_to_process.file_name,
                        &e.to_string(),
                    )
                    .await
                {
                    error!("Failed to mark rollup as verification failed: {}", e);
                }
                return;
            }
        };
        if let Err(e) = self
            .pg_client
            .update_rollup_state(&rollup_to_process.file_name, RollupState::Processing)
            .await
        {
            error!("Failed to mark rollup as processing: {}", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            return;
        };
        let mut leaf_hashes = Vec::new();
        for asset in rollup.rolled_mints.iter() {
            let leaf_hash = match Self::get_leaf_hash(asset, &rollup.tree_id) {
                Ok(leaf_hash) => leaf_hash,
                Err(e) => {
                    if let Err(err) = self
                        .pg_client
                        .mark_rollup_as_verification_failed(
                            &rollup_to_process.file_name,
                            &e.to_string(),
                        )
                        .await
                    {
                        error!("Failed to mark rollup as verification failed: {}", err);
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    return;
                }
            };
            leaf_hashes.push(leaf_hash);
        }

        if let Err(e) = validate_change_logs(
            rollup.max_depth,
            rollup.max_buffer_size,
            &leaf_hashes,
            &rollup,
        ) {
            if let Err(err) = self
                .pg_client
                .mark_rollup_as_verification_failed(&rollup_to_process.file_name, &e.to_string())
                .await
            {
                error!("Failed to mark rollup as verification failed: {}", err);
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    pub async fn process_rollups(&self, mut rx: Receiver<()>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        let rollup_to_process = match self.pg_client.fetch_rollup_for_processing().await {
                            Ok(Some(rollup)) => rollup,
                            Ok(None) => {
                                continue;
                            }
                            Err(e) => {
                                error!("Failed to fetch rollup for processing: {}", e);
                                continue;
                            }
                        };
                        self.process_rollup(rollup_to_process).await;
                    },
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return;
                },
            }
        }
    }

    pub fn get_leaf_hash(
        asset: &RolledMintInstruction,
        tree_id: &Pubkey,
    ) -> Result<[u8; 32], RollupValidationError> {
        let asset_id = get_asset_id(tree_id, asset.leaf_update.nonce());
        if asset_id != asset.leaf_update.id() {
            return Err(RollupValidationError::PDACheckFail(
                asset_id.to_string(),
                asset.leaf_update.id().to_string(),
            ));
        }

        // @dev: seller_fee_basis points is encoded twice so that it can be passed to marketplace
        // instructions, without passing the entire, un-hashed MetadataArgs struct
        let metadata_args_hash = keccak::hashv(&[asset.mint_args.try_to_vec()?.as_slice()]);
        let data_hash = keccak::hashv(&[
            &metadata_args_hash.to_bytes(),
            &asset.mint_args.seller_fee_basis_points.to_le_bytes(),
        ]);
        if asset.leaf_update.data_hash() != data_hash.to_bytes() {
            return Err(RollupValidationError::InvalidDataHash(
                data_hash.to_string(),
                Hash::new(asset.leaf_update.data_hash().as_slice()).to_string(),
            ));
        }

        // Use the metadata auth to check whether we can allow `verified` to be set to true in the
        // creator Vec.
        let creator_data = asset
            .mint_args
            .creators
            .iter()
            .map(|c| [c.address.as_ref(), &[c.verified as u8], &[c.share]].concat())
            .collect::<Vec<_>>();

        // Calculate creator hash.
        let creator_hash = keccak::hashv(
            creator_data
                .iter()
                .map(|c| c.as_slice())
                .collect::<Vec<&[u8]>>()
                .as_ref(),
        );
        if asset.leaf_update.creator_hash() != creator_hash.to_bytes() {
            return Err(RollupValidationError::InvalidCreatorsHash(
                creator_hash.to_string(),
                Hash::new(asset.leaf_update.creator_hash().as_slice()).to_string(),
            ));
        }

        Ok(asset.leaf_update.hash())
    }
}
