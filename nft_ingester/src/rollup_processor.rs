use crate::bubblegum_updates_processor::BubblegumTxProcessor;
use crate::error::{IngesterError, RollupValidationError};
use crate::tree_macros::validate_change_logs;
use anchor_lang::AnchorSerialize;
use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::crypto::base64::Base64;
use arweave_rs::Arweave;
use async_trait::async_trait;
use entities::rollup::{BatchMintInstruction, RolledMintInstruction, Rollup};
use interface::error::UsecaseError;
use interface::rollup::{RollupDownloader, RollupTxSender};
use mpl_bubblegum::utils::get_asset_id;
use postgre_client::model::RollupState;
use postgre_client::PgClient;
use rocks_db::Storage;
use solana_program::keccak;
use solana_program::keccak::Hash;
use solana_program::pubkey::Pubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};

pub struct RollupDownloaderImpl {
    pg_client: Arc<PgClient>,
}
impl RollupDownloaderImpl {
    pub fn new(pg_client: Arc<PgClient>) -> Self {
        Self { pg_client }
    }
}
#[async_trait]
impl RollupDownloader for RollupDownloaderImpl {
    async fn download_rollup(&self, url: &str) -> Result<Box<Rollup>, UsecaseError> {
        let rollup_to_process = self.pg_client.get_rollup_by_url(url).await.ok().flatten();
        if let Some(rollup_to_process) = rollup_to_process {
            if let Ok(Ok(rollup)) = tokio::fs::read_to_string(&rollup_to_process.file_path)
                .await
                .map(|json_file| serde_json::from_str::<Rollup>(&json_file))
            {
                if self
                    .pg_client
                    .update_rollup_state(&rollup_to_process.file_path, RollupState::MovingToStorage)
                    .await
                    .is_ok()
                {
                    return Ok(Box::new(rollup));
                }
            };
        }
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}
pub struct FileRollupDownloader {
    file_path: String,
}
#[async_trait]
impl RollupDownloader for FileRollupDownloader {
    async fn download_rollup(&self, _url: &str) -> Result<Box<Rollup>, UsecaseError> {
        let json_file = std::fs::read_to_string(&self.file_path)
            .map_err(|e| UsecaseError::Storage(e.to_string()))?;
        let rollup: Rollup =
            serde_json::from_str(&json_file).map_err(|e| UsecaseError::Json(e.to_string()))?;
        Ok(Box::new(rollup))
    }
}

pub struct NoopRollupTxSender;
#[async_trait]
impl RollupTxSender for NoopRollupTxSender {
    async fn send_rollup_tx(&self, _instruction: BatchMintInstruction) -> Result<(), UsecaseError> {
        Ok(())
    }
}

pub struct RollupProcessor<R: RollupTxSender> {
    pg_client: Arc<PgClient>,
    rocks: Arc<Storage>,
    arweave: Arc<Arweave>,
    rollup_tx_sender: Arc<R>,
}

impl<R: RollupTxSender> RollupProcessor<R> {
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks: Arc<Storage>,
        rollup_tx_sender: Arc<R>,
        arweave_wallet_path: &str,
    ) -> Self {
        let arweave = Arc::new(
            Arweave::from_keypair_path(
                PathBuf::from_str(arweave_wallet_path).unwrap(),
                ARWEAVE_BASE_URL.parse().unwrap(),
            )
            .unwrap(),
        );
        Self {
            pg_client,
            rocks,
            arweave,
            rollup_tx_sender,
        }
    }

    pub async fn process_rollups(&self, rx: Receiver<()>) {
        'out: while rx.is_empty() {
            let rollup_to_process = match self
                .pg_client
                .fetch_rollup_for_processing(RollupState::Uploaded)
                .await
            {
                Ok(Some(rollup)) => rollup,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                Err(e) => {
                    error!("Failed to fetch rollup for processing: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            info!("Processing {} rollup file", &rollup_to_process.file_path);
            let json_file = match tokio::fs::read_to_string(&rollup_to_process.file_path).await {
                Ok(json_file) => json_file,
                Err(e) => {
                    error!(
                        "Failed to read file to string: file_path: {}, error: {}",
                        &rollup_to_process.file_path, e
                    );
                    continue;
                }
            };
            let rollup = match serde_json::from_str::<Rollup>(&json_file) {
                Ok(rollup) => rollup,
                Err(e) => {
                    if let Err(e) = self
                        .pg_client
                        .mark_rollup_as_failed(
                            &rollup_to_process.file_path,
                            &e.to_string(),
                            RollupState::ValidationFail,
                        )
                        .await
                    {
                        error!("Failed to mark rollup as verification failed: file_path: {}, error: {}",
                        &rollup_to_process.file_path, e);
                    }
                    continue;
                }
            };
            if let Err(e) = self
                .pg_client
                .update_rollup_state(&rollup_to_process.file_path, RollupState::Processing)
                .await
            {
                error!(
                    "Failed to mark rollup as processing: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            };
            let mut leaf_hashes = Vec::new();
            for asset in rollup.rolled_mints.iter() {
                let leaf_hash = match Self::get_leaf_hash(asset, &rollup.tree_id) {
                    Ok(leaf_hash) => leaf_hash,
                    Err(e) => {
                        if let Err(err) = self
                            .pg_client
                            .mark_rollup_as_failed(
                                &rollup_to_process.file_path,
                                &e.to_string(),
                                RollupState::ValidationFail,
                            )
                            .await
                        {
                            error!("Failed to mark rollup as verification failed: file_path: {}, error: {}",
                        &rollup_to_process.file_path, err);
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        continue 'out;
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
                    .mark_rollup_as_failed(
                        &rollup_to_process.file_path,
                        &e.to_string(),
                        RollupState::ValidationFail,
                    )
                    .await
                {
                    error!(
                        "Failed to mark rollup as verification failed: file_path: {}, error: {}",
                        &rollup_to_process.file_path, err
                    );
                }
                continue 'out;
            }
            let (tx_id, reward) = match self
                .upload_to_arweave(&rollup_to_process.file_path, json_file.len())
                .await
            {
                Ok((tx_id, reward)) => (tx_id, reward),
                Err(e) => {
                    if let Err(err) = self
                        .pg_client
                        .mark_rollup_as_failed(
                            &rollup_to_process.file_path,
                            &format!("upload to arweave: {}", e),
                            RollupState::FailSendingTransaction,
                        )
                        .await
                    {
                        error!(
                            "Failed to mark rollup as failed: file_path: {}, error: {}",
                            &rollup_to_process.file_path, err
                        );
                    }
                    error!(
                        "Failed to upload rollup to arweave: file_path: {}, error: {}",
                        &rollup_to_process.file_path, e
                    );
                    continue 'out;
                }
            };
            let metadata_url = Self::build_metadata_url(&tx_id);
            if let Err(e) = self
                .pg_client
                .set_rollup_url_and_reward(
                    &rollup_to_process.file_path,
                    &metadata_url,
                    reward as i64,
                )
                .await
            {
                // Do not continue, because we already upload data to arweave, so move on
                error!(
                    "Failed to set rollup url and reward: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
            }
            if let Err(e) = self
                .rollup_tx_sender
                .send_rollup_tx(BatchMintInstruction {
                    max_depth: rollup.max_depth,
                    max_buffer_size: rollup.max_buffer_size,
                    num_minted: rollup.rolled_mints.len() as u64,
                    root: rollup.merkle_root,
                    leaf: rollup.last_leaf_hash,
                    index: rollup.rolled_mints.len().saturating_sub(1) as u32, // TODO
                    metadata_url,
                })
                .await
            {
                if let Err(err) = self
                    .pg_client
                    .mark_rollup_as_failed(
                        &rollup_to_process.file_path,
                        &format!("send rollup tx: {}", e),
                        RollupState::FailSendingTransaction,
                    )
                    .await
                {
                    error!(
                        "Failed to mark rollup as failed: file_path: {}, error: {}",
                        &rollup_to_process.file_path, err
                    );
                }
                error!(
                    "Failed to upload rollup to arweave: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
                continue 'out;
            };
            if let Err(e) = self
                .pg_client
                .update_rollup_state(&rollup_to_process.file_path, RollupState::TransactionSent)
                .await
            {
                error!(
                    "Failed to mark rollup as transaction sent: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
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

    async fn upload_to_arweave(
        &self,
        file_path: &str,
        data_size: usize,
    ) -> Result<(String, u64), IngesterError> {
        let file_path = PathBuf::from_str(file_path)?;
        let fee = self.arweave.get_fee(Base64::empty(), data_size).await?;
        self.arweave
            .upload_file_from_path(file_path, vec![], fee)
            .await
            .map_err(Into::into)
    }

    fn build_metadata_url(transaction_id: &str) -> String {
        format!("{}{}", ARWEAVE_BASE_URL, transaction_id)
    }

    pub async fn move_rollups_to_storage(&self, rx: Receiver<()>) {
        while rx.is_empty() {
            let rollup_to_process = match self
                .pg_client
                .fetch_rollup_for_processing(RollupState::TransactionSent)
                .await
            {
                Ok(Some(rollup)) => rollup,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                Err(e) => {
                    error!("Failed to fetch rollup for moving to storage: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            info!(
                "Moving to storage {} rollup file",
                &rollup_to_process.file_path
            );
            if let Err(e) = self
                .pg_client
                .update_rollup_state(&rollup_to_process.file_path, RollupState::MovingToStorage)
                .await
            {
                error!(
                    "Failed to mark rollup as processing: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            };
            if let Err(e) = BubblegumTxProcessor::store_rollup_update(
                1, // slot don`t really matter for first iteration // TODO
                &BatchMintInstruction::default(),
                FileRollupDownloader {
                    file_path: rollup_to_process.file_path.clone(),
                },
                self.rocks.clone(),
            )
            .await
            {
                error!(
                    "Failed moving rollup to storage: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            };
            if let Err(e) = self
                .pg_client
                .update_rollup_state(&rollup_to_process.file_path, RollupState::Complete)
                .await
            {
                error!(
                    "Failed to mark rollup as complete: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
            };
            if let Err(e) = tokio::fs::remove_file(&rollup_to_process.file_path).await {
                error!(
                    "Failed to remove file: file_path: {}, error: {}",
                    &rollup_to_process.file_path, e
                );
            }
        }
    }
}
