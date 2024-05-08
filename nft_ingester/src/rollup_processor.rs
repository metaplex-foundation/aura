use crate::error::{IngesterError, RollupValidationError};
use crate::tree_macros::validate_change_logs;
use anchor_lang::AnchorSerialize;
use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::crypto::base64::Base64;
use arweave_rs::Arweave;
use async_trait::async_trait;
use entities::models::RollupWithState;
use entities::rollup::{BatchMintInstruction, RolledMintInstruction, Rollup};
use interface::error::UsecaseError;
use interface::rollup::{RollupDownloader, RollupTxSender};
use mpl_bubblegum::utils::get_asset_id;
use postgre_client::model::RollupState;
use postgre_client::PgClient;
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
    file_storage_path: String,
}
impl RollupDownloaderImpl {
    pub fn new(pg_client: Arc<PgClient>, file_storage_path: String) -> Self {
        Self {
            pg_client,
            file_storage_path,
        }
    }
}
#[async_trait]
impl RollupDownloader for RollupDownloaderImpl {
    async fn download_rollup(&self, url: &str) -> Result<Box<Rollup>, UsecaseError> {
        let rollup_to_process = self.pg_client.get_rollup_by_url(url).await.ok().flatten();
        if let Some(rollup_to_process) = rollup_to_process {
            if let Ok(Ok(rollup)) = tokio::fs::read_to_string(format!(
                "{}/{}",
                self.file_storage_path, &rollup_to_process.file_name
            ))
            .await
            .map(|json_file| serde_json::from_str::<Rollup>(&json_file))
            {
                if self
                    .pg_client
                    .update_rollup_state(&rollup_to_process.file_name, RollupState::MovingToStorage)
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

#[async_trait]
pub trait PermanentStorageClient {
    async fn upload_file(
        &self,
        file_path: &str,
        data_size: usize,
    ) -> Result<(String, u64), IngesterError>;
    fn get_metadata_url(&self, transaction_id: &str) -> String;
}

#[async_trait]
impl PermanentStorageClient for Arweave {
    async fn upload_file(
        &self,
        file_path: &str,
        data_size: usize,
    ) -> Result<(String, u64), IngesterError> {
        let file_path = PathBuf::from_str(file_path)?;
        let fee = self.get_fee(Base64::empty(), data_size).await?;
        self.upload_file_from_path(file_path, vec![], fee)
            .await
            .map_err(Into::into)
    }
    fn get_metadata_url(&self, transaction_id: &str) -> String {
        format!("{}{}", ARWEAVE_BASE_URL, transaction_id)
    }
}

pub struct NoopRollupTxSender;
#[async_trait]
impl RollupTxSender for NoopRollupTxSender {
    async fn send_rollup_tx(&self, _instruction: BatchMintInstruction) -> Result<(), UsecaseError> {
        Ok(())
    }
}

pub struct RollupProcessor<R: RollupTxSender, P: PermanentStorageClient> {
    pg_client: Arc<PgClient>,
    permanent_storage_client: Arc<P>,
    rollup_tx_sender: Arc<R>,
    file_storage_path: String,
}

impl<R: RollupTxSender, P: PermanentStorageClient> RollupProcessor<R, P> {
    pub fn new(
        pg_client: Arc<PgClient>,
        rollup_tx_sender: Arc<R>,
        permanent_storage_client: Arc<P>,
        file_storage_path: String,
    ) -> Self {
        Self {
            pg_client,
            permanent_storage_client,
            rollup_tx_sender,
            file_storage_path,
        }
    }

    pub async fn process_rollups(&self, rx: Receiver<()>) {
        while rx.is_empty() {
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
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            self.process_rollup(rollup_to_process).await;
        }
    }

    pub async fn process_rollup(&self, rollup_to_process: RollupWithState) {
        info!("Processing {} rollup file", &rollup_to_process.file_name);
        let Ok((rollup, file_size)) = self.read_rollup_file(&rollup_to_process).await else {
            return;
        };
        if let Err(e) = self
            .pg_client
            .update_rollup_state(&rollup_to_process.file_name, RollupState::Processing)
            .await
        {
            error!(
                "Failed to mark rollup as processing: file_path: {}, error: {}",
                &rollup_to_process.file_name, e
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
            return;
        };

        if let Err(e) = self.validate_rollup(&rollup).await {
            if let Err(err) = self
                .pg_client
                .mark_rollup_as_failed(
                    &rollup_to_process.file_name,
                    &e.to_string(),
                    RollupState::ValidationFail,
                )
                .await
            {
                error!(
                    "Failed to mark rollup as verification failed: file_path: {}, error: {}",
                    &rollup_to_process.file_name, err
                );
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            return;
        }
        self.send_rollup_transactions(&rollup, &rollup_to_process, file_size)
            .await;
        if let Err(e) = self
            .pg_client
            .update_rollup_state(&rollup_to_process.file_name, RollupState::TransactionSent)
            .await
        {
            error!(
                "Failed to mark rollup as transaction sent: file_path: {}, error: {}",
                &rollup_to_process.file_name, e
            );
        }
    }

    async fn read_rollup_file(
        &self,
        rollup_to_process: &RollupWithState,
    ) -> Result<(Rollup, usize), IngesterError> {
        let json_file = match tokio::fs::read_to_string(format!(
            "{}/{}",
            self.file_storage_path, &rollup_to_process.file_name
        ))
        .await
        {
            Ok(json_file) => json_file,
            Err(e) => {
                error!(
                    "Failed to read file to string: file_path: {}, error: {}",
                    &rollup_to_process.file_name, e
                );
                return Err(e.into());
            }
        };
        let rollup = match serde_json::from_str::<Rollup>(&json_file) {
            Ok(rollup) => rollup,
            Err(e) => {
                if let Err(e) = self
                    .pg_client
                    .mark_rollup_as_failed(
                        &rollup_to_process.file_name,
                        &e.to_string(),
                        RollupState::ValidationFail,
                    )
                    .await
                {
                    error!(
                        "Failed to mark rollup as verification failed: file_path: {}, error: {}",
                        &rollup_to_process.file_name, e
                    );
                }
                return Err(e.into());
            }
        };

        Ok((rollup, json_file.len()))
    }

    async fn send_rollup_transactions(
        &self,
        rollup: &Rollup,
        rollup_to_process: &RollupWithState,
        file_size: usize,
    ) {
        let (tx_id, reward) = match self
            .permanent_storage_client
            .upload_file(&rollup_to_process.file_name, file_size)
            .await
        {
            Ok((tx_id, reward)) => (tx_id, reward),
            Err(e) => {
                if let Err(err) = self
                    .pg_client
                    .mark_rollup_as_failed(
                        &rollup_to_process.file_name,
                        &format!("upload to arweave: {}", e),
                        RollupState::FailSendingTransaction,
                    )
                    .await
                {
                    error!(
                        "Failed to mark rollup as failed: file_path: {}, error: {}",
                        &rollup_to_process.file_name, err
                    );
                }
                error!(
                    "Failed to upload rollup to arweave: file_path: {}, error: {}",
                    &rollup_to_process.file_name, e
                );
                return;
            }
        };
        let metadata_url = self.permanent_storage_client.get_metadata_url(&tx_id);
        if let Err(e) = self
            .pg_client
            .set_rollup_url_and_reward(&rollup_to_process.file_name, &metadata_url, reward as i64)
            .await
        {
            // Do not continue, because we already upload data to arweave, so move on
            error!(
                "Failed to set rollup url and reward: file_path: {}, error: {}",
                &rollup_to_process.file_name, e
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
                    &rollup_to_process.file_name,
                    &format!("send rollup tx: {}", e),
                    RollupState::FailSendingTransaction,
                )
                .await
            {
                error!(
                    "Failed to mark rollup as failed: file_path: {}, error: {}",
                    &rollup_to_process.file_name, err
                );
            }
            error!(
                "Failed to upload rollup to arweave: file_path: {}, error: {}",
                &rollup_to_process.file_name, e
            );
        };
    }

    async fn validate_rollup(&self, rollup: &Rollup) -> Result<(), RollupValidationError> {
        let mut leaf_hashes = Vec::new();
        for asset in rollup.rolled_mints.iter() {
            let leaf_hash = match Self::get_leaf_hash(asset, &rollup.tree_id) {
                Ok(leaf_hash) => leaf_hash,
                Err(e) => {
                    return Err(e);
                }
            };
            leaf_hashes.push(leaf_hash);
        }

        validate_change_logs(
            rollup.max_depth,
            rollup.max_buffer_size,
            &leaf_hashes,
            rollup,
        )
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
