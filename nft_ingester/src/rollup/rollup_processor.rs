use crate::error::IngesterError;
use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::crypto::base64::Base64;
use arweave_rs::Arweave;
use async_trait::async_trait;
use entities::models::RollupWithState;
use entities::rollup::{BatchMintInstruction, Rollup};
use interface::error::UsecaseError;
use interface::rollup::{RollupDownloader, RollupTxSender};
use mockall::automock;
use postgre_client::model::RollupState;
use postgre_client::PgClient;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};

use super::rollup_verifier::RollupVerifier;

pub const MAX_ROLLUP_RETRIES: usize = 5;

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
        // TODO: normalize url
        let rollup_to_process = self.pg_client.get_rollup_by_url(url).await.ok().flatten();
        if let Some(rollup_to_process) = rollup_to_process {
            if let Ok(Ok(rollup)) = tokio::fs::read_to_string(format!(
                "{}/{}",
                self.file_storage_path, &rollup_to_process.file_name
            ))
            .await
            .map(|json_file| serde_json::from_str::<Rollup>(&json_file))
            {
                return Ok(Box::new(rollup));
            };
        }
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}

#[async_trait]
#[automock]
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
    rollup_verifier: RollupVerifier,
    file_storage_path: String,
}

impl<R: RollupTxSender, P: PermanentStorageClient> RollupProcessor<R, P> {
    pub fn new(
        pg_client: Arc<PgClient>,
        rollup_tx_sender: Arc<R>,
        rollup_verifier: RollupVerifier,
        permanent_storage_client: Arc<P>,
        file_storage_path: String,
    ) -> Self {
        Self {
            pg_client,
            permanent_storage_client,
            rollup_tx_sender,
            rollup_verifier,
            file_storage_path,
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
                        let _ = self.process_rollup(rx.resubscribe(),rollup_to_process).await;
                    },
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return;
                },
            }
        }
    }

    pub async fn process_rollup(
        &self,
        rx: Receiver<()>,
        mut rollup_to_process: RollupWithState,
    ) -> Result<(), IngesterError> {
        info!("Processing {} rollup file", &rollup_to_process.file_name);
        let (rollup, file_size) = self.read_rollup_file(&rollup_to_process).await?;
        let mut metadata_url = String::new();
        while rx.is_empty() {
            match &rollup_to_process.state {
                entities::enums::RollupState::Uploaded => {
                    self.process_rollup_validation(&rollup, &mut rollup_to_process)
                        .await?;
                }
                entities::enums::RollupState::ValidationComplete
                | entities::enums::RollupState::FailUploadToArweave => {
                    metadata_url = self
                        .process_rollup_upload_to_arweave(&mut rollup_to_process, file_size)
                        .await?;
                }
                entities::enums::RollupState::UploadedToArweave
                | entities::enums::RollupState::FailSendingTransaction => {
                    self.process_rollup_send_solana_tx(
                        &mut rollup_to_process,
                        &metadata_url,
                        &rollup,
                    )
                    .await?;
                }
                _ => {
                    info!(
                        "Finish processing {} rollup file with {:?} state",
                        &rollup_to_process.file_name, &rollup_to_process.state
                    );
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn process_rollup_validation(
        &self,
        rollup: &Rollup,
        rollup_to_process: &mut RollupWithState,
    ) -> Result<(), IngesterError> {
        if let Err(e) = self.rollup_verifier.validate_rollup(rollup).await {
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
            return Err(e.into());
        }
        if let Err(err) = self
            .pg_client
            .update_rollup_state(
                &rollup_to_process.file_name,
                RollupState::ValidationComplete,
            )
            .await
        {
            error!(
                "Failed to mark rollup as verification complete: file_path: {}, error: {}",
                &rollup_to_process.file_name, err
            );
        };
        rollup_to_process.state = entities::enums::RollupState::ValidationComplete;
        Ok(())
    }

    async fn process_rollup_upload_to_arweave(
        &self,
        rollup_to_process: &mut RollupWithState,
        file_size: usize,
    ) -> Result<String, IngesterError> {
        let (tx_id, reward) = self
            .upload_file_with_retry(&rollup_to_process.file_name, file_size)
            .await?;
        let metadata_url = self.permanent_storage_client.get_metadata_url(&tx_id);
        if let Err(e) = self
            .pg_client
            .set_rollup_url_and_reward(&rollup_to_process.file_name, &metadata_url, reward as i64)
            .await
        {
            error!(
                "Failed to set rollup url and reward: file_path: {}, error: {}",
                &rollup_to_process.file_name, e
            );
        }
        rollup_to_process.state = entities::enums::RollupState::UploadedToArweave;
        Ok(metadata_url)
    }

    async fn process_rollup_send_solana_tx(
        &self,
        rollup_to_process: &mut RollupWithState,
        metadata_url: &str,
        rollup: &Rollup,
    ) -> Result<(), IngesterError> {
        self.send_rollup_tx_with_retry(&rollup_to_process.file_name, rollup, metadata_url)
            .await?;
        if let Err(err) = self
            .pg_client
            .update_rollup_state(&rollup_to_process.file_name, RollupState::Complete)
            .await
        {
            error!(
                "Failed to mark rollup as verification complete: file_path: {}, error: {}",
                &rollup_to_process.file_name, err
            );
        };
        rollup_to_process.state = entities::enums::RollupState::Complete;
        Ok(())
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

    async fn upload_file_with_retry(
        &self,
        file_name: &str,
        file_size: usize,
    ) -> Result<(String, u64), IngesterError> {
        let file_path = &format!("{}/{}", self.file_storage_path, file_name);
        let mut last_error = IngesterError::Arweave("".to_string());
        for _ in 0..MAX_ROLLUP_RETRIES {
            match self
                .permanent_storage_client
                .upload_file(file_path, file_size)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = e;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            };
        }
        if let Err(err) = self
            .pg_client
            .mark_rollup_as_failed(
                file_name,
                &format!("upload to arweave: {}", last_error),
                RollupState::FailUploadToArweave,
            )
            .await
        {
            error!(
                "Failed to mark rollup as failed: file_path: {}, error: {}",
                &file_name, err
            );
        };
        error!(
            "Failed to upload rollup to arweave: file_path: {}, error: {}",
            &file_name, last_error
        );

        Err(IngesterError::Arweave(last_error.to_string()))
    }

    async fn send_rollup_tx_with_retry(
        &self,
        file_name: &str,
        rollup: &Rollup,
        metadata_url: &str,
    ) -> Result<(), IngesterError> {
        let mut last_error = UsecaseError::Storage("".to_string());
        let instruction = BatchMintInstruction {
            max_depth: rollup.max_depth,
            max_buffer_size: rollup.max_buffer_size,
            num_minted: rollup.rolled_mints.len() as u64,
            root: rollup.merkle_root,
            leaf: rollup.last_leaf_hash,
            index: rollup.rolled_mints.len().saturating_sub(1) as u32, // TODO
            metadata_url: metadata_url.to_string(),
        };

        for _ in 0..MAX_ROLLUP_RETRIES {
            match self
                .rollup_tx_sender
                .send_rollup_tx(instruction.clone())
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    last_error = e;
                }
            }
        }
        if let Err(err) = self
            .pg_client
            .mark_rollup_as_failed(
                file_name,
                &format!("send rollup tx: {}", last_error),
                RollupState::FailSendingTransaction,
            )
            .await
        {
            error!(
                "Failed to mark rollup as failed: file_path: {}, error: {}",
                &file_name, err
            );
        }
        error!(
            "Failed to send rollup transaction: file_path: {}, error: {}",
            &file_name, last_error
        );

        Err(IngesterError::SendTransaction(last_error.to_string()))
    }
}
