use crate::error::IngesterError;
use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::crypto::base64::Base64;
use arweave_rs::Arweave;
use async_trait::async_trait;
use bubblegum_batch_sdk::model::{BatchMint, BatchMintInstruction};
use entities::models::BatchMintWithState;
use interface::batch_mint::{BatchMintDownloader, BatchMintTxSender};
use interface::error::UsecaseError;
use metrics_utils::RollupProcessorMetricsConfig;
use postgre_client::model::RollupState;
use postgre_client::PgClient;
use rocks_db::batch_mint::BatchMintWithStaker;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::Instant;
use tracing::{error, info};

pub const MAX_ROLLUP_RETRIES: usize = 5;
const SUCCESS_METRICS_LABEL: &str = "success";
const VALIDATION_FAIL_METRICS_LABEL: &str = "validation_fail";
const FAIL_READ_FILE_METRICS_LABEL: &str = "fail_read_file";
const FAIL_BUILD_JSON_FROM_FILE_METRICS_LABEL: &str = "fail_build_json_from_file";
const TRANSACTION_FAIL_METRICS_LABEL: &str = "transaction_fail";
const ARWEAVE_UPLOAD_FAIL_METRICS_LABEL: &str = "arweave_upload_fail";
const FILE_PROCESSING_METRICS_LABEL: &str = "rollup_file_processing";

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
impl BatchMintDownloader for RollupDownloaderImpl {
    async fn download_batch_mint(&self, url: &str) -> Result<Box<BatchMint>, UsecaseError> {
        // TODO: normalize url
        let rollup_to_process = self.pg_client.get_rollup_by_url(url).await.ok().flatten();
        if let Some(rollup_to_process) = rollup_to_process {
            if let Ok(Ok(rollup)) = tokio::fs::read_to_string(format!(
                "{}/{}",
                self.file_storage_path, &rollup_to_process.file_name
            ))
            .await
            .map(|json_file| serde_json::from_str::<BatchMint>(&json_file))
            {
                return Ok(Box::new(rollup));
            };
        }
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }

    async fn download_batch_mint_and_check_checksum(
        &self,
        url: &str,
        checksum: &str,
    ) -> Result<Box<BatchMint>, UsecaseError> {
        let rollup_to_process = self.pg_client.get_rollup_by_url(url).await.ok().flatten();
        if let Some(rollup_to_process) = rollup_to_process {
            if let Ok(Ok(rollup)) = tokio::fs::read(format!(
                "{}/{}",
                self.file_storage_path, &rollup_to_process.file_name
            ))
            .await
            .map(|json_file| {
                let file_hash = xxhash_rust::xxh3::xxh3_128(&json_file);
                let hash_hex = hex::encode(file_hash.to_be_bytes());
                if hash_hex != checksum {
                    return Err(UsecaseError::InvalidParameters(
                        "File checksum mismatch".to_string(),
                    ));
                }
                serde_json::from_slice::<BatchMint>(&json_file)
                    .map_err(|e| UsecaseError::Storage(e.to_string()))
            }) {
                return Ok(Box::new(rollup));
            };
        }
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

#[async_trait]
#[mockall::automock]
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
impl BatchMintTxSender for NoopRollupTxSender {
    async fn send_batch_mint_tx(
        &self,
        _instruction: BatchMintInstruction,
    ) -> Result<(), UsecaseError> {
        Ok(())
    }
}

pub struct RollupProcessor<R: BatchMintTxSender, P: PermanentStorageClient> {
    pg_client: Arc<PgClient>,
    rocks: Arc<Storage>,
    permanent_storage_client: Arc<P>,
    _rollup_tx_sender: Arc<R>,
    file_storage_path: String,
    metrics: Arc<RollupProcessorMetricsConfig>,
}

impl<R: BatchMintTxSender, P: PermanentStorageClient> RollupProcessor<R, P> {
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks: Arc<Storage>,
        rollup_tx_sender: Arc<R>,
        permanent_storage_client: Arc<P>,
        file_storage_path: String,
        metrics: Arc<RollupProcessorMetricsConfig>,
    ) -> Self {
        Self {
            pg_client,
            rocks,
            permanent_storage_client,
            _rollup_tx_sender: rollup_tx_sender,
            file_storage_path,
            metrics,
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
        mut rollup_to_process: BatchMintWithState,
    ) -> Result<(), IngesterError> {
        info!("Processing {} rollup file", &rollup_to_process.file_name);
        let start_time = Instant::now();
        let (rollup, file_size, file_checksum) = self.read_rollup_file(&rollup_to_process).await?;
        let mut metadata_url = String::new();
        while rx.is_empty() {
            match &rollup_to_process.state {
                entities::enums::BatchMintState::Uploaded => {
                    self.process_rollup_validation(&rollup, &mut rollup_to_process)
                        .await?;
                }
                entities::enums::BatchMintState::ValidationComplete
                | entities::enums::BatchMintState::FailUploadToArweave => {
                    metadata_url = self
                        .process_rollup_upload_to_arweave(&mut rollup_to_process, file_size)
                        .await?;
                }
                entities::enums::BatchMintState::UploadedToArweave
                | entities::enums::BatchMintState::FailSendingTransaction => {
                    self.process_rollup_send_solana_tx(
                        &mut rollup_to_process,
                        &metadata_url,
                        &rollup,
                        &file_checksum,
                    )
                    .await?;
                }
                _ => {
                    info!(
                        "Finish processing {} rollup file with {:?} state",
                        &rollup_to_process.file_name, &rollup_to_process.state
                    );
                    self.metrics.inc_total_rollups(SUCCESS_METRICS_LABEL);
                    self.metrics.set_processing_latency(
                        FILE_PROCESSING_METRICS_LABEL,
                        start_time.elapsed().as_millis() as f64,
                    );
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn process_rollup_validation(
        &self,
        batch_mint: &BatchMint,
        batch_mint_to_process: &mut BatchMintWithState,
    ) -> Result<(), IngesterError> {
        // TODO: add possibility to set collection and send FinilizeTreeAndCollection tx
        if let Err(e) =
            bubblegum_batch_sdk::batch_mint_validations::validate_batch_mint(batch_mint, None).await
        {
            if let Err(err) = self
                .pg_client
                .mark_rollup_as_failed(
                    &batch_mint_to_process.file_name,
                    &e.to_string(),
                    RollupState::ValidationFail,
                )
                .await
            {
                error!(
                    "Failed to mark rollup as verification failed: file_path: {}, error: {}",
                    &batch_mint_to_process.file_name, err
                );
            }
            self.metrics
                .inc_total_rollups(VALIDATION_FAIL_METRICS_LABEL);
            return Err(e.into());
        }
        if let Err(err) = self
            .pg_client
            .update_rollup_state(
                &batch_mint_to_process.file_name,
                RollupState::ValidationComplete,
            )
            .await
        {
            error!(
                "Failed to mark rollup as verification complete: file_path: {}, error: {}",
                &batch_mint_to_process.file_name, err
            );
        };
        batch_mint_to_process.state = entities::enums::BatchMintState::ValidationComplete;
        Ok(())
    }

    async fn process_rollup_upload_to_arweave(
        &self,
        rollup_to_process: &mut BatchMintWithState,
        file_size: usize,
    ) -> Result<String, IngesterError> {
        let (tx_id, reward) = match self
            .upload_file_with_retry(&rollup_to_process.file_name, file_size)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                self.metrics
                    .inc_total_rollups(ARWEAVE_UPLOAD_FAIL_METRICS_LABEL);
                error!(
                    "Failed upload file to arweave: file_path: {}, error: {}",
                    &rollup_to_process.file_name, e
                );
                return Err(e);
            }
        };
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
        rollup_to_process.state = entities::enums::BatchMintState::UploadedToArweave;
        Ok(metadata_url)
    }

    async fn process_rollup_send_solana_tx(
        &self,
        rollup_to_process: &mut BatchMintWithState,
        metadata_url: &str,
        rollup: &BatchMint,
        file_checksum: &str,
    ) -> Result<(), IngesterError> {
        if let Err(e) = self
            .send_rollup_tx_with_retry(
                &rollup_to_process.file_name,
                rollup,
                metadata_url,
                file_checksum,
            )
            .await
        {
            self.metrics
                .inc_total_rollups(TRANSACTION_FAIL_METRICS_LABEL);
            error!(
                "Failed send solana transaction: file_path: {}, error: {}",
                &rollup_to_process.file_name, e
            );
            return Err(e);
        };
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
        if let Err(err) = self
            .rocks
            .rollups
            .put_async(
                file_checksum.to_string(),
                BatchMintWithStaker {
                    batch_mint: rollup.clone(),
                    staker: Pubkey::default(), // TODO: here we must set our own pubkey
                },
            )
            .await
        {
            error!(
                "Failed to save rollup into rocks: file_checksum: {}, file_path: {}, error: {}",
                file_checksum, &rollup_to_process.file_name, err
            );
        }
        rollup_to_process.state = entities::enums::BatchMintState::Complete;
        Ok(())
    }

    async fn read_rollup_file(
        &self,
        rollup_to_process: &BatchMintWithState,
    ) -> Result<(BatchMint, usize, String), IngesterError> {
        let json_file = match tokio::fs::read(format!(
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
                self.metrics.inc_total_rollups(FAIL_READ_FILE_METRICS_LABEL);
                return Err(e.into());
            }
        };
        let rollup = match serde_json::from_slice::<BatchMint>(&json_file) {
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
                self.metrics
                    .inc_total_rollups(FAIL_BUILD_JSON_FROM_FILE_METRICS_LABEL);
                return Err(e.into());
            }
        };
        let file_hash = xxhash_rust::xxh3::xxh3_128(&json_file);
        let hash_hex = hex::encode(file_hash.to_be_bytes());

        Ok((rollup, json_file.len(), hash_hex))
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
        _file_name: &str,
        _rollup: &BatchMint,
        _metadata_url: &str,
        _file_checksum: &str,
    ) -> Result<(), IngesterError> {
        // let mut last_error = UsecaseError::Storage("".to_string());

        // for _ in 0..MAX_ROLLUP_RETRIES {
        // match self
        //     .rollup_tx_sender
        //     .send_batch_mint_tx(instruction.clone())
        //     .await
        // {
        //     Ok(_) => return Ok(()),
        //     Err(e) => {
        //         tokio::time::sleep(Duration::from_millis(500)).await;
        //         last_error = e;
        //     }
        // }
        // }
        // if let Err(err) = self
        //     .pg_client
        //     .mark_rollup_as_failed(
        //         file_name,
        //         &format!("send rollup tx: {}", last_error),
        //         RollupState::FailSendingTransaction,
        //     )
        //     .await
        // {
        //     error!(
        //         "Failed to mark rollup as failed: file_path: {}, error: {}",
        //         &file_name, err
        //     );
        // }
        // error!(
        //     "Failed to send rollup transaction: file_path: {}, error: {}",
        //     &file_name, last_error
        // );
        //
        // Err(IngesterError::SendTransaction(last_error.to_string()))
        Ok(())
    }
}
