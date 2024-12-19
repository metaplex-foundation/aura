use crate::error::IngesterError;
use arweave_rs::consts::ARWEAVE_BASE_URL;
use arweave_rs::crypto::base64::Base64;
use arweave_rs::Arweave;
use async_trait::async_trait;
use bubblegum_batch_sdk::model::{BatchMint, BatchMintInstruction};
use entities::models::BatchMintWithState;
use interface::batch_mint::{BatchMintDownloader, BatchMintTxSender};
use interface::error::UsecaseError;
use metrics_utils::BatchMintProcessorMetricsConfig;
use postgre_client::model::BatchMintState;
use postgre_client::PgClient;
use rocks_db::columns::batch_mint::BatchMintWithStaker;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinError;
use tokio::time::Instant;
use tracing::{error, info};

pub const MAX_BATCH_MINT_RETRIES: usize = 5;
const SUCCESS_METRICS_LABEL: &str = "success";
const VALIDATION_FAIL_METRICS_LABEL: &str = "validation_fail";
const FAIL_READ_FILE_METRICS_LABEL: &str = "fail_read_file";
const FAIL_BUILD_JSON_FROM_FILE_METRICS_LABEL: &str = "fail_build_json_from_file";
const TRANSACTION_FAIL_METRICS_LABEL: &str = "transaction_fail";
const ARWEAVE_UPLOAD_FAIL_METRICS_LABEL: &str = "arweave_upload_fail";
const FILE_PROCESSING_METRICS_LABEL: &str = "batch_mint_file_processing";

pub async fn process_batch_mints<R: BatchMintTxSender, P: PermanentStorageClient>(
    processor_clone: Arc<BatchMintProcessor<R, P>>,
    rx: Receiver<()>,
) -> Result<(), JoinError> {
    info!("Start processing batch_mints...");
    processor_clone.process_batch_mints(rx).await;
    info!("Finish processing batch_mints...");

    Ok(())
}

pub struct BatchMintDownloaderImpl {
    pg_client: Arc<PgClient>,
    file_storage_path: String,
}
impl BatchMintDownloaderImpl {
    pub fn new(pg_client: Arc<PgClient>, file_storage_path: String) -> Self {
        Self {
            pg_client,
            file_storage_path,
        }
    }
}
#[async_trait]
impl BatchMintDownloader for BatchMintDownloaderImpl {
    async fn download_batch_mint(&self, url: &str) -> Result<Box<BatchMint>, UsecaseError> {
        // TODO: normalize url
        let batch_mint_to_process = self
            .pg_client
            .get_batch_mint_by_url(url)
            .await
            .ok()
            .flatten();
        if let Some(batch_mint_to_process) = batch_mint_to_process {
            if let Ok(Ok(batch_mint)) = tokio::fs::read_to_string(format!(
                "{}/{}",
                self.file_storage_path, &batch_mint_to_process.file_name
            ))
            .await
            .map(|json_file| serde_json::from_str::<BatchMint>(&json_file))
            {
                return Ok(Box::new(batch_mint));
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
        let batch_mint_to_process = self
            .pg_client
            .get_batch_mint_by_url(url)
            .await
            .ok()
            .flatten();
        if let Some(batch_mint_to_process) = batch_mint_to_process {
            if let Ok(Ok(batch_mint)) = tokio::fs::read(format!(
                "{}/{}",
                self.file_storage_path, &batch_mint_to_process.file_name
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
                return Ok(Box::new(batch_mint));
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

pub struct NoopBatchMintTxSender;
#[async_trait]
impl BatchMintTxSender for NoopBatchMintTxSender {
    async fn send_batch_mint_tx(
        &self,
        _instruction: BatchMintInstruction,
    ) -> Result<(), UsecaseError> {
        Ok(())
    }
}

pub struct BatchMintProcessor<R: BatchMintTxSender, P: PermanentStorageClient> {
    pg_client: Arc<PgClient>,
    rocks: Arc<Storage>,
    permanent_storage_client: Arc<P>,
    _batch_mint_tx_sender: Arc<R>,
    file_storage_path: String,
    metrics: Arc<BatchMintProcessorMetricsConfig>,
}

impl<R: BatchMintTxSender, P: PermanentStorageClient> BatchMintProcessor<R, P> {
    pub fn new(
        pg_client: Arc<PgClient>,
        rocks: Arc<Storage>,
        batch_mint_tx_sender: Arc<R>,
        permanent_storage_client: Arc<P>,
        file_storage_path: String,
        metrics: Arc<BatchMintProcessorMetricsConfig>,
    ) -> Self {
        Self {
            pg_client,
            rocks,
            permanent_storage_client,
            _batch_mint_tx_sender: batch_mint_tx_sender,
            file_storage_path,
            metrics,
        }
    }

    pub async fn process_batch_mints(&self, mut rx: Receiver<()>) {
        while rx.is_empty() {
            let batch_mint_to_process = match self.pg_client.fetch_batch_mint_for_processing().await
            {
                Ok(Some(batch_mint)) => batch_mint,
                Ok(None) => {
                    continue;
                }
                Err(e) => {
                    error!("Failed to fetch batch_mint for processing: {}", e);
                    continue;
                }
            };
            if let Err(e) = self
                .process_batch_mint(rx.resubscribe(), batch_mint_to_process)
                .await
            {
                error!("process_batch_mint: {}", e);
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return;
                },
            }
        }
    }

    pub async fn process_batch_mint(
        &self,
        rx: Receiver<()>,
        mut batch_mint_to_process: BatchMintWithState,
    ) -> Result<(), IngesterError> {
        info!(
            "Processing {} batch_mint file",
            &batch_mint_to_process.file_name
        );
        let start_time = Instant::now();
        let (batch_mint, file_size, file_checksum) =
            self.read_batch_mint_file(&batch_mint_to_process).await?;
        let mut metadata_url = String::new();
        while rx.is_empty() {
            match &batch_mint_to_process.state {
                entities::enums::BatchMintState::Uploaded => {
                    self.process_batch_mint_validation(&batch_mint, &mut batch_mint_to_process)
                        .await?;
                }
                entities::enums::BatchMintState::ValidationComplete
                | entities::enums::BatchMintState::FailUploadToArweave => {
                    metadata_url = self
                        .process_batch_mint_upload_to_arweave(&mut batch_mint_to_process, file_size)
                        .await?;
                }
                entities::enums::BatchMintState::UploadedToArweave
                | entities::enums::BatchMintState::FailSendingTransaction => {
                    self.process_batch_mint_send_solana_tx(
                        &mut batch_mint_to_process,
                        &metadata_url,
                        &batch_mint,
                        &file_checksum,
                    )
                    .await?;
                }
                _ => {
                    info!(
                        "Finish processing {} batch_mint file with {:?} state",
                        &batch_mint_to_process.file_name, &batch_mint_to_process.state
                    );
                    self.metrics.inc_total_batch_mints(SUCCESS_METRICS_LABEL);
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

    async fn process_batch_mint_validation(
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
                .mark_batch_mint_as_failed(
                    &batch_mint_to_process.file_name,
                    &e.to_string(),
                    BatchMintState::ValidationFail,
                )
                .await
            {
                error!(
                    "Failed to mark batch_mint as verification failed: file_path: {}, error: {}",
                    &batch_mint_to_process.file_name, err
                );
            }
            self.metrics
                .inc_total_batch_mints(VALIDATION_FAIL_METRICS_LABEL);
            return Err(e.into());
        }
        if let Err(err) = self
            .pg_client
            .update_batch_mint_state(
                &batch_mint_to_process.file_name,
                BatchMintState::ValidationComplete,
            )
            .await
        {
            error!(
                "Failed to mark batch_mint as verification complete: file_path: {}, error: {}",
                &batch_mint_to_process.file_name, err
            );
        };
        batch_mint_to_process.state = entities::enums::BatchMintState::ValidationComplete;
        Ok(())
    }

    async fn process_batch_mint_upload_to_arweave(
        &self,
        batch_mint_to_process: &mut BatchMintWithState,
        file_size: usize,
    ) -> Result<String, IngesterError> {
        let (tx_id, reward) = match self
            .upload_file_with_retry(&batch_mint_to_process.file_name, file_size)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                self.metrics
                    .inc_total_batch_mints(ARWEAVE_UPLOAD_FAIL_METRICS_LABEL);
                error!(
                    "Failed upload file to arweave: file_path: {}, error: {}",
                    &batch_mint_to_process.file_name, e
                );
                return Err(e);
            }
        };
        let metadata_url = self.permanent_storage_client.get_metadata_url(&tx_id);
        if let Err(e) = self
            .pg_client
            .set_batch_mint_url_and_reward(
                &batch_mint_to_process.file_name,
                &metadata_url,
                reward as i64,
            )
            .await
        {
            error!(
                "Failed to set batch_mint url and reward: file_path: {}, error: {}",
                &batch_mint_to_process.file_name, e
            );
        }
        batch_mint_to_process.state = entities::enums::BatchMintState::UploadedToArweave;
        Ok(metadata_url)
    }

    async fn process_batch_mint_send_solana_tx(
        &self,
        batch_mint_to_process: &mut BatchMintWithState,
        metadata_url: &str,
        batch_mint: &BatchMint,
        file_checksum: &str,
    ) -> Result<(), IngesterError> {
        if let Err(e) = self
            .send_batch_mint_tx_with_retry(
                &batch_mint_to_process.file_name,
                batch_mint,
                metadata_url,
                file_checksum,
            )
            .await
        {
            self.metrics
                .inc_total_batch_mints(TRANSACTION_FAIL_METRICS_LABEL);
            error!(
                "Failed send solana transaction: file_path: {}, error: {}",
                &batch_mint_to_process.file_name, e
            );
            return Err(e);
        };
        if let Err(err) = self
            .pg_client
            .update_batch_mint_state(&batch_mint_to_process.file_name, BatchMintState::Complete)
            .await
        {
            error!(
                "Failed to mark batch_mint as verification complete: file_path: {}, error: {}",
                &batch_mint_to_process.file_name, err
            );
        };
        if let Err(err) = self
            .rocks
            .batch_mints
            .put_async(
                file_checksum.to_string(),
                BatchMintWithStaker {
                    batch_mint: batch_mint.clone(),
                    staker: Pubkey::default(), // TODO: here we must set our own pubkey
                },
            )
            .await
        {
            error!(
                "Failed to save batch_mint into rocks: file_checksum: {}, file_path: {}, error: {}",
                file_checksum, &batch_mint_to_process.file_name, err
            );
        }
        batch_mint_to_process.state = entities::enums::BatchMintState::Complete;
        Ok(())
    }

    async fn read_batch_mint_file(
        &self,
        batch_mint_to_process: &BatchMintWithState,
    ) -> Result<(BatchMint, usize, String), IngesterError> {
        let json_file = match tokio::fs::read(format!(
            "{}/{}",
            self.file_storage_path, &batch_mint_to_process.file_name
        ))
        .await
        {
            Ok(json_file) => json_file,
            Err(e) => {
                error!(
                    "Failed to read file to string: file_path: {}, error: {}",
                    &batch_mint_to_process.file_name, e
                );
                self.metrics
                    .inc_total_batch_mints(FAIL_READ_FILE_METRICS_LABEL);
                return Err(e.into());
            }
        };
        let batch_mint = match serde_json::from_slice::<BatchMint>(&json_file) {
            Ok(batch_mint) => batch_mint,
            Err(e) => {
                if let Err(e) = self
                    .pg_client
                    .mark_batch_mint_as_failed(
                        &batch_mint_to_process.file_name,
                        &e.to_string(),
                        BatchMintState::ValidationFail,
                    )
                    .await
                {
                    error!(
                        "Failed to mark batch_mint as verification failed: file_path: {}, error: {}",
                        &batch_mint_to_process.file_name, e
                    );
                }
                self.metrics
                    .inc_total_batch_mints(FAIL_BUILD_JSON_FROM_FILE_METRICS_LABEL);
                return Err(e.into());
            }
        };
        let file_hash = xxhash_rust::xxh3::xxh3_128(&json_file);
        let hash_hex = hex::encode(file_hash.to_be_bytes());

        Ok((batch_mint, json_file.len(), hash_hex))
    }

    async fn upload_file_with_retry(
        &self,
        file_name: &str,
        file_size: usize,
    ) -> Result<(String, u64), IngesterError> {
        let file_path = &format!("{}/{}", self.file_storage_path, file_name);
        let mut last_error = IngesterError::Arweave("".to_string());
        for _ in 0..MAX_BATCH_MINT_RETRIES {
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
            .mark_batch_mint_as_failed(
                file_name,
                &format!("upload to arweave: {}", last_error),
                BatchMintState::FailUploadToArweave,
            )
            .await
        {
            error!(
                "Failed to mark batch_mint as failed: file_path: {}, error: {}",
                &file_name, err
            );
        };
        error!(
            "Failed to upload batch_mint to arweave: file_path: {}, error: {}",
            &file_name, last_error
        );

        Err(IngesterError::Arweave(last_error.to_string()))
    }

    async fn send_batch_mint_tx_with_retry(
        &self,
        _file_name: &str,
        _batch_mint: &BatchMint,
        _metadata_url: &str,
        _file_checksum: &str,
    ) -> Result<(), IngesterError> {
        // let mut last_error = UsecaseError::Storage("".to_string());

        // for _ in 0..MAX_ROLLUP_RETRIES {
        // match self
        //     .batch_mint_tx_sender
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
        //     .mark_batch_mint_as_failed(
        //         file_name,
        //         &format!("send batch_mint tx: {}", last_error),
        //         RollupState::FailSendingTransaction,
        //     )
        //     .await
        // {
        //     error!(
        //         "Failed to mark batch_mint as failed: file_path: {}, error: {}",
        //         &file_name, err
        //     );
        // }
        // error!(
        //     "Failed to send batch_mint transaction: file_path: {}, error: {}",
        //     &file_name, last_error
        // );
        //
        // Err(IngesterError::SendTransaction(last_error.to_string()))
        Ok(())
    }
}
