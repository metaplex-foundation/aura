use std::ops::Deref;
use std::{sync::Arc, time::Duration};

use crate::{
    error::IngesterError,
    processors::transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
};
use async_trait::async_trait;
use bubblegum_batch_sdk::model::BatchMint;
use entities::enums::PersistingBatchMintState;
use entities::{enums::FailedBatchMintState, models::BatchMintToVerify};
use interface::{batch_mint::BatchMintDownloader, error::UsecaseError};
use metrics_utils::{BatchMintPersisterMetricsConfig, MetricStatus};
use rocks_db::columns::batch_mint::BatchMintWithStaker;
use tokio::{sync::broadcast::Receiver, task::JoinError, time::Instant};
use tracing::{error, info};

pub const MAX_BATCH_MINT_DOWNLOAD_ATTEMPTS: u8 = 5;

pub struct BatchMintPersister<D: BatchMintDownloader> {
    rocks_client: Arc<rocks_db::Storage>,
    downloader: D,
    metrics: Arc<BatchMintPersisterMetricsConfig>,
}

pub struct BatchMintDownloaderForPersister;

#[async_trait]
impl BatchMintDownloader for BatchMintDownloaderForPersister {
    async fn download_batch_mint(&self, url: &str) -> Result<Box<BatchMint>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;
        Ok(Box::new(serde_json::from_slice(&response)?))
    }

    async fn download_batch_mint_and_check_checksum(
        &self,
        url: &str,
        checksum: &str,
    ) -> Result<Box<BatchMint>, UsecaseError> {
        let response = reqwest::get(url).await?.bytes().await?;
        let file_hash = xxhash_rust::xxh3::xxh3_128(&response);
        let hash_hex = hex::encode(file_hash.to_be_bytes());
        if hash_hex != checksum {
            return Err(UsecaseError::HashMismatch(checksum.to_string(), hash_hex));
        }
        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}

impl<D: BatchMintDownloader> BatchMintPersister<D> {
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        downloader: D,
        metrics: Arc<BatchMintPersisterMetricsConfig>,
    ) -> Self {
        Self {
            rocks_client,
            downloader,
            metrics,
        }
    }

    pub async fn persist_batch_mints(&self, mut rx: Receiver<()>) -> Result<(), JoinError> {
        while rx.is_empty() {
            let (batch_mint_to_verify, batch_mint) = match self.get_batch_mint_to_verify().await {
                Ok(res) => res,
                Err(_) => {
                    continue;
                }
            };
            let Some(batch_mint_to_verify) = batch_mint_to_verify else {
                // no batch_mints to persist
                continue;
            };
            self.persist_batch_mint(&rx, batch_mint_to_verify, batch_mint)
                .await;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return Ok(());
                },
            }
        }
        Ok(())
    }

    pub async fn persist_batch_mint(
        &self,
        rx: &Receiver<()>,
        mut batch_mint_to_verify: BatchMintToVerify,
        mut batch_mint: Option<Box<BatchMint>>,
    ) {
        let start_time = Instant::now();
        info!("Persisting {} batch_mint", &batch_mint_to_verify.url);
        while rx.is_empty() {
            match &batch_mint_to_verify.persisting_state {
                &PersistingBatchMintState::ReceivedTransaction => {
                    if let Err(err) = self
                        .download_batch_mint(&mut batch_mint_to_verify, &mut batch_mint)
                        .await
                    {
                        error!("Error during batch_mint downloading: {}", err)
                    };
                }
                &PersistingBatchMintState::SuccessfullyDownload => {
                    if let Some(r) = &batch_mint {
                        self.validate_batch_mint(&mut batch_mint_to_verify, r).await;
                    } else {
                        error!(
                            "Trying to validate non downloaded batch_mint: {:#?}",
                            &batch_mint_to_verify
                        )
                    }
                }
                &PersistingBatchMintState::SuccessfullyValidate => {
                    if let Some(r) = &batch_mint {
                        self.store_batch_mint_update(&mut batch_mint_to_verify, r)
                            .await;
                    } else {
                        error!(
                            "Trying to store update for non downloaded batch_mint: {:#?}",
                            &batch_mint_to_verify
                        )
                    }
                }
                &PersistingBatchMintState::FailedToPersist
                | &PersistingBatchMintState::StoredUpdate => {
                    self.drop_batch_mint_from_queue(batch_mint_to_verify.file_hash.clone())
                        .await;
                    info!(
                        "Finish processing {} batch_mint file with {:?} state",
                        &batch_mint_to_verify.url, &batch_mint_to_verify.persisting_state
                    );
                    self.metrics.set_persisting_latency(
                        "batch_mint_persisting",
                        start_time.elapsed().as_millis() as f64,
                    );
                    return;
                }
            }
        }
        if let Err(e) = self
            .rocks_client
            .batch_mint_to_verify
            .put_async(
                batch_mint_to_verify.file_hash.clone(),
                batch_mint_to_verify.clone(),
            )
            .await
        {
            error!("Update batch_mint to verify state: {}", e)
        };
    }

    async fn get_batch_mint_to_verify(
        &self,
    ) -> Result<(Option<BatchMintToVerify>, Option<Box<BatchMint>>), IngesterError> {
        match self.rocks_client.fetch_batch_mint_for_verifying().await {
            Ok((Some(batch_mint_to_verify), batch_mint_data)) => {
                Ok((Some(batch_mint_to_verify), batch_mint_data.map(Box::new)))
            }
            Ok((None, _)) => Ok((None, None)),
            Err(e) => {
                self.metrics
                    .inc_batch_mints_with_status("batch_mint_fetch", MetricStatus::FAILURE);
                error!("Failed to fetch batch_mint for verifying: {}", e);
                Err(e.into())
            }
        }
    }

    async fn download_batch_mint(
        &self,
        batch_mint_to_verify: &mut BatchMintToVerify,
        batch_mint: &mut Option<Box<BatchMint>>,
    ) -> Result<(), IngesterError> {
        let begin_processing = Instant::now();
        if batch_mint.is_some() {
            batch_mint_to_verify.persisting_state = PersistingBatchMintState::SuccessfullyDownload;
            return Ok(());
        }
        match self
            .downloader
            .download_batch_mint_and_check_checksum(
                batch_mint_to_verify.url.as_ref(),
                &batch_mint_to_verify.file_hash,
            )
            .await
        {
            Ok(r) => {
                self.metrics
                    .inc_batch_mints_with_status("batch_mint_download", MetricStatus::SUCCESS);
                if let Err(e) = self.rocks_client.batch_mints.put(
                    batch_mint_to_verify.file_hash.clone(),
                    BatchMintWithStaker {
                        batch_mint: r.deref().clone(),
                        staker: batch_mint_to_verify.staker,
                    },
                ) {
                    self.metrics
                        .inc_batch_mints_with_status("persist_batch_mint", MetricStatus::FAILURE);
                    return Err(e.into());
                }
                *batch_mint = Some(r);
                batch_mint_to_verify.persisting_state =
                    PersistingBatchMintState::SuccessfullyDownload;
            }
            Err(e) => {
                if let UsecaseError::HashMismatch(expected, actual) = e {
                    batch_mint_to_verify.persisting_state =
                        PersistingBatchMintState::FailedToPersist;
                    self.metrics.inc_batch_mints_with_status(
                        "batch_mint_checksum_verify",
                        MetricStatus::FAILURE,
                    );
                    self.save_batch_mint_as_failed(
                        FailedBatchMintState::ChecksumVerifyFailed,
                        batch_mint_to_verify,
                    )
                    .await?;

                    return Err(IngesterError::FileChecksumMismatch(expected, actual));
                }
                if let UsecaseError::Serialization(e) = e {
                    batch_mint_to_verify.persisting_state =
                        PersistingBatchMintState::FailedToPersist;
                    self.metrics.inc_batch_mints_with_status(
                        "batch_mint_file_deserialization",
                        MetricStatus::FAILURE,
                    );
                    self.save_batch_mint_as_failed(
                        FailedBatchMintState::FileSerialization,
                        batch_mint_to_verify,
                    )
                    .await?;

                    return Err(IngesterError::SerializatonError(e));
                }

                self.metrics
                    .inc_batch_mints_with_status("batch_mint_download", MetricStatus::FAILURE);
                if batch_mint_to_verify.download_attempts + 1 > MAX_BATCH_MINT_DOWNLOAD_ATTEMPTS {
                    batch_mint_to_verify.persisting_state =
                        PersistingBatchMintState::FailedToPersist;
                    self.save_batch_mint_as_failed(
                        FailedBatchMintState::DownloadFailed,
                        batch_mint_to_verify,
                    )
                    .await?;
                } else {
                    if let Err(e) = self
                        .rocks_client
                        .inc_batch_mint_to_verify_download_attempts(batch_mint_to_verify)
                        .await
                    {
                        self.metrics.inc_batch_mints_with_status(
                            "batch_mint_attempts_update",
                            MetricStatus::FAILURE,
                        );
                        return Err(e.into());
                    }
                    self.metrics.inc_batch_mints_with_status(
                        "batch_mint_attempts_update",
                        MetricStatus::SUCCESS,
                    );
                }
                return Err(IngesterError::Usecase(e.to_string()));
            }
        }
        self.metrics.set_persisting_latency(
            "batch_mint_download",
            begin_processing.elapsed().as_millis() as f64,
        );
        Ok(())
    }

    async fn validate_batch_mint(
        &self,
        batch_mint_to_verify: &mut BatchMintToVerify,
        batch_mint: &BatchMint,
    ) {
        let begin_processing = Instant::now();
        if let Err(e) = bubblegum_batch_sdk::batch_mint_validations::validate_batch_mint(
            batch_mint,
            batch_mint_to_verify.collection_mint,
        )
        .await
        {
            self.metrics
                .inc_batch_mints_with_status("batch_mint_validating", MetricStatus::FAILURE);
            error!("Error while validating batch_mint: {}", e.to_string());

            batch_mint_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
            if let Err(err) = self
                .save_batch_mint_as_failed(
                    FailedBatchMintState::BatchMintVerifyFailed,
                    batch_mint_to_verify,
                )
                .await
            {
                error!("Save batch_mint as failed: {}", err);
            };
            return;
        }
        self.metrics
            .inc_batch_mints_with_status("batch_mint_validating", MetricStatus::SUCCESS);
        self.metrics.set_persisting_latency(
            "batch_mint_validation",
            begin_processing.elapsed().as_millis() as f64,
        );
        batch_mint_to_verify.persisting_state = PersistingBatchMintState::SuccessfullyValidate;
    }

    async fn store_batch_mint_update(
        &self,
        batch_mint_to_verify: &mut BatchMintToVerify,
        batch_mint: &BatchMint,
    ) {
        let begin_processing = Instant::now();
        if BubblegumTxProcessor::store_batch_mint_update(
            batch_mint_to_verify.created_at_slot,
            batch_mint,
            self.rocks_client.clone(),
            batch_mint_to_verify.signature,
        )
        .await
        .is_err()
        {
            self.metrics
                .inc_batch_mints_with_status("batch_mint_persist", MetricStatus::FAILURE);
            batch_mint_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
            return;
        }
        self.metrics
            .inc_batch_mints_with_status("batch_mint_persist", MetricStatus::SUCCESS);
        self.metrics.set_persisting_latency(
            "batch_mint_store_into_db",
            begin_processing.elapsed().as_millis() as f64,
        );
        batch_mint_to_verify.persisting_state = PersistingBatchMintState::StoredUpdate;
    }

    async fn save_batch_mint_as_failed(
        &self,
        status: FailedBatchMintState,
        batch_mint: &BatchMintToVerify,
    ) -> Result<(), IngesterError> {
        if let Err(e) = self
            .rocks_client
            .save_batch_mint_as_failed(status, batch_mint)
            .await
        {
            self.metrics
                .inc_batch_mints_with_status("batch_mint_mark_as_failure", MetricStatus::FAILURE);
            return Err(e.into());
        };
        Ok(())
    }

    async fn drop_batch_mint_from_queue(&self, file_hash: String) {
        if let Err(e) = self
            .rocks_client
            .drop_batch_mint_from_queue(file_hash)
            .await
        {
            self.metrics
                .inc_batch_mints_with_status("batch_mint_queue_clear", MetricStatus::FAILURE);
            error!("batch_mint queue clear: {}", e)
        }
    }
}
