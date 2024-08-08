use std::ops::Deref;
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bubblegum_batch_sdk::model::BatchMint;
use entities::enums::PersistingBatchMintState;
use entities::{enums::FailedBatchMintState, models::BatchMintToVerify};
use interface::{batch_mint::BatchMintDownloader, error::UsecaseError};
use log::{error, info};
use metrics_utils::{MetricStatus, RollupPersisterMetricsConfig};
use rocks_db::batch_mint::BatchMintWithStaker;
use tokio::{sync::broadcast::Receiver, task::JoinError, time::Instant};

use crate::{bubblegum_updates_processor::BubblegumTxProcessor, error::IngesterError};
use usecase::error::RollupValidationError;

pub const MAX_ROLLUP_DOWNLOAD_ATTEMPTS: u8 = 5;

pub struct RollupPersister<D: BatchMintDownloader> {
    rocks_client: Arc<rocks_db::Storage>,
    downloader: D,
    metrics: Arc<RollupPersisterMetricsConfig>,
}

pub struct RollupDownloaderForPersister {}

#[async_trait]
impl BatchMintDownloader for RollupDownloaderForPersister {
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

impl<D: BatchMintDownloader> RollupPersister<D> {
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        downloader: D,
        metrics: Arc<RollupPersisterMetricsConfig>,
    ) -> Self {
        Self {
            rocks_client,
            downloader,
            metrics,
        }
    }

    pub async fn persist_rollups(&self, mut rx: Receiver<()>) -> Result<(), JoinError> {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        let (rollup_to_verify, rollup) = match self.get_rollup_to_verify().await {
                            Ok(res) => res,
                            Err(_) => {
                                continue;
                            }
                        };
                        let Some(rollup_to_verify) = rollup_to_verify else {
                            // no rollups to persist
                            continue;
                        };
                        self.persist_rollup(&rx, rollup_to_verify, rollup).await
                    },
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return Ok(());
                },
            }
        }
    }

    pub async fn persist_rollup(
        &self,
        rx: &Receiver<()>,
        mut rollup_to_verify: BatchMintToVerify,
        mut rollup: Option<Box<BatchMint>>,
    ) {
        let start_time = Instant::now();
        info!("Persisting {} rollup", &rollup_to_verify.url);
        while rx.is_empty() {
            match &rollup_to_verify.persisting_state {
                &PersistingBatchMintState::ReceivedTransaction => {
                    if let Err(err) = self
                        .download_rollup(&mut rollup_to_verify, &mut rollup)
                        .await
                    {
                        error!("Error during rollup downloading: {}", err)
                    };
                }
                &PersistingBatchMintState::SuccessfullyDownload => {
                    if let Some(r) = &rollup {
                        self.validate_rollup(&mut rollup_to_verify, r).await;
                    } else {
                        error!(
                            "Trying to validate non downloaded rollup: {:#?}",
                            &rollup_to_verify
                        )
                    }
                }
                &PersistingBatchMintState::SuccessfullyValidate => {
                    if let Some(r) = &rollup {
                        self.store_rollup_update(&mut rollup_to_verify, r).await;
                    } else {
                        error!(
                            "Trying to store update for non downloaded rollup: {:#?}",
                            &rollup_to_verify
                        )
                    }
                }
                &PersistingBatchMintState::FailedToPersist
                | &PersistingBatchMintState::StoredUpdate => {
                    self.drop_rollup_from_queue(rollup_to_verify.file_hash.clone())
                        .await;
                    info!(
                        "Finish processing {} rollup file with {:?} state",
                        &rollup_to_verify.url, &rollup_to_verify.persisting_state
                    );
                    self.metrics.set_persisting_latency(
                        "rollup_persisting",
                        start_time.elapsed().as_millis() as f64,
                    );
                    return;
                }
            }
        }
        if let Err(e) = self
            .rocks_client
            .rollup_to_verify
            .put_async(rollup_to_verify.file_hash.clone(), rollup_to_verify.clone())
            .await
        {
            error!("Update rollup to verify state: {}", e)
        };
    }

    async fn get_rollup_to_verify(
        &self,
    ) -> Result<(Option<BatchMintToVerify>, Option<Box<BatchMint>>), IngesterError> {
        match self.rocks_client.fetch_rollup_for_verifying().await {
            Ok((Some(rollup_to_verify), rollup_data)) => {
                Ok((Some(rollup_to_verify), rollup_data.map(Box::new)))
            }
            Ok((None, _)) => Ok((None, None)),
            Err(e) => {
                self.metrics
                    .inc_rollups_with_status("rollup_fetch", MetricStatus::FAILURE);
                error!("Failed to fetch rollup for verifying: {}", e);
                Err(e.into())
            }
        }
    }

    async fn download_rollup(
        &self,
        rollup_to_verify: &mut BatchMintToVerify,
        rollup: &mut Option<Box<BatchMint>>,
    ) -> Result<(), IngesterError> {
        if rollup.is_some() {
            rollup_to_verify.persisting_state = PersistingBatchMintState::SuccessfullyDownload;
            return Ok(());
        }
        match self
            .downloader
            .download_batch_mint_and_check_checksum(
                rollup_to_verify.url.as_ref(),
                &rollup_to_verify.file_hash,
            )
            .await
        {
            Ok(r) => {
                self.metrics
                    .inc_rollups_with_status("rollup_download", MetricStatus::SUCCESS);
                if let Err(e) = self.rocks_client.rollups.put(
                    rollup_to_verify.file_hash.clone(),
                    BatchMintWithStaker {
                        batch_mint: r.deref().clone(),
                        staker: rollup_to_verify.staker,
                    },
                ) {
                    self.metrics
                        .inc_rollups_with_status("persist_rollup", MetricStatus::FAILURE);
                    return Err(e.into());
                }
                *rollup = Some(r);
                rollup_to_verify.persisting_state = PersistingBatchMintState::SuccessfullyDownload;
            }
            Err(e) => {
                if let UsecaseError::HashMismatch(expected, actual) = e {
                    rollup_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
                    self.metrics
                        .inc_rollups_with_status("rollup_checksum_verify", MetricStatus::FAILURE);
                    self.save_rollup_as_failed(
                        FailedBatchMintState::ChecksumVerifyFailed,
                        rollup_to_verify,
                    )
                    .await?;

                    return Err(IngesterError::RollupValidation(
                        RollupValidationError::FileChecksumMismatch(expected, actual),
                    ));
                }
                if let UsecaseError::Serialization(e) = e {
                    rollup_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
                    self.metrics.inc_rollups_with_status(
                        "rollup_file_deserialization",
                        MetricStatus::FAILURE,
                    );
                    self.save_rollup_as_failed(
                        FailedBatchMintState::FileSerialization,
                        rollup_to_verify,
                    )
                    .await?;

                    return Err(IngesterError::SerializatonError(e));
                }

                self.metrics
                    .inc_rollups_with_status("rollup_download", MetricStatus::FAILURE);
                if rollup_to_verify.download_attempts + 1 > MAX_ROLLUP_DOWNLOAD_ATTEMPTS {
                    rollup_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
                    self.save_rollup_as_failed(
                        FailedBatchMintState::DownloadFailed,
                        rollup_to_verify,
                    )
                    .await?;
                } else {
                    if let Err(e) = self
                        .rocks_client
                        .inc_rollup_to_verify_download_attempts(rollup_to_verify)
                        .await
                    {
                        self.metrics.inc_rollups_with_status(
                            "rollup_attempts_update",
                            MetricStatus::FAILURE,
                        );
                        return Err(e.into());
                    }
                    self.metrics
                        .inc_rollups_with_status("rollup_attempts_update", MetricStatus::SUCCESS);
                }
                return Err(IngesterError::Usecase(e.to_string()));
            }
        }
        Ok(())
    }

    async fn validate_rollup(&self, rollup_to_verify: &mut BatchMintToVerify, rollup: &BatchMint) {
        if let Err(e) = bubblegum_batch_sdk::batch_mint_validations::validate_batch_mint(
            rollup,
            rollup_to_verify.collection_mint,
        )
        .await
        {
            self.metrics
                .inc_rollups_with_status("rollup_validating", MetricStatus::FAILURE);
            error!("Error while validating rollup: {}", e.to_string());

            rollup_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
            if let Err(err) = self
                .save_rollup_as_failed(FailedBatchMintState::RollupVerifyFailed, rollup_to_verify)
                .await
            {
                error!("Save rollup as failed: {}", err);
            };
            return;
        }
        self.metrics
            .inc_rollups_with_status("rollup_validating", MetricStatus::SUCCESS);
        rollup_to_verify.persisting_state = PersistingBatchMintState::SuccessfullyValidate;
    }

    async fn store_rollup_update(
        &self,
        rollup_to_verify: &mut BatchMintToVerify,
        rollup: &BatchMint,
    ) {
        if BubblegumTxProcessor::store_rollup_update(
            rollup_to_verify.created_at_slot,
            rollup,
            self.rocks_client.clone(),
            rollup_to_verify.signature,
        )
        .await
        .is_err()
        {
            self.metrics
                .inc_rollups_with_status("rollup_persist", MetricStatus::FAILURE);
            rollup_to_verify.persisting_state = PersistingBatchMintState::FailedToPersist;
            return;
        }
        self.metrics
            .inc_rollups_with_status("rollup_persist", MetricStatus::SUCCESS);
        rollup_to_verify.persisting_state = PersistingBatchMintState::StoredUpdate;
    }

    async fn save_rollup_as_failed(
        &self,
        status: FailedBatchMintState,
        rollup: &BatchMintToVerify,
    ) -> Result<(), IngesterError> {
        if let Err(e) = self
            .rocks_client
            .save_rollup_as_failed(status, rollup)
            .await
        {
            self.metrics
                .inc_rollups_with_status("rollup_mark_as_failure", MetricStatus::FAILURE);
            return Err(e.into());
        };
        Ok(())
    }

    async fn drop_rollup_from_queue(&self, file_hash: String) {
        if let Err(e) = self.rocks_client.drop_rollup_from_queue(file_hash).await {
            self.metrics
                .inc_rollups_with_status("rollup_queue_clear", MetricStatus::FAILURE);
            error!("Rollup queue clear: {}", e)
        }
    }
}
