use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use entities::{
    enums::FailedRollupState,
    models::{DownloadedRollupData, FailedRollup, FailedRollupKey, RollupToVerify},
    rollup::Rollup,
};
use interface::{error::UsecaseError, rollup::RollupDownloader};
use log::{error, info};
use metrics_utils::{MetricStatus, RollupPersisterMetricsConfig};
use postgre_client::PgClient;
use tokio::{sync::broadcast::Receiver, task::JoinError, time::Instant};

use crate::{bubblegum_updates_processor::BubblegumTxProcessor, error::IngesterError};
use usecase::error::RollupValidationError;

use super::rollup_verifier::RollupVerifier;

pub const MAX_ROLLUP_DOWNLOAD_ATTEMPTS: u8 = 5;

pub struct RollupPersister<D: RollupDownloader> {
    rocks_client: Arc<rocks_db::Storage>,
    postgre_client: Arc<PgClient>,
    rollup_verifier: RollupVerifier,
    downloader: D,
    metrics: Arc<RollupPersisterMetricsConfig>,
}

pub struct RollupDownloaderForPersister {}

#[async_trait]
impl RollupDownloader for RollupDownloaderForPersister {
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
            return Err(UsecaseError::HashMismatch(format!(
                "File checksum mismatch. Expected {:?} got {:?}",
                checksum, hash_hex
            )));
        }
        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}

impl<D: RollupDownloader> RollupPersister<D> {
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        postgre_client: Arc<PgClient>,
        rollup_verifier: RollupVerifier,
        downloader: D,
        metrics: Arc<RollupPersisterMetricsConfig>,
    ) -> Self {
        Self {
            rocks_client,
            postgre_client,
            rollup_verifier,
            downloader,
            metrics,
        }
    }

    pub async fn persist_rollups(&self, mut rx: Receiver<()>) -> Result<(), JoinError> {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        match self.get_rollup_to_verify().await {
                            Ok((rollup_to_verify, rollup_data)) => {
                                if let Some(rollup_to_verify) = rollup_to_verify {
                                    if let Err(e) = self.verify_rollup(rollup_to_verify, rollup_data).await {
                                        error!("Error during rollup verification: {}", e.to_string());
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                            }
                            Err(e) => {
                                error!("Could not fetch rollup for verification from queue: {:?}", e.to_string());
                                continue;
                            }
                        }
                    },
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return Ok(());
                },
            }
        }
    }

    async fn get_rollup_to_verify(
        &self,
    ) -> Result<(Option<RollupToVerify>, Option<Box<Rollup>>), IngesterError> {
        match self.rocks_client.fetch_rollup_for_verifying().await {
            Ok((Some(rollup), rollup_data)) => {
                // check if this rollup was created with our instance
                let res = self
                    .postgre_client
                    .get_rollup_by_url(rollup.url.as_ref())
                    .await;

                match res {
                    Ok(rollup_with_state) => {
                        if rollup_with_state.is_some() {
                            self.drop_rollup_from_queue(rollup.file_hash.clone())
                                .await?;

                            self.metrics.inc_rollups("rollup_from_api_in_persister");

                            return Ok((None, None));
                        }
                    }
                    Err(e) => {
                        self.metrics.inc_rollups_with_status(
                            "rollup_check_in_postgre",
                            MetricStatus::FAILURE,
                        );
                        error!("Failed to check if rollup exist in Postgre: {:?}", e);
                        return Err(IngesterError::DatabaseError(e));
                    }
                }

                if let Some(data) = rollup_data {
                    let deserialized =
                        Box::new(bincode::deserialize::<Rollup>(data.rollup.as_ref()).unwrap());

                    return Ok((Some(rollup), Some(deserialized)));
                }

                Ok((Some(rollup), None))
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

    pub async fn verify_rollup(
        &self,
        rollup_to_process: RollupToVerify,
        rollup_data: Option<Box<Rollup>>,
    ) -> Result<(), IngesterError> {
        let start_time = Instant::now();

        let rollup = if let Some(rollup) = rollup_data {
            rollup
        } else {
            match self
                .downloader
                .download_rollup_and_check_checksum(
                    rollup_to_process.url.as_ref(),
                    &rollup_to_process.file_hash,
                )
                .await
            {
                Ok(rollup) => {
                    let downloaded_rollup_data = DownloadedRollupData {
                        hash: rollup_to_process.file_hash.clone(),
                        rollup: bincode::serialize(&rollup)?,
                    };

                    if let Err(e) = self
                        .rocks_client
                        .downloaded_rollups
                        .put(rollup_to_process.file_hash.clone(), downloaded_rollup_data)
                    {
                        self.metrics
                            .inc_rollups_with_status("persist_rollup", MetricStatus::FAILURE);
                        return Err(e.into());
                    }

                    rollup
                }
                Err(e) => {
                    if let UsecaseError::HashMismatch(e) = e {
                        self.metrics.inc_rollups_with_status(
                            "rollup_checksum_verify",
                            MetricStatus::FAILURE,
                        );

                        self.save_rollup_as_failed(
                            FailedRollupState::ChecksumVerifyFailed,
                            &rollup_to_process,
                        )?;

                        self.drop_rollup_from_queue(rollup_to_process.file_hash.clone())
                            .await?;

                        return Err(IngesterError::RollupValidation(
                            RollupValidationError::FileChecksumMismatch(e),
                        ));
                    }
                    if let UsecaseError::Serialization(e) = e {
                        self.metrics.inc_rollups_with_status(
                            "rollup_file_deserialization",
                            MetricStatus::FAILURE,
                        );

                        self.save_rollup_as_failed(
                            FailedRollupState::FileSerialization,
                            &rollup_to_process,
                        )?;

                        self.drop_rollup_from_queue(rollup_to_process.file_hash.clone())
                            .await?;

                        return Err(IngesterError::SerializatonError(e));
                    }

                    self.metrics
                        .inc_rollups_with_status("rollup_download", MetricStatus::FAILURE);

                    if rollup_to_process.download_attempts + 1 > MAX_ROLLUP_DOWNLOAD_ATTEMPTS {
                        self.drop_rollup_from_queue(rollup_to_process.file_hash.clone())
                            .await?;

                        self.save_rollup_as_failed(
                            FailedRollupState::DownloadFailed,
                            &rollup_to_process,
                        )?;
                    } else {
                        if let Err(e) = self.rocks_client.rollup_to_verify.put(
                            rollup_to_process.file_hash.clone(),
                            RollupToVerify {
                                download_attempts: rollup_to_process.download_attempts + 1,
                                ..rollup_to_process
                            },
                        ) {
                            self.metrics.inc_rollups_with_status(
                                "rollup_attempts_update",
                                MetricStatus::FAILURE,
                            );
                            return Err(e.into());
                        }
                        self.metrics.inc_rollups_with_status(
                            "rollup_attempts_update",
                            MetricStatus::SUCCESS,
                        );
                    }

                    return Err(IngesterError::Usecase(e.to_string()));
                }
            }
        };

        if let Err(e) = self.rollup_verifier.validate_rollup(rollup.as_ref()).await {
            self.metrics
                .inc_rollups_with_status("rollup_validating", MetricStatus::FAILURE);
            error!("Error while validating rollup: {}", e.to_string());

            self.save_rollup_as_failed(FailedRollupState::RollupVerifyFailed, &rollup_to_process)?;
            self.drop_rollup_from_queue(rollup_to_process.file_hash.clone())
                .await?;

            return Err(e.into());
        } else if let Err(e) = BubblegumTxProcessor::store_rollup_update(
            rollup_to_process.created_at_slot,
            rollup,
            self.rocks_client.clone(),
            rollup_to_process.signature,
        )
        .await
        {
            self.metrics
                .inc_rollups_with_status("rollup_persist", MetricStatus::FAILURE);
            return Err(e);
        }

        self.metrics
            .inc_rollups_with_status("rollup_validating", MetricStatus::SUCCESS);

        self.metrics
            .set_persisting_latency("rollup_persisting", start_time.elapsed().as_millis() as f64);

        self.drop_rollup_from_queue(rollup_to_process.file_hash.clone())
            .await
    }

    fn save_rollup_as_failed(
        &self,
        status: FailedRollupState,
        rollup: &RollupToVerify,
    ) -> Result<(), IngesterError> {
        let key = FailedRollupKey {
            status: status.clone(),
            hash: rollup.file_hash.clone(),
        };
        let value = FailedRollup {
            status,
            file_hash: rollup.file_hash.clone(),
            url: rollup.url.clone(),
            created_at_slot: rollup.created_at_slot,
            signature: rollup.signature,
            download_attempts: rollup.download_attempts + 1,
        };
        if let Err(e) = self.rocks_client.failed_rollups.put(key, value) {
            self.metrics
                .inc_rollups_with_status("rollup_mark_as_failure", MetricStatus::FAILURE);
            return Err(e.into());
        }
        Ok(())
    }

    async fn drop_rollup_from_queue(&self, file_hash: String) -> Result<(), IngesterError> {
        if let Err(e) = self.rocks_client.drop_rollup_from_queue(file_hash).await {
            self.metrics
                .inc_rollups_with_status("rollup_queue_clear", MetricStatus::FAILURE);
            return Err(e.into());
        }
        Ok(())
    }
}
