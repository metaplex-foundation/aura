use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use entities::{models::RollupToVerify, rollup::Rollup};
use interface::{error::UsecaseError, rollup::RollupDownloader};
use log::{error, info};
use metrics_utils::{MetricStatus, RollupPersisterMetricsConfig};
use tokio::{sync::broadcast::Receiver, task::JoinError, time::Instant};

use crate::{bubblegum_updates_processor::BubblegumTxProcessor, error::IngesterError};

use super::rollup_verifier::RollupVerifier;

pub struct RollupPersister<D: RollupDownloader> {
    rocks_client: Arc<rocks_db::Storage>,
    rollup_verifier: RollupVerifier,
    downloader: D,
    metrics: Arc<RollupPersisterMetricsConfig>,
}

pub struct RollupDownloaderForPersister{}

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
            return Err(UsecaseError::InvalidParameters(
                "File checksum mismatch".to_string(),
            ));
        }
        Ok(Box::new(serde_json::from_slice(&response)?))
    }
}

impl<D: RollupDownloader> RollupPersister<D> {
    pub fn new(rocks_client: Arc<rocks_db::Storage>, rollup_verifier: RollupVerifier, downloader: D, metrics: Arc<RollupPersisterMetricsConfig>,) -> Self {
        Self {
            rocks_client,
            rollup_verifier,
            downloader,
            metrics,
        }
    }

    pub async fn persist_rollups(&self, mut rx: Receiver<()>) -> Result<(), JoinError> {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        // get rollup from RocksDB
                        let rollup_to_verify = match self.rocks_client.fetch_rollup_for_verifying().await {
                            Ok(Some(rollup)) => rollup,
                            Ok(None) => {
                                continue;
                            }
                            Err(e) => {
                                self.metrics.inc_total_rollups("rollup_fetch", MetricStatus::FAILURE);
                                error!("Failed to fetch rollup for verifying: {}", e);
                                continue;
                            }
                        };
                        let start_time = Instant::now();
                        if let Err(e) = self.verify_rollup(rollup_to_verify).await {
                            error!("Error during rollup verification: {}", e.to_string());
                            continue;
                        }
                        self.metrics.set_persisting_latency(
                            "rollup_persisting",
                            start_time.elapsed().as_millis() as f64,
                        );
                        self.metrics.inc_total_rollups("rollup_persisting", MetricStatus::SUCCESS);
                    },
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return Ok(());
                },
            }
        }
    }

    pub async fn verify_rollup(
        &self,
        rollup_to_process: RollupToVerify,
    ) -> Result<(), IngesterError> {
        match self.downloader
        .download_rollup_and_check_checksum(
            rollup_to_process.url.unwrap().as_ref(),
            &rollup_to_process.file_hash,
        )
        .await {
            Ok(rollup) => {
                if let Err(e) = self.rollup_verifier.validate_rollup(rollup.as_ref()).await {
                    self.metrics.inc_total_rollups("rollup_validating", MetricStatus::FAILURE);
                    error!("Error while validating rollup: {}", e.to_string());
                } else {
                    if let Err(e) = BubblegumTxProcessor::store_rollup_update(
                        rollup_to_process.created_at_slot,
                        rollup,
                        self.rocks_client.clone(),
                        rollup_to_process.signature,
                    )
                    .await {
                        self.metrics.inc_total_rollups("rollup_persist", MetricStatus::FAILURE);
                        return Err(IngesterError::DatabaseError(e.to_string()));
                    }
                }
        
                if let Err(e) = self
                    .rocks_client
                    .drop_rollup_from_queue(rollup_to_process.file_hash)
                    .await
                {
                    self.metrics.inc_total_rollups("rollup_queue_clear", MetricStatus::FAILURE);
                    return Err(IngesterError::DatabaseError(e.to_string()));
                }
            }
            Err(e) => {
                self.metrics.inc_total_rollups("rollup_download", MetricStatus::FAILURE);
                return Err(IngesterError::Usecase(e.to_string()));
            }
        }

        Ok(())
    }
}
