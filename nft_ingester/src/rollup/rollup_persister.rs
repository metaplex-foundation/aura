use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use entities::{models::RollupToVerify, rollup::Rollup};
use interface::{error::UsecaseError, rollup::RollupDownloader};
use log::{error, info};
use tokio::{sync::broadcast::Receiver, task::JoinError};

use crate::{bubblegum_updates_processor::BubblegumTxProcessor, error::IngesterError};

use super::rollup_verifier::RollupVerifier;

pub struct RollupPersister {
    rocks_client: Arc<rocks_db::Storage>,
    rollup_verifier: RollupVerifier,
}

#[async_trait]
impl RollupDownloader for RollupPersister {
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

impl RollupPersister {
    pub fn new(rocks_client: Arc<rocks_db::Storage>, rollup_verifier: RollupVerifier) -> Self {
        Self {
            rocks_client,
            rollup_verifier,
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
                                error!("Failed to fetch rollup for verifying: {}", e);
                                continue;
                            }
                        };
                        let _ = self.verify_rollup(rollup_to_verify).await;
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
        // TODO: add retry
        let rollup = self
            .download_rollup_and_check_checksum(
                rollup_to_process.url.unwrap().as_ref(),
                &rollup_to_process.file_hash,
            )
            .await
            .unwrap();

        if let Err(e) = self.rollup_verifier.validate_rollup(rollup.as_ref()).await {
            error!("Error while validating rollup: {}", e.to_string());
        } else {
            if let Err(e) = BubblegumTxProcessor::store_rollup_update(
                rollup_to_process.created_at_slot,
                rollup,
                self.rocks_client.clone(),
                rollup_to_process.signature,
            )
            .await
            {
                error!(
                    "Error while saving rollup data to RocksDB: {}",
                    e.to_string()
                );
            } else {
                info!("All good, rollup is saved");
            }
        }
        // TODO: do not drop if there was error during rollup saving
        if let Err(e) = self
            .rocks_client
            .drop_rollup_from_queue(rollup_to_process.file_hash)
            .await
        {
            error!(
                "Error while deleting rollup from the RocksDB queue: {}",
                e.to_string()
            );
        } else {
            info!("Rollup is successfully dropped from the queue");
        }

        Ok(())
    }
}
