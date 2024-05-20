// in RocksDB rollups queue will be data like this: [{"file hash": "rollup data"}, ...]
// data there will be saved by bubblegum updates processor

use std::{sync::Arc, time::Duration};

use entities::models::RollupToVerify;
use interface::rollup::RollupDownloader;
use log::{error, info};
use tokio::sync::broadcast::Receiver;

use crate::error::IngesterError;

use super::rollup_verifier::RollupVerifier;

pub struct RollupPersister<D: RollupDownloader> {
    rocks_client: Arc<rocks_db::Storage>,
    rollup_verifier: RollupVerifier,
    rollup_downloader: Arc<D>,
}

impl<D: RollupDownloader> RollupPersister<D> {
    pub fn new(
        rocks_client: Arc<rocks_db::Storage>,
        rollup_verifier: RollupVerifier,
        rollup_downloader: Arc<D>,
    ) -> Self {
        Self {
            rocks_client,
            rollup_verifier,
            rollup_downloader,
        }
    }

    pub async fn persist_rollups(&self, mut rx: Receiver<()>) {
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
                        let _ = self.verify_rollup(rx.resubscribe(),rollup_to_verify).await;
                    },
                _ = rx.recv() => {
                    info!("Received stop signal, stopping ...");
                    return;
                },
            }
        }
    }

    pub async fn verify_rollup(
        &self,
        rx: Receiver<()>,
        mut rollup_to_process: RollupToVerify,
    ) -> Result<(), IngesterError> {
        // using RollupDownloader download rollup file
        // verify it
        // if verification was successful - save data to RocksDB using store_rollup_update(...) method
        // regardless of the result of verification rollup should be dropped from the queue

        Ok(())
    }
}
