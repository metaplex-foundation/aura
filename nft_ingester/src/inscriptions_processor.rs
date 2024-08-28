use crate::buffer::Buffer;
use crate::process_accounts;
use libreplex_inscriptions::Inscription;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::broadcast::Receiver;
use usecase::save_metrics::result_to_metrics;

pub struct InscriptionInfo {
    pub inscription: Inscription,
    pub write_version: u64,
    pub slot_updated: u64,
}

impl From<&InscriptionInfo> for rocks_db::inscriptions::Inscription {
    fn from(value: &InscriptionInfo) -> Self {
        Self {
            authority: value.inscription.authority,
            root: value.inscription.root,
            content_type: value.inscription.media_type.convert_to_string(),
            encoding: value.inscription.encoding_type.convert_to_string(),
            inscription_data_account: value.inscription.inscription_data,
            order: value.inscription.order,
            size: value.inscription.size,
            validation_hash: value.inscription.validation_hash.clone(),
            write_version: value.write_version,
        }
    }
}

pub struct InscriptionDataInfo {
    pub inscription_data: Vec<u8>,
    pub write_version: u64,
    pub slot_updated: u64,
}

#[derive(Clone)]
pub struct InscriptionsProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,
    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,

    last_received_inscription_at: Option<SystemTime>,
    last_received_inscription_data_at: Option<SystemTime>,
}

impl InscriptionsProcessor {
    pub fn new(
        rocks_db: Arc<Storage>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
            buffer,
            batch_size,
            metrics,
            last_received_inscription_at: None,
            last_received_inscription_data_at: None,
        }
    }

    pub async fn process_inscriptions(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
            self.buffer.inscriptions,
            self.batch_size,
            |s: InscriptionInfo| s,
            self.last_received_inscription_at,
            Self::store_inscriptions,
            "inscriptions"
        );
    }

    pub async fn process_inscriptions_data(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
            self.buffer.inscriptions_data,
            self.batch_size,
            |s: InscriptionDataInfo| s,
            self.last_received_inscription_data_at,
            Self::store_inscriptions_data,
            "inscriptions_data"
        );
    }

    pub async fn store_inscriptions(&self, inscriptions: &HashMap<Pubkey, InscriptionInfo>) {
        let begin_processing = Instant::now();
        let res = self
            .rocks_db
            .inscriptions
            .merge_batch(
                inscriptions
                    .iter()
                    .map(|(_, inscription)| {
                        (
                            // root - address of the nft this inscription relates to
                            inscription.inscription.root,
                            inscription.into(),
                        )
                    })
                    .collect(),
            )
            .await;

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_saving");
        self.metrics.set_latency(
            "inscriptions_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn store_inscriptions_data(
        &self,
        inscriptions_data: &HashMap<Pubkey, InscriptionDataInfo>,
    ) {
        let begin_processing = Instant::now();
        let res = self
            .rocks_db
            .inscription_data
            .merge_batch(
                inscriptions_data
                    .iter()
                    .map(|(key, inscription)| {
                        (
                            *key,
                            rocks_db::inscriptions::InscriptionData {
                                pubkey: *key,
                                data: inscription.inscription_data.clone(),
                                write_version: inscription.write_version,
                            },
                        )
                    })
                    .collect(),
            )
            .await;

        result_to_metrics(self.metrics.clone(), &res, "inscriptions_data_saving");
        self.metrics.set_latency(
            "inscriptions_data_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
    }
}
