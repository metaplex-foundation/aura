use crate::buffer::Buffer;
use crate::db_v2::DBClient;
use log::error;
use metrics_utils::{IngesterMetricsConfig, MetricStatus};
use rocks_db::asset::{AssetDynamicDetails, AssetOwner};
use rocks_db::Storage;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Instant;
use entities::enums::OwnerType;

pub const BUFFER_PROCESSING_COUNTER: i32 = 10;

#[derive(Clone)]
pub struct TokenAccsProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,
    pub db_client: Arc<DBClient>,

    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
}

impl TokenAccsProcessor {
    pub fn new(
        rocks_db: Arc<Storage>,
        db_client: Arc<DBClient>,
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
            buffer,
            metrics,
            batch_size,
            db_client,
        }
    }

    pub async fn process_token_accs(&self, keep_running: Arc<AtomicBool>) {
        let mut counter = BUFFER_PROCESSING_COUNTER;
        let mut prev_buffer_size = 0;

        while keep_running.load(Ordering::SeqCst) {
            let mut token_accounts = self.buffer.token_accs.lock().await;

            let buffer_size = token_accounts.len();

            if prev_buffer_size == 0 {
                prev_buffer_size = buffer_size;
            } else if prev_buffer_size == buffer_size {
                counter -= 1;
            } else {
                prev_buffer_size = buffer_size;
            }

            if buffer_size < self.batch_size && counter != 0 {
                drop(token_accounts);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            counter = BUFFER_PROCESSING_COUNTER;

            let mut accs_to_save = Vec::new();

            for key in token_accounts
                .keys()
                .take(self.batch_size)
                .cloned()
                .collect::<Vec<Vec<u8>>>()
            {
                if let Some(value) = token_accounts.remove(&key) {
                    accs_to_save.push(value);
                }
            }
            drop(token_accounts);

            let begin_processing = Instant::now();

            for acc in accs_to_save.iter() {
                let res = self.rocks_db.asset_owner_data.merge(
                    acc.mint,
                    &AssetOwner {
                        pubkey: acc.mint,
                        owner: acc.owner,
                        delegate: acc.delegate,
                        owner_type: OwnerType::Token,
                        owner_delegate_seq: None,
                        slot_updated: acc.slot_updated as u64,
                    },
                );

                match res {
                    Err(e) => {
                        self.metrics
                            .inc_process("accounts_saving_owner", MetricStatus::FAILURE);

                        error!("Error while saving owner: {}", e);
                    }
                    Ok(_) => {
                        self.metrics
                            .inc_process("accounts_saving_owner", MetricStatus::SUCCESS);

                        let upd_res = self
                            .rocks_db
                            .asset_updated(acc.slot_updated as u64, acc.mint);

                        if let Err(e) = upd_res {
                            error!("Error while updating assets update idx: {}", e);
                        }
                    }
                }
            }

            self.metrics.set_latency(
                "token_accounts_saving",
                begin_processing.elapsed().as_secs_f64(),
            );
        }
    }

    pub async fn process_mint_accs(&self, keep_running: Arc<AtomicBool>) {
        let mut counter = BUFFER_PROCESSING_COUNTER;
        let mut prev_buffer_size = 0;

        while keep_running.load(Ordering::SeqCst) {
            let mut mint_accounts = self.buffer.mints.lock().await;
            let buffer_size = mint_accounts.len();

            if prev_buffer_size == 0 {
                prev_buffer_size = buffer_size;
            } else if prev_buffer_size == buffer_size {
                counter -= 1;
            } else {
                prev_buffer_size = buffer_size;
            }

            if buffer_size < self.batch_size / 5 && counter != 0 {
                drop(mint_accounts);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            counter = BUFFER_PROCESSING_COUNTER;

            let mut mint_accs_to_save = Vec::new();

            for key in mint_accounts
                .keys()
                .take(self.batch_size)
                .cloned()
                .collect::<Vec<Vec<u8>>>()
            {
                if let Some(value) = mint_accounts.remove(&key) {
                    mint_accs_to_save.push(value);
                }
            }
            drop(mint_accounts);

            let begin_processing = Instant::now();

            for mint in mint_accs_to_save.iter() {
                let res = self.rocks_db.asset_dynamic_data.merge(
                    mint.pubkey,
                    &AssetDynamicDetails {
                        pubkey: mint.pubkey,
                        supply: (mint.slot_updated as u64, Some(mint.supply as u64)),
                        seq: (mint.slot_updated as u64, Some(mint.slot_updated as u64)),
                        ..Default::default()
                    },
                );

                match res {
                    Err(e) => {
                        self.metrics
                            .inc_process("mint_update_supply", MetricStatus::FAILURE);
                        error!("Error while saving mints: {}", e);
                    }
                    Ok(_) => {
                        self.metrics
                            .inc_process("mint_update_supply", MetricStatus::SUCCESS);
                        let upd_res = self
                            .rocks_db
                            .asset_updated(mint.slot_updated as u64, mint.pubkey);

                        if let Err(e) = upd_res {
                            error!("Error while updating assets update idx: {}", e);
                        }
                    }
                }
            }

            self.metrics.set_latency(
                "mint_accounts_saving",
                begin_processing.elapsed().as_secs_f64(),
            );
        }
    }
}
