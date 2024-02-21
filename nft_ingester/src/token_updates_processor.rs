use crate::buffer::Buffer;
use crate::db_v2::DBClient;
use crate::mplx_updates_processor::{
    result_to_metrics, FLUSH_INTERVAL_SEC, WORKER_IDLE_TIMEOUT_MS,
};
use entities::enums::OwnerType;
use entities::models::Updated;
use log::error;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::{AssetDynamicDetails, AssetOwner};
use rocks_db::columns::{Mint, TokenAccount};
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::Instant;

#[derive(Clone)]
pub struct TokenAccsProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,
    pub db_client: Arc<DBClient>,

    pub buffer: Arc<Buffer>,
    pub metrics: Arc<IngesterMetricsConfig>,
    last_received_mint_at: Option<SystemTime>,
    last_received_token_acc_at: Option<SystemTime>,
}

#[derive(Default)]
struct DynamicAndAssetOwnerDetails {
    pub asset_dynamic_details: HashMap<Pubkey, AssetDynamicDetails>,
    pub asset_owner_details: HashMap<Pubkey, AssetOwner>,
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
            last_received_mint_at: None,
            last_received_token_acc_at: None,
        }
    }

    pub async fn process_token_accs(&mut self, keep_running: Arc<AtomicBool>) {
        while keep_running.load(Ordering::SeqCst) {
            let buffer_len = self.buffer.token_accs.lock().await.len();
            if buffer_len < self.batch_size {
                // sleep only in case when buffer is empty or n seconds passed since last insert
                if buffer_len == 0
                    || self.last_received_token_acc_at.is_some_and(|t| {
                        t.elapsed().is_ok_and(|e| e.as_secs() < FLUSH_INTERVAL_SEC)
                    })
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(WORKER_IDLE_TIMEOUT_MS))
                        .await;
                    continue;
                }
            }

            let mut max_slot = 0;
            let accs_to_save = {
                let mut token_accounts = self.buffer.token_accs.lock().await;
                let mut elems = Vec::new();

                for key in token_accounts
                    .keys()
                    .take(self.batch_size)
                    .cloned()
                    .collect::<Vec<Vec<u8>>>()
                {
                    if let Some(value) = token_accounts.remove(&key) {
                        if value.slot_updated > max_slot {
                            max_slot = value.slot_updated;
                        }
                        elems.push(value);
                    }
                }

                elems
            };

            self.transform_and_save_token_accs(&accs_to_save).await;

            self.metrics
                .set_last_processed_slot("spl_token_acc", max_slot);
        }

        self.last_received_token_acc_at = Some(SystemTime::now());
    }

    pub async fn transform_and_save_token_accs(&self, accs_to_save: &[TokenAccount]) {
        let dynamic_and_asset_owner_details = accs_to_save.to_owned().clone().into_iter().fold(
            DynamicAndAssetOwnerDetails::default(),
            |mut accumulated_asset_info: DynamicAndAssetOwnerDetails, token_account| {
                accumulated_asset_info.asset_owner_details.insert(
                    token_account.mint,
                    AssetOwner {
                        pubkey: token_account.mint,
                        owner: Updated::new(
                            token_account.slot_updated as u64,
                            None,
                            token_account.owner,
                        ),
                        delegate: Updated::new(
                            token_account.slot_updated as u64,
                            None,
                            token_account.delegate,
                        ),
                        owner_type: Updated::default(),
                        owner_delegate_seq: Updated::new(
                            token_account.slot_updated as u64,
                            None,
                            None,
                        ),
                    },
                );

                accumulated_asset_info.asset_dynamic_details.insert(
                    token_account.mint,
                    AssetDynamicDetails {
                        pubkey: token_account.mint,
                        is_frozen: Updated::new(
                            token_account.slot_updated as u64,
                            None,
                            token_account.frozen,
                        ),
                        ..Default::default()
                    },
                );

                accumulated_asset_info
            },
        );

        let begin_processing = Instant::now();
        let res = self
            .rocks_db
            .asset_owner_data
            .merge_batch(dynamic_and_asset_owner_details.asset_owner_details)
            .await;

        result_to_metrics(self.metrics.clone(), &res, "accounts_saving_owner");

        let res = self
            .rocks_db
            .asset_dynamic_data
            .merge_batch(dynamic_and_asset_owner_details.asset_dynamic_details)
            .await;

        result_to_metrics(self.metrics.clone(), &res, "accounts_updating_is_frozen");

        accs_to_save.iter().for_each(|acc| {
            let upd_res = self
                .rocks_db
                .asset_updated(acc.slot_updated as u64, acc.mint);

            if let Err(e) = upd_res {
                error!("Error while updating assets update idx: {}", e);
            }
        });

        self.metrics.set_latency(
            "token_accounts_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
    }

    pub async fn process_mint_accs(&mut self, keep_running: Arc<AtomicBool>) {
        while keep_running.load(Ordering::SeqCst) {
            let buffer_len = self.buffer.mints.lock().await.len();
            // batch_size for flushing mint 5 times smaller than batch_size for token accounts
            if buffer_len < self.batch_size / 5 {
                // sleep only in case when buffer is empty or n seconds passed since last insert
                if buffer_len == 0
                    || self.last_received_mint_at.is_some_and(|t| {
                        t.elapsed().is_ok_and(|e| e.as_secs() < FLUSH_INTERVAL_SEC)
                    })
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(WORKER_IDLE_TIMEOUT_MS))
                        .await;
                    continue;
                }
            }

            let mut max_slot = 0;
            let mint_accs_to_save = {
                let mut mint_accounts = self.buffer.mints.lock().await;
                let mut elems = Vec::new();

                for key in mint_accounts
                    .keys()
                    .take(self.batch_size)
                    .cloned()
                    .collect::<Vec<Vec<u8>>>()
                {
                    if let Some(value) = mint_accounts.remove(&key) {
                        if value.slot_updated > max_slot {
                            max_slot = value.slot_updated;
                        }
                        elems.push(value);
                    }
                }
                elems
            };

            self.transform_and_save_mint_accs(&mint_accs_to_save).await;

            self.metrics.set_last_processed_slot("spl_mint", max_slot);

            self.last_received_mint_at = Some(SystemTime::now());
        }
    }

    pub async fn transform_and_save_mint_accs(&self, mint_accs_to_save: &[Mint]) {
        let dynamic_and_asset_owner_details =
            mint_accs_to_save.to_owned().clone().into_iter().fold(
                DynamicAndAssetOwnerDetails::default(),
                |mut accumulated_asset_info: DynamicAndAssetOwnerDetails, mint| {
                    accumulated_asset_info.asset_dynamic_details.insert(
                        mint.pubkey,
                        AssetDynamicDetails {
                            pubkey: mint.pubkey,
                            supply: Some(Updated::new(
                                mint.slot_updated as u64,
                                None,
                                mint.supply as u64,
                            )),
                            ..Default::default()
                        },
                    );

                    let owner_type_value = if mint.supply > 1 {
                        OwnerType::Token
                    } else {
                        OwnerType::Single
                    };

                    accumulated_asset_info.asset_owner_details.insert(
                        mint.pubkey,
                        AssetOwner {
                            pubkey: mint.pubkey,
                            owner_type: Updated::new(
                                mint.slot_updated as u64,
                                None,
                                owner_type_value,
                            ),
                            ..Default::default()
                        },
                    );

                    accumulated_asset_info
                },
            );

        let begin_processing = Instant::now();

        let res = self
            .rocks_db
            .asset_dynamic_data
            .merge_batch(dynamic_and_asset_owner_details.asset_dynamic_details)
            .await;

        result_to_metrics(self.metrics.clone(), &res, "accounts_saving_owner");

        let res = self
            .rocks_db
            .asset_owner_data
            .merge_batch(dynamic_and_asset_owner_details.asset_owner_details)
            .await;

        result_to_metrics(self.metrics.clone(), &res, "owner_type_update");

        mint_accs_to_save.iter().for_each(|mint| {
            let upd_res = self
                .rocks_db
                .asset_updated(mint.slot_updated as u64, mint.pubkey);

            if let Err(e) = upd_res {
                error!("Error while updating assets update idx: {}", e);
            }
        });

        self.metrics.set_latency(
            "mint_accounts_saving",
            begin_processing.elapsed().as_millis() as f64,
        );
    }
}
