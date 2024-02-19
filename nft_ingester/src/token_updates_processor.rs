use crate::buffer::Buffer;
use crate::mplx_updates_processor::result_to_metrics;
use crate::process_accounts;
use entities::models::Updated;
use futures::future;
use log::error;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::{AssetDynamicDetails, AssetOwner};
use rocks_db::columns::{Mint, TokenAccount};
use rocks_db::errors::StorageError;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::Instant;

#[derive(Clone)]
pub struct TokenAccsProcessor {
    pub rocks_db: Arc<Storage>,
    pub batch_size: usize,

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
        buffer: Arc<Buffer>,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        Self {
            rocks_db,
            buffer,
            metrics,
            batch_size,
            last_received_mint_at: None,
            last_received_token_acc_at: None,
        }
    }

    pub async fn process_mint_accs(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.mints,
            self.batch_size / 5,
            |s: Mint| s,
            self.last_received_mint_at,
            Self::transform_and_save_mint_accs,
            "spl_mint"
        );
    }

    pub async fn process_token_accs(&mut self, keep_running: Arc<AtomicBool>) {
        process_accounts!(
            self,
            keep_running,
            self.buffer.token_accs,
            self.batch_size,
            |s: TokenAccount| s,
            self.last_received_token_acc_at,
            Self::transform_and_save_token_accs,
            "spl_token_acc"
        );
    }

    async fn finalize_processing<T, F>(
        &self,
        operation: F,
        asset_updates: Vec<(u64, Pubkey)>,
        metric_name_latency: &str,
        metric_name_result: &str,
    ) where
        F: Future<Output = Result<T, StorageError>>,
    {
        let begin_processing = Instant::now();
        let result = operation.await;

        asset_updates.into_iter().for_each(|(slot, pk)| {
            let upd_res = self.rocks_db.asset_updated(slot, pk);

            if let Err(e) = upd_res {
                error!("Error while updating assets update idx: {}", e);
            }
        });

        self.metrics.set_latency(
            metric_name_latency,
            begin_processing.elapsed().as_millis() as f64,
        );
        result_to_metrics(self.metrics.clone(), &result, metric_name_result);
    }

    pub async fn transform_and_save_token_accs(
        &self,
        accs_to_save: &HashMap<Vec<u8>, TokenAccount>,
    ) {
        let dynamic_and_asset_owner_details = accs_to_save.clone().into_values().fold(
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

        self.finalize_processing(
            future::try_join(
                self.rocks_db
                    .asset_owner_data
                    .merge_batch(dynamic_and_asset_owner_details.asset_owner_details),
                self.rocks_db
                    .asset_dynamic_data
                    .merge_batch(dynamic_and_asset_owner_details.asset_dynamic_details),
            ),
            accs_to_save
                .values()
                .map(|a| (a.slot_updated as u64, a.mint))
                .collect::<Vec<_>>(),
            "token_accounts_saving",
            "accounts_saving_owner",
        )
        .await
    }

    pub async fn transform_and_save_mint_accs(&self, mint_accs_to_save: &HashMap<Vec<u8>, Mint>) {
        let save_values = mint_accs_to_save.clone().into_values().fold(
            HashMap::new(),
            |mut acc: HashMap<_, _>, mint| {
                acc.insert(
                    mint.pubkey,
                    AssetDynamicDetails {
                        pubkey: mint.pubkey,
                        supply: Some(Updated::new(
                            mint.slot_updated as u64,
                            None,
                            mint.supply as u64,
                        )),
                        seq: Some(Updated::new(
                            mint.slot_updated as u64,
                            None,
                            mint.slot_updated as u64,
                        )),
                        ..Default::default()
                    },
                );
                acc
            },
        );

        self.finalize_processing(
            self.rocks_db.asset_dynamic_data.merge_batch(save_values),
            mint_accs_to_save
                .values()
                .map(|a| (a.slot_updated as u64, a.pubkey))
                .collect::<Vec<_>>(),
            "mint_accounts_saving",
            "accounts_saving_dynamic_data",
        )
        .await
    }
}
