use crate::buffer::Buffer;
use crate::process_accounts;
use entities::enums::OwnerType;
use entities::models::{
    PubkeyWithSlot, TokenAccountMintOwnerIdxKey, TokenAccountOwnerIdxKey, UpdateVersion, Updated,
};
use futures::future;
use tracing::error;
use metrics_utils::IngesterMetricsConfig;
use num_traits::Zero;
use rocks_db::asset::{AssetDynamicDetails, AssetOwner};
use rocks_db::columns::{Mint, TokenAccount, TokenAccountMintOwnerIdx, TokenAccountOwnerIdx};
use rocks_db::errors::StorageError;
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast::Receiver;
use tokio::time::Instant;
use usecase::save_metrics::result_to_metrics;

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

    pub async fn process_mint_accs(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
            self.buffer.mints,
            self.batch_size,
            |s: Mint| s,
            self.last_received_mint_at,
            Self::transform_and_save_mint_accs,
            "spl_mint"
        );
    }

    pub async fn process_token_accs(&mut self, rx: Receiver<()>) {
        process_accounts!(
            self,
            rx,
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
        metric_name: &str,
    ) where
        F: Future<Output = Result<T, StorageError>>,
    {
        let begin_processing = Instant::now();
        let result = operation.await;

        if let Err(e) = self.rocks_db.asset_updated_batch(
            asset_updates
                .into_iter()
                .map(|(slot, pubkey)| PubkeyWithSlot { slot, pubkey })
                .collect(),
        ) {
            error!("Error while updating assets update idx: {}", e);
        }

        self.metrics
            .set_latency(metric_name, begin_processing.elapsed().as_millis() as f64);
        result_to_metrics(self.metrics.clone(), &result, metric_name);
    }

    pub async fn transform_and_save_token_accs(
        &self,
        accs_to_save: &HashMap<Pubkey, TokenAccount>,
    ) {
        self.save_token_accounts_with_idxs(accs_to_save).await;
        let dynamic_and_asset_owner_details = accs_to_save.clone().into_values().fold(
            DynamicAndAssetOwnerDetails::default(),
            |mut accumulated_asset_info: DynamicAndAssetOwnerDetails, token_account| {
                // geyser can send us old accounts with zero token balances for some reason
                // we should not process it
                // asset supply we track at mint update
                if token_account.amount.is_zero() {
                    return accumulated_asset_info;
                }
                accumulated_asset_info.asset_owner_details.insert(
                    token_account.mint,
                    AssetOwner {
                        pubkey: token_account.mint,
                        owner: Updated::new(
                            token_account.slot_updated as u64,
                            Some(UpdateVersion::WriteVersion(token_account.write_version)),
                            Some(token_account.owner),
                        ),
                        delegate: Updated::new(
                            token_account.slot_updated as u64,
                            Some(UpdateVersion::WriteVersion(token_account.write_version)),
                            token_account.delegate,
                        ),
                        owner_type: Updated::default(),
                        owner_delegate_seq: Updated::new(
                            token_account.slot_updated as u64,
                            Some(UpdateVersion::WriteVersion(token_account.write_version)),
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
                            Some(UpdateVersion::WriteVersion(token_account.write_version)),
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
            "token_accounts_asset_saving",
        )
        .await
    }

    pub async fn transform_and_save_mint_accs(&self, mint_accs_to_save: &HashMap<Vec<u8>, Mint>) {
        let dynamic_and_asset_owner_details = mint_accs_to_save.clone().into_values().fold(
            DynamicAndAssetOwnerDetails::default(),
            |mut accumulated_asset_info: DynamicAndAssetOwnerDetails, mint| {
                accumulated_asset_info.asset_dynamic_details.insert(
                    mint.pubkey,
                    AssetDynamicDetails {
                        pubkey: mint.pubkey,
                        supply: Some(Updated::new(
                            mint.slot_updated as u64,
                            Some(UpdateVersion::WriteVersion(mint.write_version)),
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
                            Some(UpdateVersion::WriteVersion(mint.write_version)),
                            owner_type_value,
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
                    .asset_dynamic_data
                    .merge_batch(dynamic_and_asset_owner_details.asset_dynamic_details),
                self.rocks_db
                    .asset_owner_data
                    .merge_batch(dynamic_and_asset_owner_details.asset_owner_details),
            ),
            mint_accs_to_save
                .values()
                .map(|a| (a.slot_updated as u64, a.pubkey))
                .collect::<Vec<_>>(),
            "mint_accounts_saving",
        )
        .await
    }

    pub async fn save_token_accounts_with_idxs(
        &self,
        accs_to_save: &HashMap<Pubkey, TokenAccount>,
    ) {
        self.finalize_processing(
            future::try_join3(
                self.rocks_db
                    .token_accounts
                    .merge_batch(accs_to_save.clone()),
                self.rocks_db.token_account_owner_idx.merge_batch(
                    accs_to_save
                        .values()
                        .map(|ta| {
                            (
                                TokenAccountOwnerIdxKey {
                                    owner: ta.owner,
                                    token_account: ta.pubkey,
                                },
                                TokenAccountOwnerIdx {
                                    is_zero_balance: ta.amount.is_zero(),
                                    write_version: ta.write_version,
                                },
                            )
                        })
                        .collect(),
                ),
                self.rocks_db.token_account_mint_owner_idx.merge_batch(
                    accs_to_save
                        .values()
                        .map(|ta| {
                            (
                                TokenAccountMintOwnerIdxKey {
                                    mint: ta.mint,
                                    owner: ta.owner,
                                    token_account: ta.pubkey,
                                },
                                TokenAccountMintOwnerIdx {
                                    is_zero_balance: ta.amount.is_zero(),
                                    write_version: ta.write_version,
                                },
                            )
                        })
                        .collect(),
                ),
            ),
            accs_to_save
                .values()
                .map(|a| (a.slot_updated as u64, a.mint))
                .collect::<Vec<_>>(),
            "token_accounts_saving",
        )
        .await;
    }
}
