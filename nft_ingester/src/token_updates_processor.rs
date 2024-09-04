use entities::enums::OwnerType;
use entities::models::{
    Mint, PubkeyWithSlot, TokenAccount, TokenAccountMintOwnerIdxKey, TokenAccountOwnerIdxKey,
    UpdateVersion, Updated,
};
use metrics_utils::IngesterMetricsConfig;
use num_traits::Zero;
use rocks_db::asset::{AssetDynamicDetails, AssetOwner};
use rocks_db::errors::StorageError;
use rocks_db::token_accounts::{TokenAccountMintOwnerIdx, TokenAccountOwnerIdx};
use rocks_db::Storage;
use solana_program::pubkey::Pubkey;
use std::sync::Arc;
use tokio::time::Instant;
use usecase::save_metrics::result_to_metrics;

#[derive(Clone)]
pub struct TokenAccountsProcessor {
    pub rocks_db: Arc<Storage>,
    pub metrics: Arc<IngesterMetricsConfig>,
}

impl TokenAccountsProcessor {
    pub fn new(rocks_db: Arc<Storage>, metrics: Arc<IngesterMetricsConfig>) -> Self {
        Self { rocks_db, metrics }
    }

    fn finalize_processing<T, F>(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        operation: F,
        asset_update: PubkeyWithSlot,
        metric_name: &str,
    ) -> Result<(), StorageError>
    where
        F: Fn(&mut rocksdb::WriteBatchWithTransaction<false>) -> Result<T, StorageError>,
    {
        let begin_processing = Instant::now();
        operation(db_batch)?;
        let res = self.rocks_db.asset_updated_with_batch(
            db_batch,
            asset_update.slot,
            asset_update.pubkey,
        );
        self.metrics
            .set_latency(metric_name, begin_processing.elapsed().as_millis() as f64);
        result_to_metrics(self.metrics.clone(), &res, metric_name);
        res
    }

    pub fn transform_and_save_token_account(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: Pubkey,
        token_account: &TokenAccount,
    ) -> Result<(), StorageError> {
        self.save_token_account_with_idxs(db_batch, key, token_account)?;
        let asset_owner_details = AssetOwner {
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
        };
        let asset_dynamic_details = AssetDynamicDetails {
            pubkey: token_account.mint,
            is_frozen: Updated::new(
                token_account.slot_updated as u64,
                Some(UpdateVersion::WriteVersion(token_account.write_version)),
                token_account.frozen,
            ),
            ..Default::default()
        };

        self.finalize_processing(
            db_batch,
            |batch| {
                self.rocks_db.asset_owner_data.merge_with_batch(
                    batch,
                    token_account.mint,
                    &asset_owner_details,
                )?;
                self.rocks_db.asset_dynamic_data.merge_with_batch(
                    batch,
                    token_account.mint,
                    &asset_dynamic_details,
                )
            },
            PubkeyWithSlot {
                pubkey: token_account.mint,
                slot: token_account.slot_updated as u64,
            },
            "token_accounts_asset_saving",
        )
    }

    pub fn transform_and_save_mint_account(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        mint: &Mint,
    ) -> Result<(), StorageError> {
        let asset_dynamic_details = AssetDynamicDetails {
            pubkey: mint.pubkey,
            supply: Some(Updated::new(
                mint.slot_updated as u64,
                Some(UpdateVersion::WriteVersion(mint.write_version)),
                mint.supply as u64,
            )),
            ..Default::default()
        };
        let owner_type_value = if mint.supply > 1 {
            OwnerType::Token
        } else {
            OwnerType::Single
        };
        let asset_owner_details = AssetOwner {
            pubkey: mint.pubkey,
            owner_type: Updated::new(
                mint.slot_updated as u64,
                Some(UpdateVersion::WriteVersion(mint.write_version)),
                owner_type_value,
            ),
            ..Default::default()
        };

        self.finalize_processing(
            db_batch,
            |batch| {
                self.rocks_db.asset_dynamic_data.merge_with_batch(
                    batch,
                    mint.pubkey,
                    &asset_dynamic_details,
                )?;
                self.rocks_db.asset_owner_data.merge_with_batch(
                    batch,
                    mint.pubkey,
                    &asset_owner_details,
                )
            },
            PubkeyWithSlot {
                pubkey: mint.pubkey,
                slot: mint.slot_updated as u64,
            },
            "mint_accounts_saving",
        )
    }

    pub fn save_token_account_with_idxs(
        &self,
        db_batch: &mut rocksdb::WriteBatchWithTransaction<false>,
        key: Pubkey,
        token_account: &TokenAccount,
    ) -> Result<(), StorageError> {
        self.finalize_processing(
            db_batch,
            |batch| {
                self.rocks_db
                    .token_accounts
                    .merge_with_batch(batch, key, token_account)?;
                self.rocks_db.token_account_owner_idx.merge_with_batch(
                    batch,
                    TokenAccountOwnerIdxKey {
                        owner: token_account.owner,
                        token_account: token_account.pubkey,
                    },
                    &TokenAccountOwnerIdx {
                        is_zero_balance: token_account.amount.is_zero(),
                        write_version: token_account.write_version,
                    },
                )?;
                self.rocks_db.token_account_mint_owner_idx.merge_with_batch(
                    batch,
                    TokenAccountMintOwnerIdxKey {
                        mint: token_account.mint,
                        owner: token_account.owner,
                        token_account: token_account.pubkey,
                    },
                    &TokenAccountMintOwnerIdx {
                        is_zero_balance: token_account.amount.is_zero(),
                        write_version: token_account.write_version,
                    },
                )
            },
            PubkeyWithSlot {
                pubkey: token_account.mint,
                slot: token_account.slot_updated as u64,
            },
            "token_accounts_saving",
        )
    }
}
