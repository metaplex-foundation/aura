use entities::enums::OwnerType;
use entities::models::{Mint, PubkeyWithSlot, TokenAccount, UpdateVersion, Updated};
use metrics_utils::IngesterMetricsConfig;
use rocks_db::asset::{AssetDynamicDetails, AssetOwner};
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::errors::StorageError;
use solana_program::pubkey::Pubkey;
use std::sync::Arc;
use tokio::time::Instant;
use usecase::save_metrics::result_to_metrics;

pub struct TokenAccountsProcessor {
    metrics: Arc<IngesterMetricsConfig>,
}

impl TokenAccountsProcessor {
    pub fn new(metrics: Arc<IngesterMetricsConfig>) -> Self {
        Self { metrics }
    }

    fn finalize_processing<T, F>(
        &self,
        storage: &mut BatchSaveStorage,
        operation: F,
        asset_update: PubkeyWithSlot,
        metric_name: &str,
    ) -> Result<(), StorageError>
    where
        F: Fn(&mut BatchSaveStorage) -> Result<T, StorageError>,
    {
        let begin_processing = Instant::now();
        operation(storage)?;
        let res = storage.asset_updated_with_batch(asset_update.slot, asset_update.pubkey);
        self.metrics
            .set_latency(metric_name, begin_processing.elapsed().as_millis() as f64);
        result_to_metrics(self.metrics.clone(), &res, metric_name);
        res
    }

    pub fn transform_and_save_token_account(
        &self,
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        token_account: &TokenAccount,
    ) -> Result<(), StorageError> {
        self.save_token_account_with_idxs(storage, key, token_account)?;
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

        let metrics = self.metrics.clone();
        self.finalize_processing(
            storage,
            |storage: &mut BatchSaveStorage| {
                storage.store_owner(&asset_owner_details, metrics.clone())?;
                storage.store_dynamic(&asset_dynamic_details, metrics.clone())
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
        storage: &mut BatchSaveStorage,
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

        let metrics = self.metrics.clone();
        self.finalize_processing(
            storage,
            |storage: &mut BatchSaveStorage| {
                storage.store_owner(&asset_owner_details, metrics.clone())?;
                storage.store_dynamic(&asset_dynamic_details, metrics.clone())
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
        storage: &mut BatchSaveStorage,
        key: Pubkey,
        token_account: &TokenAccount,
    ) -> Result<(), StorageError> {
        self.finalize_processing(
            storage,
            |storage: &mut BatchSaveStorage| {
                storage.save_token_account_with_idxs(key, token_account)
            },
            PubkeyWithSlot {
                pubkey: token_account.mint,
                slot: token_account.slot_updated as u64,
            },
            "token_accounts_saving",
        )
    }
}
