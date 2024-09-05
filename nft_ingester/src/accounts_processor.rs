use crate::inscriptions_processor::InscriptionsProcessor;
use crate::mpl_core_processor::MplCoreProcessor;
use crate::mplx_updates_processor::MplxAccountsProcessor;
use crate::token_updates_processor::TokenAccountsProcessor;
use entities::enums::UnprocessedAccount;
use interface::unprocessed_data_getter::UnprocessedAccountsGetter;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::Storage;
use std::sync::Arc;
use tokio::sync::broadcast::Receiver;
use tracing::error;

// interval after which buffer is flushed
const WORKER_IDLE_TIMEOUT_MS: u64 = 100;
// worker idle timeout
const _FLUSH_INTERVAL_SEC: u64 = 5;

#[derive(Clone)]
pub struct AccountsProcessor<T: UnprocessedAccountsGetter> {
    pub batch_size: usize,
    pub rocks_db: Arc<Storage>,
    pub unprocessed_account_getter: Arc<T>,
    pub metrics: Arc<IngesterMetricsConfig>,
    mplx_accounts_processor: Arc<MplxAccountsProcessor>,
    token_accounts_processor: Arc<TokenAccountsProcessor>,
    mpl_core_processor: Arc<MplCoreProcessor>,
    inscription_processor: Arc<InscriptionsProcessor>,
}

impl<T: UnprocessedAccountsGetter> AccountsProcessor<T> {
    pub fn build(
        batch_size: usize,
        rocks_db: Arc<Storage>,
        unprocessed_account_getter: Arc<T>,
        metrics: Arc<IngesterMetricsConfig>,
    ) -> Self {
        let mplx_accounts_processor = Arc::new(MplxAccountsProcessor::new(
            rocks_db.clone(),
            metrics.clone(),
        ));
        let token_accounts_processor = Arc::new(TokenAccountsProcessor::new(
            rocks_db.clone(),
            metrics.clone(),
        ));
        let mpl_core_processor = Arc::new(MplCoreProcessor::new(rocks_db.clone(), metrics.clone()));
        let inscription_processor = Arc::new(InscriptionsProcessor::new(
            rocks_db.clone(),
            metrics.clone(),
        ));

        Self {
            batch_size,
            rocks_db,
            unprocessed_account_getter,
            metrics,
            mplx_accounts_processor,
            token_accounts_processor,
            mpl_core_processor,
            inscription_processor,
        }
    }

    pub async fn process_accounts(&self, rx: Receiver<()>) {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        let mut ack_ids = Vec::new();
        while rx.is_empty() {
            let unprocessed_accounts = match self.unprocessed_account_getter.next_accounts().await {
                Ok(unprocessed_accounts) => unprocessed_accounts,
                Err(err) => {
                    error!("Get unprocessed accounts: {}", err);
                    tokio::time::sleep(tokio::time::Duration::from_millis(WORKER_IDLE_TIMEOUT_MS))
                        .await;
                    continue;
                }
            };
            for unprocessed_account in unprocessed_accounts {
                let processing_result = match unprocessed_account.account {
                    UnprocessedAccount::MetadataInfo(metadata_info) => self
                        .mplx_accounts_processor
                        .transform_and_store_metadata_account(
                            &mut batch,
                            unprocessed_account.key,
                            &metadata_info,
                        ),
                    UnprocessedAccount::Token(token_account) => self
                        .token_accounts_processor
                        .transform_and_save_token_account(
                            &mut batch,
                            unprocessed_account.key,
                            &token_account,
                        ),
                    UnprocessedAccount::Mint(mint) => self
                        .token_accounts_processor
                        .transform_and_save_mint_account(&mut batch, &mint),
                    UnprocessedAccount::Edition(edition) => self
                        .mplx_accounts_processor
                        .transform_and_store_edition_account(
                            &mut batch,
                            unprocessed_account.key,
                            &edition.edition,
                        ),
                    UnprocessedAccount::BurnMetadata(burn_metadata) => self
                        .mplx_accounts_processor
                        .transform_and_store_burnt_metadata(
                            &mut batch,
                            unprocessed_account.key,
                            &burn_metadata,
                        ),
                    UnprocessedAccount::BurnMplCore(burn_mpl_core) => {
                        self.mpl_core_processor.transform_and_store_burnt_mpl_asset(
                            &mut batch,
                            unprocessed_account.key,
                            &burn_mpl_core,
                        )
                    }
                    UnprocessedAccount::MplCore(mpl_core) => {
                        self.mpl_core_processor.transform_and_store_mpl_asset(
                            &mut batch,
                            unprocessed_account.key,
                            &mpl_core,
                        )
                    }
                    UnprocessedAccount::Inscription(inscription) => self
                        .inscription_processor
                        .store_inscription(&mut batch, &inscription),
                    UnprocessedAccount::InscriptionData(inscription_data) => {
                        self.inscription_processor.store_inscription_data(
                            &mut batch,
                            unprocessed_account.key,
                            &inscription_data,
                        )
                    }
                    UnprocessedAccount::MplCoreFee(_) => todo!(),
                };
                if let Err(err) = processing_result {
                    error!("Processing account {}: {}", unprocessed_account.key, err);
                    continue;
                }
                ack_ids.push(unprocessed_account.id);
                if batch.len() >= self.batch_size {
                    let write_batch_result = self.rocks_db.db.write(batch);
                    batch = rocksdb::WriteBatchWithTransaction::<false>::default();
                    match write_batch_result {
                        Ok(_) => {
                            self.unprocessed_account_getter
                                .ack(std::mem::take(&mut ack_ids));
                        }
                        Err(err) => {
                            error!("Write batch: {}", err);
                            ack_ids.clear();
                        }
                    }
                }
            }
            if batch.len() >= self.batch_size {
                let write_batch_result = self.rocks_db.db.write(batch);
                batch = rocksdb::WriteBatchWithTransaction::<false>::default();
                match write_batch_result {
                    Ok(_) => {
                        self.unprocessed_account_getter
                            .ack(std::mem::take(&mut ack_ids));
                    }
                    Err(err) => {
                        error!("Write batch: {}", err);
                        ack_ids.clear();
                    }
                }
            }
        }
    }
}
