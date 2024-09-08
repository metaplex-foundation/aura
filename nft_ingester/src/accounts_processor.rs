use crate::error::IngesterError;
use crate::inscriptions_processor::InscriptionsProcessor;
use crate::mpl_core_fee_indexing_processor::MplCoreFeeProcessor;
use crate::mpl_core_processor::MplCoreProcessor;
use crate::mplx_updates_processor::MplxAccountsProcessor;
use crate::token_updates_processor::TokenAccountsProcessor;
use entities::enums::UnprocessedAccount;
use interface::unprocessed_data_getter::UnprocessedAccountsGetter;
use metrics_utils::IngesterMetricsConfig;
use postgre_client::PgClient;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::Storage;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::error;

// worker idle timeout
const WORKER_IDLE_TIMEOUT_MS: u64 = 100;
// interval after which buffer is flushed
const FLUSH_INTERVAL_SEC: u64 = 1;

pub struct AccountsProcessor<T: UnprocessedAccountsGetter> {
    fees_batch_size: usize,
    unprocessed_account_getter: Arc<T>,
    mplx_accounts_processor: MplxAccountsProcessor,
    token_accounts_processor: TokenAccountsProcessor,
    mpl_core_processor: MplCoreProcessor,
    inscription_processor: InscriptionsProcessor,
    core_fees_processor: MplCoreFeeProcessor,
    batch_storage: Rc<RefCell<BatchSaveStorage>>,
}

// Need to manually implement Send trait here, because Rc<RefCell<T>>
// which used inside processors are not Send by default.
// Here it is safe because batch_storage used only inside thread where it was created

// An alternative way is to use Arc<Mutex<T>> instead of Rc<RefCell<T>>.
// If do so, we will not need to manually implement the Send trait
// but using Mutex instead of RefCell will cause a performance decrease
unsafe impl<T: UnprocessedAccountsGetter> Send for AccountsProcessor<T> {}

#[allow(clippy::too_many_arguments)]
impl<T: UnprocessedAccountsGetter> AccountsProcessor<T> {
    pub async fn build(
        rx: Receiver<()>,
        accounts_batch_size: usize,
        fees_batch_size: usize,
        storage: Arc<Storage>,
        unprocessed_account_getter: Arc<T>,
        metrics: Arc<IngesterMetricsConfig>,
        postgre_client: Arc<PgClient>,
        rpc_client: Arc<RpcClient>,
        join_set: Arc<Mutex<JoinSet<Result<(), tokio::task::JoinError>>>>,
    ) -> Result<Self, IngesterError> {
        // Use shared BatchSaveStorage across all processors
        // to put all processing accounts into the same batch object,
        // so accounts of all types will be processed with the same speed
        let batch_storage = Rc::new(RefCell::new(BatchSaveStorage::new(
            storage,
            accounts_batch_size,
        )));
        let mplx_accounts_processor =
            MplxAccountsProcessor::new(batch_storage.clone(), metrics.clone());
        let token_accounts_processor =
            TokenAccountsProcessor::new(batch_storage.clone(), metrics.clone());
        let mpl_core_processor = MplCoreProcessor::new(batch_storage.clone(), metrics.clone());
        let inscription_processor =
            InscriptionsProcessor::new(batch_storage.clone(), metrics.clone());
        let core_fees_processor =
            MplCoreFeeProcessor::build(postgre_client, metrics.clone(), rpc_client, join_set)
                .await?;
        core_fees_processor.update_rent(rx).await;

        Ok(Self {
            fees_batch_size,
            unprocessed_account_getter,
            mplx_accounts_processor,
            token_accounts_processor,
            mpl_core_processor,
            inscription_processor,
            core_fees_processor,
            batch_storage,
        })
    }

    pub async fn process_accounts(&mut self, rx: Receiver<()>) {
        let mut core_fees = HashMap::new();
        let mut ack_ids = Vec::new();
        let mut last_flushed_at = SystemTime::now();
        while rx.is_empty() {
            if last_flushed_at
                .elapsed()
                .is_ok_and(|e| e.as_secs() >= FLUSH_INTERVAL_SEC)
            {
                self.flush(&mut ack_ids, &mut last_flushed_at);
            };
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
                            unprocessed_account.key,
                            &metadata_info,
                        ),
                    UnprocessedAccount::Token(token_account) => self
                        .token_accounts_processor
                        .transform_and_save_token_account(unprocessed_account.key, &token_account),
                    UnprocessedAccount::Mint(mint) => self
                        .token_accounts_processor
                        .transform_and_save_mint_account(&mint),
                    UnprocessedAccount::Edition(edition) => self
                        .mplx_accounts_processor
                        .transform_and_store_edition_account(
                            unprocessed_account.key,
                            &edition.edition,
                        ),
                    UnprocessedAccount::BurnMetadata(burn_metadata) => self
                        .mplx_accounts_processor
                        .transform_and_store_burnt_metadata(
                            unprocessed_account.key,
                            &burn_metadata,
                        ),
                    UnprocessedAccount::BurnMplCore(burn_mpl_core) => {
                        self.mpl_core_processor.transform_and_store_burnt_mpl_asset(
                            unprocessed_account.key,
                            &burn_mpl_core,
                        )
                    }
                    UnprocessedAccount::MplCore(mpl_core) => self
                        .mpl_core_processor
                        .transform_and_store_mpl_asset(unprocessed_account.key, &mpl_core),
                    UnprocessedAccount::Inscription(inscription) => {
                        self.inscription_processor.store_inscription(&inscription)
                    }
                    UnprocessedAccount::InscriptionData(inscription_data) => self
                        .inscription_processor
                        .store_inscription_data(unprocessed_account.key, &inscription_data),
                    UnprocessedAccount::MplCoreFee(core_fee) => {
                        core_fees.insert(unprocessed_account.key, core_fee);
                        Ok(())
                    }
                };
                if let Err(err) = processing_result {
                    error!("Processing account {}: {}", unprocessed_account.key, err);
                    continue;
                }
                ack_ids.push(unprocessed_account.id);
                if self.batch_storage.borrow().batch_filled() {
                    self.flush(&mut ack_ids, &mut last_flushed_at);
                }
                if core_fees.len() > self.fees_batch_size {
                    self.core_fees_processor
                        .store_mpl_assets_fee(&std::mem::take(&mut core_fees))
                        .await;
                }
            }
            self.flush(&mut ack_ids, &mut last_flushed_at);
            self.core_fees_processor
                .store_mpl_assets_fee(&std::mem::take(&mut core_fees))
                .await;
        }
    }

    fn flush(&self, ack_ids: &mut Vec<String>, last_flushed_at: &mut SystemTime) {
        let write_batch_result = self.batch_storage.borrow_mut().flush();
        match write_batch_result {
            Ok(_) => {
                self.unprocessed_account_getter.ack(std::mem::take(ack_ids));
            }
            Err(err) => {
                error!("Write batch: {}", err);
                ack_ids.clear();
            }
        }
        *last_flushed_at = SystemTime::now();
    }
}
