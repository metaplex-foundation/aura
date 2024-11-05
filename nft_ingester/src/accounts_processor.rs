use crate::consistency_calculator::NftChangesTracker;
use crate::error::IngesterError;
use crate::inscriptions_processor::InscriptionsProcessor;
use crate::mpl_core_fee_indexing_processor::MplCoreFeeProcessor;
use crate::mpl_core_processor::MplCoreProcessor;
use crate::mplx_updates_processor::MplxAccountsProcessor;
use crate::token_updates_processor::TokenAccountsProcessor;
use entities::enums::UnprocessedAccount;
use entities::models::{CoreAssetFee, UnprocessedAccountMessage};
use interface::unprocessed_data_getter::UnprocessedAccountsGetter;
use metrics_utils::IngesterMetricsConfig;
use postgre_client::PgClient;
use rocks_db::batch_savers::BatchSaveStorage;
use rocks_db::Storage;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use tracing::error;

// worker idle timeout
const WORKER_IDLE_TIMEOUT: Duration = Duration::from_millis(100);
// interval after which buffer is flushed
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);

#[allow(clippy::too_many_arguments)]
pub async fn run_accounts_processor<AG: UnprocessedAccountsGetter + Sync + Send + 'static>(
    rx: Receiver<()>,
    mutexed_tasks: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
    unprocessed_transactions_getter: Arc<AG>,
    rocks_storage: Arc<Storage>,
    account_buffer_size: usize,
    fees_buffer_size: usize,
    metrics: Arc<IngesterMetricsConfig>,
    postgre_client: Arc<PgClient>,
    rpc_client: Arc<RpcClient>,
    nft_changes_tracker: Arc<NftChangesTracker>,
    join_set: Arc<Mutex<JoinSet<Result<(), JoinError>>>>,
) {
    mutexed_tasks.lock().await.spawn(async move {
        let account_processor = AccountsProcessor::build(
            rx.resubscribe(),
            fees_buffer_size,
            unprocessed_transactions_getter,
            metrics,
            postgre_client,
            rpc_client,
            join_set,
        )
        .await
        .expect("Failed to build 'AccountsProcessor'!");

        account_processor
            .process_accounts(rx, rocks_storage, account_buffer_size, nft_changes_tracker)
            .await;

        Ok(())
    });
}

pub struct AccountsProcessor<T: UnprocessedAccountsGetter> {
    fees_batch_size: usize,
    unprocessed_account_getter: Arc<T>,
    mplx_accounts_processor: MplxAccountsProcessor,
    token_accounts_processor: TokenAccountsProcessor,
    mpl_core_processor: MplCoreProcessor,
    inscription_processor: InscriptionsProcessor,
    core_fees_processor: MplCoreFeeProcessor,
    metrics: Arc<IngesterMetricsConfig>,
}

// AccountsProcessor responsible for processing all account updates received
// from Geyser plugin.
// unprocessed_account_getter field represents a source of parsed accounts
// It can be either Redis or Buffer filled by TCP-stream.
// Each account is process by corresponding processor
#[allow(clippy::too_many_arguments)]
impl<T: UnprocessedAccountsGetter> AccountsProcessor<T> {
    pub async fn build(
        rx: Receiver<()>,
        fees_batch_size: usize,
        unprocessed_account_getter: Arc<T>,
        metrics: Arc<IngesterMetricsConfig>,
        postgre_client: Arc<PgClient>,
        rpc_client: Arc<RpcClient>,
        join_set: Arc<Mutex<JoinSet<Result<(), tokio::task::JoinError>>>>,
    ) -> Result<Self, IngesterError> {
        let mplx_accounts_processor = MplxAccountsProcessor::new(metrics.clone());
        let token_accounts_processor = TokenAccountsProcessor::new(metrics.clone());
        let mpl_core_processor = MplCoreProcessor::new(metrics.clone());
        let inscription_processor = InscriptionsProcessor::new(metrics.clone());
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
            metrics,
        })
    }

    pub async fn process_accounts(
        &self,
        rx: Receiver<()>,
        storage: Arc<Storage>,
        accounts_batch_size: usize,
        nft_changes_tracker: Arc<NftChangesTracker>,
    ) {
        let mut batch_storage =
            BatchSaveStorage::new(storage, accounts_batch_size, self.metrics.clone());
        let mut core_fees = HashMap::new();
        let mut ack_ids = Vec::new();
        let mut interval = tokio::time::interval(FLUSH_INTERVAL);
        let mut batch_fill_instant = Instant::now();
        while rx.is_empty() {
            tokio::select! {
                unprocessed_accounts = self.unprocessed_account_getter.next_accounts(accounts_batch_size) => {
                        let unprocessed_accounts = match unprocessed_accounts {
                            Ok(unprocessed_accounts) => unprocessed_accounts,
                            Err(err) => {
                                error!("Get unprocessed accounts: {}", err);
                                tokio::time::sleep(WORKER_IDLE_TIMEOUT)
                                    .await;
                                continue;
                            }
                        };
                        self.process_account(&mut batch_storage, unprocessed_accounts, &mut core_fees, &mut ack_ids, &mut interval, &mut batch_fill_instant, &nft_changes_tracker).await;
                    },
                _ = interval.tick() => {
                    self.flush(&mut batch_storage, &mut ack_ids, &mut interval, &mut batch_fill_instant);
                    self.core_fees_processor.store_mpl_assets_fee(&std::mem::take(&mut core_fees)).await;
                }
            }
        }
        self.flush(
            &mut batch_storage,
            &mut ack_ids,
            &mut interval,
            &mut batch_fill_instant,
        );
        self.core_fees_processor
            .store_mpl_assets_fee(&std::mem::take(&mut core_fees))
            .await;
    }

    async fn process_account(
        &self,
        batch_storage: &mut BatchSaveStorage,
        unprocessed_accounts: Vec<UnprocessedAccountMessage>,
        core_fees: &mut HashMap<Pubkey, CoreAssetFee>,
        ack_ids: &mut Vec<String>,
        interval: &mut tokio::time::Interval,
        batch_fill_instant: &mut Instant,
        nft_changes_tracker: &NftChangesTracker,
    ) {
        for unprocessed_account in unprocessed_accounts {
            let processing_result = match &unprocessed_account.account {
                UnprocessedAccount::MetadataInfo(metadata_info) => self
                    .mplx_accounts_processor
                    .transform_and_store_metadata_account(
                        batch_storage,
                        unprocessed_account.key,
                        metadata_info,
                    ),
                UnprocessedAccount::Token(token_account) => self
                    .token_accounts_processor
                    .transform_and_save_token_account(
                        batch_storage,
                        unprocessed_account.key,
                        token_account,
                    ),
                UnprocessedAccount::Mint(mint) => self
                    .token_accounts_processor
                    .transform_and_save_mint_account(batch_storage, mint),
                UnprocessedAccount::Edition(edition) => self
                    .mplx_accounts_processor
                    .transform_and_store_edition_account(
                        batch_storage,
                        unprocessed_account.key,
                        &edition.edition,
                    ),
                UnprocessedAccount::BurnMetadata(burn_metadata) => self
                    .mplx_accounts_processor
                    .transform_and_store_burnt_metadata(
                        batch_storage,
                        unprocessed_account.key,
                        burn_metadata,
                    ),
                UnprocessedAccount::BurnMplCore(burn_mpl_core) => {
                    self.mpl_core_processor.transform_and_store_burnt_mpl_asset(
                        batch_storage,
                        unprocessed_account.key,
                        burn_mpl_core,
                    )
                }
                UnprocessedAccount::MplCore(mpl_core) => {
                    self.mpl_core_processor.transform_and_store_mpl_asset(
                        batch_storage,
                        unprocessed_account.key,
                        mpl_core,
                    )
                }
                UnprocessedAccount::Inscription(inscription) => self
                    .inscription_processor
                    .store_inscription(batch_storage, inscription),
                UnprocessedAccount::InscriptionData(inscription_data) => {
                    self.inscription_processor.store_inscription_data(
                        batch_storage,
                        unprocessed_account.key,
                        inscription_data,
                    )
                }
                UnprocessedAccount::MplCoreFee(core_fee) => {
                    core_fees.insert(unprocessed_account.key, core_fee.clone());
                    Ok(())
                }
            };
            if let Err(err) = processing_result {
                error!("Processing account {}: {}", unprocessed_account.key, err);
                continue;
            }
            {
                let (account_pubkey, slot, write_version, data_hash) =
                    unprocessed_account.solana_change_info();
                nft_changes_tracker
                    .track_account_change(
                        batch_storage,
                        account_pubkey,
                        slot,
                        write_version,
                        data_hash,
                    )
                    .await;
            }
            self.metrics
                .inc_accounts(unprocessed_account.account.into());
            ack_ids.push(unprocessed_account.id);
            if batch_storage.batch_filled() {
                self.flush(batch_storage, ack_ids, interval, batch_fill_instant);
            }
            if core_fees.len() > self.fees_batch_size {
                self.core_fees_processor
                    .store_mpl_assets_fee(&std::mem::take(core_fees))
                    .await;
            }
        }
    }

    fn flush(
        &self,
        storage: &mut BatchSaveStorage,
        ack_ids: &mut Vec<String>,
        interval: &mut tokio::time::Interval,
        batch_fill_instant: &mut Instant,
    ) {
        let write_batch_result = storage.flush();
        match write_batch_result {
            Ok(_) => {
                self.unprocessed_account_getter.ack(std::mem::take(ack_ids));
            }
            Err(err) => {
                error!("Write batch: {}", err);
                ack_ids.clear();
            }
        }
        interval.reset();
        self.metrics.set_latency(
            "accounts_batch_filling",
            batch_fill_instant.elapsed().as_millis() as f64,
        );
        *batch_fill_instant = Instant::now();
    }
}
