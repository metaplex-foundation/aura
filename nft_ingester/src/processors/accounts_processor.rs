use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use entities::{
    enums::UnprocessedAccount,
    models::{CoreAssetFee, UnprocessedAccountMessage},
};
use interface::{
    error::{MessengerError, UsecaseError},
    unprocessed_data_getter::{AccountSource, UnprocessedAccountsGetter},
};
use metrics_utils::{IngesterMetricsConfig, MessageProcessMetricsConfig};
use postgre_client::PgClient;
use rocks_db::{batch_savers::BatchSaveStorage, Storage};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use uuid::Uuid;

use super::account_based::{
    inscriptions_processor::InscriptionsProcessor,
    mpl_core_fee_indexing_processor::MplCoreFeeProcessor, mpl_core_processor::MplCoreProcessor,
    mplx_updates_processor::MplxAccountsProcessor, token_updates_processor::TokenAccountsProcessor,
};
use crate::{error::IngesterError, redis_receiver::get_timestamp_from_id};

// regular stream worker idle timeout
const STREAM_WORKER_IDLE_TIMEOUT: Duration = Duration::from_millis(100);
// backfill worker idle timeout for snapshot parsing
const BACKFILL_WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
// interval after which buffer is flushed
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);
// interval to try & build account processor if the previous build fails
const ACCOUNT_PROCESSOR_RESTART_INTERVAL: Duration = Duration::from_secs(5);

// EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
pub const USDC_MINT_BYTES: [u8; 32] = [
    198, 250, 122, 243, 190, 219, 173, 58, 61, 101, 243, 106, 171, 201, 116, 49, 177, 187, 228,
    194, 210, 246, 224, 228, 124, 166, 2, 3, 69, 47, 93, 97,
];
// Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB
pub const USDT_MINT_BYTES: [u8; 32] = [
    206, 1, 14, 96, 175, 237, 178, 39, 23, 189, 99, 25, 47, 84, 20, 90, 63, 150, 90, 51, 187, 130,
    210, 199, 2, 158, 178, 206, 30, 32, 130, 100,
];

lazy_static::lazy_static! {
    pub static ref POPULAR_FUNGIBLE_TOKENS: HashSet<Pubkey> = {
        let mut set = HashSet::new();
        set.insert(Pubkey::new_from_array(USDC_MINT_BYTES));
        set.insert(Pubkey::new_from_array(USDT_MINT_BYTES));
        set
    };
}

#[allow(clippy::too_many_arguments)]
pub fn run_accounts_processor<AG: UnprocessedAccountsGetter + Sync + Send + 'static>(
    cancellation_token: CancellationToken,
    unprocessed_transactions_getter: Arc<AG>,
    rocks_storage: Arc<Storage>,
    account_buffer_size: usize,
    fees_buffer_size: usize,
    metrics: Arc<IngesterMetricsConfig>,
    message_process_metrics: Option<Arc<MessageProcessMetricsConfig>>,
    postgre_client: Arc<PgClient>,
    rpc_client: Arc<RpcClient>,
    processor_name: Option<String>,
    wellknown_fungible_accounts: HashMap<String, String>,
    source: AccountSource,
) {
    usecase::executor::spawn(async move {
        let account_processor = loop {
            match AccountsProcessor::build(
                cancellation_token.child_token(),
                fees_buffer_size,
                unprocessed_transactions_getter.clone(),
                metrics.clone(),
                message_process_metrics.clone(),
                postgre_client.clone(),
                rpc_client.clone(),
                processor_name.clone(),
                wellknown_fungible_accounts.clone(),
            )
            .await
            {
                Ok(processor) => break processor,
                Err(e) => {
                    error!(%e, "Failed to build accounts processor {:?}, retrying in {} seconds...", processor_name.clone(), ACCOUNT_PROCESSOR_RESTART_INTERVAL.as_secs());
                    tokio::time::sleep(ACCOUNT_PROCESSOR_RESTART_INTERVAL).await;
                },
            }
        };

        account_processor
            .process_accounts(
                cancellation_token.child_token(),
                rocks_storage,
                account_buffer_size,
                source,
            )
            .await;
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
    message_process_metrics: Option<Arc<MessageProcessMetricsConfig>>,
    processor_name: String,
    wellknown_fungible_accounts: HashMap<String, String>,
}

// AccountsProcessor responsible for processing all account updates received
// from Geyser plugin.
// unprocessed_account_getter field represents a source of parsed accounts
// It can be either Redis or Buffer filled by TCP-stream.
// Each account is process by corresponding processor
#[allow(clippy::too_many_arguments)]
impl<T: UnprocessedAccountsGetter> AccountsProcessor<T> {
    pub async fn build(
        cancellation_token: CancellationToken,
        fees_batch_size: usize,
        unprocessed_account_getter: Arc<T>,
        metrics: Arc<IngesterMetricsConfig>,
        message_process_metrics: Option<Arc<MessageProcessMetricsConfig>>,
        postgre_client: Arc<PgClient>,
        rpc_client: Arc<RpcClient>,
        processor_name: Option<String>,
        wellknown_fungible_accounts: HashMap<String, String>,
    ) -> Result<Self, IngesterError> {
        let mplx_accounts_processor = MplxAccountsProcessor::new(metrics.clone());
        let token_accounts_processor = TokenAccountsProcessor::new(metrics.clone());
        let mpl_core_processor = MplCoreProcessor::new(metrics.clone());
        let inscription_processor = InscriptionsProcessor::new(metrics.clone());
        let core_fees_processor =
            MplCoreFeeProcessor::build(postgre_client, metrics.clone(), rpc_client).await?;
        core_fees_processor.update_rent(cancellation_token.child_token());

        Ok(Self {
            fees_batch_size,
            unprocessed_account_getter,
            mplx_accounts_processor,
            token_accounts_processor,
            mpl_core_processor,
            inscription_processor,
            core_fees_processor,
            metrics,
            message_process_metrics,
            processor_name: processor_name.unwrap_or_else(|| Uuid::new_v4().to_string()),
            wellknown_fungible_accounts,
        })
    }

    pub async fn process_accounts(
        &self,
        cancellation_token: CancellationToken,
        storage: Arc<Storage>,
        accounts_batch_size: usize,
        source: AccountSource,
    ) {
        let mut batch_storage =
            BatchSaveStorage::new(storage, accounts_batch_size, self.metrics.clone());
        let mut core_fees = HashMap::new();
        let mut ack_ids = Vec::new();
        let mut interval = tokio::time::interval(FLUSH_INTERVAL);
        let mut batch_fill_instant = Instant::now();

        loop {
            tokio::select! {
                unprocessed_accounts = self.unprocessed_account_getter.next_accounts(accounts_batch_size, &source) => {
                        let unprocessed_accounts = match unprocessed_accounts {
                            Ok(unprocessed_accounts) => unprocessed_accounts,
                            Err(err) => {
                                if !matches!(err, UsecaseError::Messenger(MessengerError::Empty(_))) {
                                    error!("Get unprocessed accounts: {}", err);
                                }
                                cancellation_token.run_until_cancelled(async {
                                    match &source {
                                        AccountSource::Stream => tokio::time::sleep(STREAM_WORKER_IDLE_TIMEOUT).await,
                                        AccountSource::Backfill => tokio::time::sleep(BACKFILL_WORKER_IDLE_TIMEOUT).await,
                                    }
                                }).await;
                                continue;
                            }
                        };

                        debug!(
                            processor = %self.processor_name,
                            unprocessed_accounts_len = %unprocessed_accounts.len(),
                            "Processor {}, Unprocessed_accounts: {}  {:?}",
                            self.processor_name,
                            unprocessed_accounts.len(),
                            unprocessed_accounts.iter().map(|account| account.id.to_string()).collect::<Vec<_>>().join(", ")
                        );

                        self.process_account(&mut batch_storage, unprocessed_accounts, &mut core_fees, &mut ack_ids, &mut interval, &mut batch_fill_instant, &source).await;
                    },
                _ = interval.tick() => {
                    self.flush(&mut batch_storage, &mut ack_ids, &mut interval, &mut batch_fill_instant, &source);
                    self.core_fees_processor.store_mpl_assets_fee(&std::mem::take(&mut core_fees)).await;
                }
                _ = cancellation_token.cancelled() => { break; }
            }
        }
        self.flush(
            &mut batch_storage,
            &mut ack_ids,
            &mut interval,
            &mut batch_fill_instant,
            &source,
        );
        self.core_fees_processor.store_mpl_assets_fee(&std::mem::take(&mut core_fees)).await;
    }

    pub async fn process_account(
        &self,
        batch_storage: &mut BatchSaveStorage,
        unprocessed_accounts: Vec<UnprocessedAccountMessage>,
        core_fees: &mut HashMap<Pubkey, CoreAssetFee>,
        ack_ids: &mut Vec<String>,
        interval: &mut tokio::time::Interval,
        batch_fill_instant: &mut Instant,
        source: &AccountSource,
    ) {
        for unprocessed_account in unprocessed_accounts {
            debug!(
                "Process account with Id: {:?} {:?}",
                &unprocessed_account.id, &unprocessed_account.account
            );

            let processing_result = match &unprocessed_account.account {
                UnprocessedAccount::MetadataInfo(metadata_info) => {
                    self.mplx_accounts_processor.transform_and_store_metadata_account(
                        batch_storage,
                        unprocessed_account.key,
                        metadata_info,
                        &self.wellknown_fungible_accounts,
                    )
                },
                UnprocessedAccount::Token(token_account) => {
                    if self
                        .wellknown_fungible_accounts
                        .contains_key(&token_account.mint.to_string())
                    {
                        self.token_accounts_processor.transform_and_save_fungible_token_account(
                            batch_storage,
                            unprocessed_account.key,
                            token_account,
                        )
                    } else {
                        self.token_accounts_processor.transform_and_save_token_account(
                            batch_storage,
                            unprocessed_account.key,
                            token_account,
                        )
                    }
                },
                UnprocessedAccount::Mint(mint) => {
                    self.token_accounts_processor.transform_and_save_mint_account(
                        batch_storage,
                        mint,
                        &self.wellknown_fungible_accounts,
                    )
                },
                UnprocessedAccount::Edition(edition) => {
                    self.mplx_accounts_processor.transform_and_store_edition_account(
                        batch_storage,
                        unprocessed_account.key,
                        &edition.edition,
                    )
                },
                UnprocessedAccount::BurnMetadata(burn_metadata) => {
                    self.mplx_accounts_processor.transform_and_store_burnt_metadata(
                        batch_storage,
                        unprocessed_account.key,
                        burn_metadata,
                    )
                },
                UnprocessedAccount::BurnMplCore(burn_mpl_core) => {
                    self.mpl_core_processor.transform_and_store_burnt_mpl_asset(
                        batch_storage,
                        unprocessed_account.key,
                        burn_mpl_core,
                    )
                },
                UnprocessedAccount::MplCore(mpl_core) => {
                    self.mpl_core_processor.transform_and_store_mpl_asset(
                        batch_storage,
                        unprocessed_account.key,
                        mpl_core,
                    )
                },
                UnprocessedAccount::Inscription(inscription) => {
                    self.inscription_processor.store_inscription(batch_storage, inscription)
                },
                UnprocessedAccount::InscriptionData(inscription_data) => {
                    self.inscription_processor.store_inscription_data(
                        batch_storage,
                        unprocessed_account.key,
                        inscription_data,
                    )
                },
                UnprocessedAccount::MplCoreFee(core_fee) => {
                    core_fees.insert(unprocessed_account.key, core_fee.clone());
                    Ok(())
                },
            };
            if let Err(err) = processing_result {
                error!("Processing account {}: {}", unprocessed_account.key, err);
                continue;
            }
            self.metrics.inc_accounts(unprocessed_account.account.into());

            if let Some(message_process_metrics) = &self.message_process_metrics {
                if let Some(message_timestamp) = get_timestamp_from_id(&unprocessed_account.id) {
                    let current_timestamp = Utc::now().timestamp_millis() as u64;

                    message_process_metrics.set_data_read_time(
                        "accounts",
                        current_timestamp.checked_sub(message_timestamp).unwrap_or_default() as f64,
                    );
                }
            }

            ack_ids.push(unprocessed_account.id);
            if batch_storage.batch_filled() {
                self.flush(batch_storage, ack_ids, interval, batch_fill_instant, source);
            }
            if core_fees.len() > self.fees_batch_size {
                self.core_fees_processor.store_mpl_assets_fee(&std::mem::take(core_fees)).await;
            }
        }
    }

    fn flush(
        &self,
        storage: &mut BatchSaveStorage,
        ack_ids: &mut Vec<String>,
        interval: &mut tokio::time::Interval,
        batch_fill_instant: &mut Instant,
        source: &AccountSource,
    ) {
        let write_batch_result = storage.flush();
        match write_batch_result {
            Ok(_) => {
                self.unprocessed_account_getter.ack(std::mem::take(ack_ids), source);
            },
            Err(err) => {
                error!("Write batch: {}", err);
                ack_ids.clear();
            },
        }
        interval.reset();
        self.metrics
            .set_latency("accounts_batch_filling", batch_fill_instant.elapsed().as_millis() as f64);
        *batch_fill_instant = Instant::now();
    }
}
