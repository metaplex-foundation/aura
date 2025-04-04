use std::{collections::HashMap, sync::Arc};

use entities::{
    enums::UnprocessedAccount,
    models::{CoreAssetFee, UnprocessedAccountMessage},
};
use metrics_utils::IngesterMetricsConfig;
use postgre_client::PgClient;
use rocks_db::batch_savers::BatchSaveStorage;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use tokio_util::sync::CancellationToken;

use super::account_based::{
    inscriptions_processor::InscriptionsProcessor,
    mpl_core_fee_indexing_processor::MplCoreFeeProcessor, mpl_core_processor::MplCoreProcessor,
    mplx_updates_processor::MplxAccountsProcessor, token_updates_processor::TokenAccountsProcessor,
};
use crate::error::IngesterError;

pub struct ProcessorsWrapper {
    pub mplx_accounts_processor: MplxAccountsProcessor,
    pub token_accounts_processor: TokenAccountsProcessor,
    pub mpl_core_processor: MplCoreProcessor,
    pub inscription_processor: InscriptionsProcessor,
    pub core_fees_processor: MplCoreFeeProcessor,
    pub metrics: Arc<IngesterMetricsConfig>,
    pub wellknown_fungible_accounts: HashMap<String, String>,
}

impl ProcessorsWrapper {
    pub async fn build(
        cancellation_token: CancellationToken,
        metrics: Arc<IngesterMetricsConfig>,
        postgre_client: Arc<PgClient>,
        rpc_client: Arc<RpcClient>,
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
            mplx_accounts_processor,
            token_accounts_processor,
            mpl_core_processor,
            inscription_processor,
            core_fees_processor,
            metrics,
            wellknown_fungible_accounts,
        })
    }

    pub fn process_account_by_type(
        &self,
        batch_storage: &mut BatchSaveStorage,
        unprocessed_account: &UnprocessedAccountMessage,
        core_fees: &mut HashMap<Pubkey, CoreAssetFee>,
    ) -> Result<(), IngesterError> {
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
                if self.wellknown_fungible_accounts.contains_key(&token_account.mint.to_string()) {
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
            UnprocessedAccount::MplCore(mpl_core) => self
                .mpl_core_processor
                .transform_and_store_mpl_asset(batch_storage, unprocessed_account.key, mpl_core),
            UnprocessedAccount::Inscription(inscription) => {
                self.inscription_processor.store_inscription(batch_storage, inscription)
            },
            UnprocessedAccount::InscriptionData(inscription_data) => self
                .inscription_processor
                .store_inscription_data(batch_storage, unprocessed_account.key, inscription_data),
            UnprocessedAccount::MplCoreFee(core_fee) => {
                core_fees.insert(unprocessed_account.key, core_fee.clone());
                Ok(())
            },
        };
        processing_result.map_err(|e| e.into())
    }
}
