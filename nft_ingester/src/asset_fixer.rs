use std::{collections::HashMap, sync::Arc};

use entities::models::UnprocessedAccountMessage;
use metrics_utils::IngesterMetricsConfig;
use rocks_db::{batch_savers::BatchSaveStorage, errors::StorageError, Storage};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    client_error::ClientError, nonblocking::rpc_client::RpcClient,
    rpc_config::RpcAccountInfoConfig, rpc_response::Response,
};
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use tracing::{debug, error, info};

use crate::{
    message_parser::MessageParser, plerkle, processors::processors_wrapper::ProcessorsWrapper,
};

/// Responsible for fixing account-based assets by retrieving them from RPC
/// and processing them through the AccountsProcessor infrastructure
pub struct AssetFixer {
    rocks_storage: Arc<Storage>,
    rpc_client: Arc<RpcClient>,
    accounts_processor: ProcessorsWrapper,
    message_parser: MessageParser,
    metrics: Arc<IngesterMetricsConfig>,
    batch_size: usize,
}

impl AssetFixer {
    pub fn new(
        rocks_storage: Arc<Storage>,
        rpc_client: Arc<RpcClient>,
        accounts_processor: ProcessorsWrapper,
        metrics: Arc<IngesterMetricsConfig>,
        batch_size: usize,
    ) -> Self {
        let message_parser = MessageParser::new();
        Self { rocks_storage, rpc_client, accounts_processor, message_parser, metrics, batch_size }
    }

    /// Fix a list of accounts by retrieving their data and processing them
    pub async fn fix_accounts(&self, pubkeys: Vec<Pubkey>) -> Result<FixResult, FixError> {
        if pubkeys.is_empty() {
            return Ok(FixResult { fixed_count: 0, failed_pubkeys: Vec::new() });
        }

        info!("Fixing {} accounts", pubkeys.len());

        // Create a batch storage with the configured batch size
        let mut batch_storage = BatchSaveStorage::new(
            self.rocks_storage.clone(),
            self.batch_size,
            self.metrics.clone(),
        );

        let mut core_fees = HashMap::new();

        let mut fixed_count = 0;
        let mut failed_pubkeys = Vec::new();

        // Get accounts data from RPC with finalized commitment
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            data_slice: None,
            commitment: Some(CommitmentConfig { commitment: CommitmentLevel::Finalized }),
            min_context_slot: None,
        };

        // Process accounts in chunks to avoid RPC limits
        for chunk in pubkeys.chunks(1000) {
            match self.rpc_client.get_multiple_accounts_with_config(chunk, config.clone()).await {
                Ok(Response { value: accounts, context: ctx }) => {
                    for (i, account_opt) in accounts.iter().enumerate() {
                        if let Some(account) = account_opt {
                            let pubkey = chunk[i];

                            let account_info = plerkle::AccountInfo {
                                slot: ctx.slot,
                                pubkey,
                                lamports: account.lamports,
                                owner: account.owner,
                                executable: account.executable,
                                rent_epoch: account.rent_epoch,
                                data: account.data.clone(),
                                write_version: 0,
                            };

                            // Transform RPC account into UnprocessedAccountMessage

                            let unprocessed_account_result =
                                self.message_parser.parse_account_info(&account_info);

                            match unprocessed_account_result {
                                Ok(unprocessed_accounts) => {
                                    let unprocessed_accounts = unprocessed_accounts
                                        .into_iter()
                                        .map(|unprocessed_account| UnprocessedAccountMessage {
                                            account: unprocessed_account.unprocessed_account,
                                            key: unprocessed_account.pubkey,
                                            id: String::new(),
                                        });
                                    for unprocessed_account in unprocessed_accounts {
                                        // Process the account through processors wrapper
                                        if let Err(e) =
                                            self.accounts_processor.process_account_by_type(
                                                &mut batch_storage,
                                                &unprocessed_account,
                                                &mut core_fees,
                                            )
                                        {
                                            error!("Error processing account {}: {}", pubkey, e);
                                            failed_pubkeys.push(pubkey);
                                            continue;
                                        }

                                        fixed_count += 1;
                                        debug!("Successfully processed account {}", pubkey);
                                    }
                                },
                                Err(e) => {
                                    error!("Error transforming account {}: {}", pubkey, e);
                                    failed_pubkeys.push(pubkey);
                                },
                            }
                        } else {
                            // Account not found
                            let pubkey = chunk[i];
                            error!("Account {} not found", pubkey);
                            failed_pubkeys.push(pubkey);
                        }
                    }
                },
                Err(e) => {
                    error!("RPC error fetching accounts: {}", e);
                    // Add all pubkeys in this chunk to failed list
                    failed_pubkeys.extend_from_slice(chunk);
                },
            }
        }
        batch_storage.flush().map_err(|e| FixError::Storage(e))?;

        Ok(FixResult { fixed_count, failed_pubkeys })
    }
}

/// Result of the fix operation
pub struct FixResult {
    pub fixed_count: usize,
    pub failed_pubkeys: Vec<Pubkey>,
}

/// Errors that can occur during the fix operation
#[derive(thiserror::Error, Debug)]
pub enum FixError {
    #[error("RPC error: {0}")]
    Rpc(#[from] ClientError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Other error: {0}")]
    Other(String),
}
