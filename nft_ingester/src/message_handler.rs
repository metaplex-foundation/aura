use std::sync::atomic::{AtomicU64, Ordering};
use std::{str::FromStr, sync::Arc};

use blockbuster::programs::token_account::TokenProgramAccount;
use blockbuster::{
    program_handler::ProgramParser,
    programs::{
        token_account::TokenAccountParser,
        token_metadata::{TokenMetadataAccountData, TokenMetadataParser},
        ProgramParseResult,
    },
};
use chrono::Utc;
use flatbuffers::FlatBufferBuilder;
use log::{error, warn};
use plerkle_serialization::AccountInfo;
use solana_sdk::pubkey::Pubkey;
use utils::flatbuffer::account_data_generated::account_data::root_as_account_data;

use rocks_db::columns::{Mint, TokenAccount};

use crate::buffer::{Buffer, BufferedTransaction};
use crate::error::IngesterError;
use crate::error::IngesterError::MissingFlatbuffersFieldError;
use crate::mplx_updates_processor::MetadataInfo;

const BYTE_PREFIX_TX_SIMPLE_FINALIZED: u8 = 22;
const BYTE_PREFIX_TX_FINALIZED: u8 = 12;
const BYTE_PREFIX_TX_PROCESSED: u8 = 2;
const BYTE_PREFIX_ACCOUNT_FINALIZED: u8 = 10;
const BYTE_PREFIX_ACCOUNT_PROCESSED: u8 = 0;

pub struct MessageHandler {
    buffer: Arc<Buffer>,
    token_acc_parser: Arc<TokenAccountParser>,
    mplx_acc_parser: Arc<TokenMetadataParser>,
    first_processed_slot: Arc<AtomicU64>,
}

impl MessageHandler {
    pub fn new(buffer: Arc<Buffer>, first_processed_slot: Arc<AtomicU64>) -> Self {
        let token_acc_parser = Arc::new(TokenAccountParser {});
        let mplx_acc_parser = Arc::new(TokenMetadataParser {});

        Self {
            buffer,
            token_acc_parser,
            mplx_acc_parser,
            first_processed_slot,
        }
    }

    pub async fn call(&self, msg: Vec<u8>) {
        let prefix = msg[0];
        let data = msg[1..].to_vec();

        match prefix {
            BYTE_PREFIX_ACCOUNT_FINALIZED | BYTE_PREFIX_ACCOUNT_PROCESSED => {
                if let Err(err) = self.handle_account(data).await {
                    error!("handle_account: {:?}", err)
                }
            }
            BYTE_PREFIX_TX_SIMPLE_FINALIZED
            | BYTE_PREFIX_TX_FINALIZED
            | BYTE_PREFIX_TX_PROCESSED => {
                let mut res = self.buffer.transactions.lock().await;
                res.push_back(BufferedTransaction {
                    transaction: data,
                    map_flatbuffer: true,
                });
            }
            _ => {}
        }
    }
    fn store_first_processed_slot(&self, slot: u64) {
        if self.first_processed_slot.load(Ordering::SeqCst) != 0 {
            // slot already stored
            return;
        }

        self.first_processed_slot.store(slot, Ordering::SeqCst)
    }

    async fn handle_account(&self, data: Vec<u8>) -> Result<(), IngesterError> {
        let account_update =
            utils::flatbuffer::account_info_generated::account_info::root_as_account_info(&data)?;

        self.store_first_processed_slot(account_update.slot());

        let account_info_bytes = map_account_info_fb_bytes(account_update)?;

        let account_info = plerkle_serialization::root_as_account_info(account_info_bytes.as_ref())
            .map_err(|e| IngesterError::AccountParsingError(e.to_string()))
            .unwrap();

        let account_owner = account_info
            .owner()
            .ok_or(IngesterError::DeserializationError(
                "Missed account owner field for account".to_string(),
            ))?;

        if account_owner.0 == spl_token::id().to_bytes() {
            self.handle_spl_token_account(&account_info).await?;
        } else if account_owner.0
            == blockbuster::programs::token_metadata::token_metadata_id().to_bytes()
        {
            self.handle_token_metadata_account(&account_info).await?;
        } else {
            warn!("Received account with unknown owner: {:?}", account_owner);
        }

        Ok(())
    }

    async fn handle_spl_token_account<'a>(
        &self,
        account_info: &'a AccountInfo<'a>,
    ) -> Result<(), IngesterError> {
        let acc_parse_result = self.token_acc_parser.handle_account(account_info);

        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();

                match concrete {
                    ProgramParseResult::TokenProgramAccount(parsing_result) => {
                        self.write_spl_accounts_models_to_buffer(account_info, parsing_result)
                            .await
                    }
                    _ => warn!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                warn!("Error while parsing account: {:?}", e);
            }
        }

        Ok(())
    }

    async fn write_spl_accounts_models_to_buffer<'a, 'b>(
        &self,
        account_update: &'a AccountInfo<'a>,
        parsing_result: &'b TokenProgramAccount,
    ) {
        let key = *account_update.pubkey().unwrap();
        let key_bytes = key.0.to_vec();
        match &parsing_result {
            TokenProgramAccount::TokenAccount(ta) => {
                let frozen = matches!(ta.state, spl_token::state::AccountState::Frozen);

                let token_acc_model = TokenAccount {
                    pubkey: Pubkey::try_from(key_bytes.clone()).unwrap(),
                    mint: ta.mint,
                    delegate: ta.delegate.into(),
                    owner: ta.owner,
                    frozen,
                    delegated_amount: ta.delegated_amount as i64,
                    slot_updated: account_update.slot() as i64,
                    amount: ta.amount as i64,
                };

                let mut token_accounts = self.buffer.token_accs.lock().await;

                if let Some(existing_model) = token_accounts.get(key_bytes.as_slice()) {
                    if token_acc_model.slot_updated > existing_model.slot_updated {
                        token_accounts.insert(key_bytes, token_acc_model);
                    }
                } else {
                    token_accounts.insert(key_bytes, token_acc_model);
                }
            }
            TokenProgramAccount::Mint(m) => {
                if m.decimals > 0 || m.supply > 0 {
                    return;
                }

                let mint_acc_model = Mint {
                    pubkey: Pubkey::try_from(key_bytes.clone()).unwrap_or_default(),
                    slot_updated: account_update.slot() as i64,
                    supply: m.supply as i64,
                    decimals: m.decimals as i32,
                    mint_authority: m.mint_authority.into(),
                    freeze_authority: m.freeze_authority.into(),
                };

                let mut mints = self.buffer.mints.lock().await;

                if let Some(existing_model) = mints.get(key_bytes.as_slice()) {
                    if mint_acc_model.slot_updated > existing_model.slot_updated {
                        mints.insert(key_bytes, mint_acc_model);
                    }
                } else {
                    mints.insert(key_bytes, mint_acc_model);
                }
            }
            _ => warn!("Not implemented"),
        };
    }

    async fn handle_token_metadata_account<'a>(
        &self,
        account_info: &'a AccountInfo<'a>,
    ) -> Result<(), IngesterError> {
        let acc_parse_result = self.mplx_acc_parser.handle_account(account_info);

        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();

                match concrete {
                    ProgramParseResult::TokenMetadata(parsing_result) => {
                        let key = account_info.pubkey().unwrap();

                        match &parsing_result.data {
                            TokenMetadataAccountData::MetadataV1(m) => {
                                let mut metadata = self.buffer.mplx_metadata_info.lock().await;

                                if let Some(existed_models) =
                                    metadata.get(m.mint.to_bytes().as_ref())
                                {
                                    if existed_models.slot < account_info.slot() {
                                        metadata.insert(
                                            key.0.to_vec(),
                                            MetadataInfo {
                                                metadata: m.clone(),
                                                slot: account_info.slot(),
                                            },
                                        );
                                    }
                                } else {
                                    metadata.insert(
                                        key.0.to_vec(),
                                        MetadataInfo {
                                            metadata: m.clone(),
                                            slot: account_info.slot(),
                                        },
                                    );
                                }
                            }
                            _ => warn!("Not implemented"),
                        };
                    }
                    _ => warn!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                warn!("Error while parsing account: {:?}", e);
            }
        }

        Ok(())
    }
}

fn map_account_info_fb_bytes(
    account_update: utils::flatbuffer::account_info_generated::account_info::AccountInfo,
) -> Result<Vec<u8>, IngesterError> {
    let account_data = root_as_account_data(
        account_update
            .account_data()
            .ok_or(MissingFlatbuffersFieldError("account data".to_string()))?
            .bytes(),
    )?;

    let pubkey = plerkle_serialization::Pubkey::new(
        &Pubkey::from_str(
            account_update
                .pubkey()
                .ok_or(MissingFlatbuffersFieldError("pubkey".to_string()))?,
        )?
        .to_bytes(),
    );
    let owner = plerkle_serialization::Pubkey::new(
        &Pubkey::from_str(
            account_update
                .owner()
                .ok_or(MissingFlatbuffersFieldError("owner".to_string()))?,
        )?
        .to_bytes(),
    );

    let mut builder = FlatBufferBuilder::new();

    let data = builder.create_vector(
        account_data
            .data()
            .ok_or(MissingFlatbuffersFieldError("data".to_string()))?
            .bytes(),
    );

    let args = plerkle_serialization::AccountInfoArgs {
        pubkey: Some(&pubkey),
        owner: Some(&owner),
        data: Some(data),
        slot: account_update.slot(),
        is_startup: false,
        seen_at: Utc::now().timestamp_millis(),
        lamports: account_data.lamports(),
        executable: account_data.executable(),
        rent_epoch: account_data.rent_epoch(),
        write_version: account_data.version(),
    };

    let account_info_wip = plerkle_serialization::AccountInfo::create(&mut builder, &args);
    builder.finish(account_info_wip, None);

    Ok(builder.finished_data().to_owned())
}
