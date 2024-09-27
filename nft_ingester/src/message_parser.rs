use crate::error::IngesterError;
use crate::error::IngesterError::MissingFlatbuffersFieldError;
use crate::inscription_raw_parsing::ParsedInscription;
use crate::plerkle;
use crate::plerkle::PlerkleAccountInfo;
use blockbuster::error::BlockbusterError;
use blockbuster::program_handler::ProgramParser;
use blockbuster::programs::mpl_core_program::{
    MplCoreAccountData, MplCoreAccountState, MplCoreParser,
};
use blockbuster::programs::token_account::{TokenAccountParser, TokenProgramAccount};
use blockbuster::programs::token_extensions::{
    Token2022AccountParser, TokenExtensionsProgramAccount,
};
use blockbuster::programs::token_metadata::{TokenMetadataAccountData, TokenMetadataParser};
use blockbuster::programs::ProgramParseResult;
use chrono::Utc;
use entities::enums::UnprocessedAccount;
use entities::models::{BufferedTransaction, EditionV1, MasterEdition};
use flatbuffers::FlatBufferBuilder;
use itertools::Itertools;
use solana_program::program_pack::Pack;
use solana_program::pubkey::Pubkey;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use tracing::log::{debug, warn};
use utils::flatbuffer::account_data_generated::account_data::root_as_account_data;

pub struct MessageParser {
    token_acc_parser: Arc<TokenAccountParser>,
    mplx_acc_parser: Arc<TokenMetadataParser>,
    mpl_core_parser: Arc<MplCoreParser>,
    token_2022_parser: Arc<Token2022AccountParser>,
}

pub struct UnprocessedAccountWithMetadata {
    pub unprocessed_account: UnprocessedAccount,
    pub pubkey: Pubkey,
    pub write_version: u64,
}

impl Default for MessageParser {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageParser {
    pub fn new() -> Self {
        let token_acc_parser = Arc::new(TokenAccountParser {});
        let mplx_acc_parser = Arc::new(TokenMetadataParser {});
        let mpl_core_parser = Arc::new(MplCoreParser {});
        let token_2022_parser = Arc::new(Token2022AccountParser {});

        Self {
            token_acc_parser,
            mplx_acc_parser,
            mpl_core_parser,
            token_2022_parser,
        }
    }

    pub(crate) fn parse_transaction(
        &self,
        data: Vec<u8>,
        map_flatbuffer: bool,
    ) -> Option<BufferedTransaction> {
        // If we use Redis as messanger there no need to check if transaction failed because txs with error
        // do not fall into Redis queues
        if map_flatbuffer {
            let status = utils::flatbuffer::transaction_info_generated::transaction_info::root_as_transaction_info(
                &data,
            ).ok().and_then(|tx| tx.transaction_meta().and_then(|meta| meta.status()));
            // status is None if there no error with tx
            if status.is_some() {
                return None;
            }
        }

        Some(BufferedTransaction {
            transaction: data,
            map_flatbuffer,
        })
    }

    pub fn parse_account(
        &self,
        data: Vec<u8>,
        map_flatbuffer: bool,
    ) -> Result<Vec<UnprocessedAccountWithMetadata>, IngesterError> {
        let account_info_bytes = if map_flatbuffer {
            let account_update =
                utils::flatbuffer::account_info_generated::account_info::root_as_account_info(
                    &data,
                )?;
            map_account_info_fb_bytes(account_update)?
        } else {
            data
        };

        let account_info = plerkle_serialization::root_as_account_info(account_info_bytes.as_ref())
            .map_err(|e| IngesterError::AccountParsingError(e.to_string()))
            .unwrap();

        let account_info: plerkle::AccountInfo = PlerkleAccountInfo(account_info).try_into()?;
        let account_owner = account_info.owner;

        // do not use match expression
        // because match cases cannot contain function calls like spl_token::id()
        let accounts = if account_owner == spl_token::id() {
            self.handle_spl_token_account(&account_info)
                .into_iter()
                .collect_vec()
        } else if account_owner == blockbuster::programs::token_metadata::token_metadata_id() {
            self.handle_token_metadata_account(&account_info)
                .into_iter()
                .collect_vec()
        } else if account_owner == self.mpl_core_parser.key() {
            self.handle_mpl_core_account(&account_info)
        } else if account_owner == libreplex_inscriptions::id() {
            self.handle_inscription_account(&account_info)
                .into_iter()
                .collect_vec()
        } else if account_owner == spl_token_2022::id() {
            self.parse_spl_2022_accounts(&account_info)
                .into_iter()
                .collect_vec()
        } else {
            Vec::new()
        };

        Ok(accounts
            .into_iter()
            .map(|account| UnprocessedAccountWithMetadata {
                unprocessed_account: account,
                pubkey: account_info.pubkey,
                write_version: account_info.write_version,
            })
            .collect())
    }

    fn handle_spl_token_account(
        &self,
        account_info: &plerkle::AccountInfo,
    ) -> Option<UnprocessedAccount> {
        // skip processing of empty mints and multi-sig accounts
        if account_info.data.as_slice() == [0; spl_token::state::Mint::LEN] // empty mint
            || account_info.data.as_slice() == [0; spl_token::state::Account::LEN] // empty token account
            || account_info.data.len() == spl_token::state::Multisig::LEN
        {
            return None;
        }
        let acc_parse_result = self
            .token_acc_parser
            .handle_account(account_info.data.as_slice());

        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();
                match concrete {
                    ProgramParseResult::TokenProgramAccount(parsing_result) => {
                        return Some(self.parse_spl_accounts(account_info, parsing_result))
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }

        None
    }

    fn parse_spl_accounts(
        &self,
        account_update: &plerkle::AccountInfo,
        parsing_result: &TokenProgramAccount,
    ) -> UnprocessedAccount {
        let key = account_update.pubkey;
        match &parsing_result {
            TokenProgramAccount::TokenAccount(ta) => {
                let frozen = matches!(ta.state, spl_token::state::AccountState::Frozen);
                UnprocessedAccount::Token(entities::models::TokenAccount {
                    pubkey: key,
                    mint: ta.mint,
                    delegate: ta.delegate.into(),
                    owner: ta.owner,
                    extensions: None,
                    frozen,
                    delegated_amount: ta.delegated_amount as i64,
                    slot_updated: account_update.slot as i64,
                    amount: ta.amount as i64,
                    write_version: account_update.write_version,
                })
            }
            TokenProgramAccount::Mint(m) => {
                UnprocessedAccount::Mint(Box::new(entities::models::Mint {
                    pubkey: key,
                    slot_updated: account_update.slot as i64,
                    supply: m.supply as i64,
                    decimals: m.decimals as i32,
                    mint_authority: m.mint_authority.into(),
                    freeze_authority: m.freeze_authority.into(),
                    token_program: account_update.owner,
                    extensions: None,
                    write_version: account_update.write_version,
                }))
            }
        }
    }

    fn parse_spl_2022_accounts(
        &self,
        account_update: &plerkle::AccountInfo,
    ) -> Option<UnprocessedAccount> {
        let acc_parse_result = self
            .token_2022_parser
            .handle_account(account_update.data.as_slice());

        let key = account_update.pubkey;
        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();
                match concrete {
                    ProgramParseResult::TokenExtensionsProgramAccount(parsing_result) => {
                        return match &parsing_result {
                            TokenExtensionsProgramAccount::TokenAccount(ta) => {
                                let frozen = matches!(
                                    ta.account.state,
                                    spl_token_2022::state::AccountState::Frozen
                                );
                                Some(UnprocessedAccount::Token(entities::models::TokenAccount {
                                    pubkey: key,
                                    mint: ta.account.mint,
                                    delegate: ta.account.delegate.into(),
                                    owner: ta.account.owner,
                                    extensions: serde_json::to_value(&ta.extensions)
                                        .ok()
                                        .map(|e| e.to_string()),
                                    frozen,
                                    delegated_amount: ta.account.delegated_amount as i64,
                                    slot_updated: account_update.slot as i64,
                                    amount: ta.account.amount as i64,
                                    write_version: account_update.write_version,
                                }))
                            }
                            TokenExtensionsProgramAccount::MintAccount(m) => {
                                Some(UnprocessedAccount::Mint(Box::new(entities::models::Mint {
                                    pubkey: key,
                                    slot_updated: account_update.slot as i64,
                                    supply: m.account.supply as i64,
                                    decimals: m.account.decimals as i32,
                                    mint_authority: m.account.mint_authority.into(),
                                    freeze_authority: m.account.freeze_authority.into(),
                                    token_program: account_update.owner,
                                    extensions: Some(m.extensions.clone()),
                                    write_version: account_update.write_version,
                                })))
                            }
                            _ => None,
                        }
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                account_parsing_error(e, account_update);
            }
        }

        None
    }

    fn handle_token_metadata_account(
        &self,
        account_info: &plerkle::AccountInfo,
    ) -> Option<UnprocessedAccount> {
        let acc_parse_result = self
            .mplx_acc_parser
            .handle_account(account_info.data.as_slice());
        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();

                match concrete {
                    ProgramParseResult::TokenMetadata(parsing_result) => {
                        let key = account_info.pubkey;
                        match &parsing_result.data {
                            TokenMetadataAccountData::EmptyAccount => {
                                return Some(UnprocessedAccount::BurnMetadata(
                                    entities::models::BurntMetadataSlot {
                                        slot_updated: account_info.slot,
                                        write_version: account_info.write_version,
                                    },
                                ))
                            }
                            TokenMetadataAccountData::MetadataV1(m) => {
                                return Some(UnprocessedAccount::MetadataInfo(
                                    entities::models::MetadataInfo {
                                        metadata: m.clone(),
                                        slot_updated: account_info.slot,
                                        write_version: account_info.write_version,
                                        lamports: account_info.lamports,
                                        executable: account_info.executable,
                                        rent_epoch: account_info.rent_epoch,
                                        metadata_owner: Some(account_info.owner.to_string()),
                                    },
                                ))
                            }
                            TokenMetadataAccountData::MasterEditionV1(m) => {
                                return Some(UnprocessedAccount::Edition(
                                    entities::models::EditionMetadata {
                                        edition:
                                            entities::enums::TokenMetadataEdition::MasterEdition(
                                                MasterEdition {
                                                    key,
                                                    supply: m.supply,
                                                    max_supply: m.max_supply,
                                                    write_version: account_info.write_version,
                                                },
                                            ),
                                        write_version: account_info.write_version,
                                        slot_updated: account_info.slot,
                                    },
                                ))
                            }
                            TokenMetadataAccountData::MasterEditionV2(m) => {
                                return Some(UnprocessedAccount::Edition(
                                    entities::models::EditionMetadata {
                                        edition:
                                            entities::enums::TokenMetadataEdition::MasterEdition(
                                                MasterEdition {
                                                    key,
                                                    supply: m.supply,
                                                    max_supply: m.max_supply,
                                                    write_version: account_info.write_version,
                                                },
                                            ),
                                        write_version: account_info.write_version,
                                        slot_updated: account_info.slot,
                                    },
                                ))
                            }
                            TokenMetadataAccountData::EditionV1(e) => {
                                return Some(UnprocessedAccount::Edition(
                                    entities::models::EditionMetadata {
                                        edition: entities::enums::TokenMetadataEdition::EditionV1(
                                            EditionV1 {
                                                key,
                                                parent: e.parent,
                                                edition: e.edition,
                                                write_version: account_info.write_version,
                                            },
                                        ),
                                        write_version: account_info.write_version,
                                        slot_updated: account_info.slot,
                                    },
                                ))
                            }
                            _ => debug!("Not implemented"),
                        };
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => match e {
                BlockbusterError::AccountTypeNotImplemented
                | BlockbusterError::UninitializedAccount => {}
                _ => account_parsing_error(e, account_info),
            },
        }

        None
    }

    fn handle_mpl_core_account(
        &self,
        account_info: &plerkle::AccountInfo,
    ) -> Vec<UnprocessedAccount> {
        let acc_parse_result = self
            .mpl_core_parser
            .handle_account(account_info.data.as_slice());
        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();
                match concrete {
                    ProgramParseResult::MplCore(parsing_result) => {
                        return self.parse_mpl_core_accounts(account_info, parsing_result)
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }

        Vec::new()
    }

    pub fn handle_inscription_account(
        &self,
        account_info: &plerkle::AccountInfo,
    ) -> Option<UnprocessedAccount> {
        let acc_parse_result = crate::inscription_raw_parsing::handle_inscription_account(
            account_info.data.as_slice(),
        );
        match acc_parse_result {
            Ok(parsed_inscription) => match parsed_inscription {
                ParsedInscription::Inscription(inscription) => {
                    return Some(UnprocessedAccount::Inscription(
                        entities::models::InscriptionInfo {
                            inscription,
                            write_version: account_info.write_version,
                            slot_updated: account_info.slot,
                        },
                    ))
                }
                ParsedInscription::InscriptionData(inscription_data) => {
                    return Some(UnprocessedAccount::InscriptionData(
                        entities::models::InscriptionDataInfo {
                            inscription_data,
                            write_version: account_info.write_version,
                            slot_updated: account_info.slot,
                        },
                    ))
                }
                ParsedInscription::UnhandledAccount => {}
            },
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }

        None
    }

    fn parse_mpl_core_accounts(
        &self,
        account_update: &plerkle::AccountInfo,
        parsing_result: &MplCoreAccountState,
    ) -> Vec<UnprocessedAccount> {
        let mut response = Vec::new();
        match &parsing_result.data {
            MplCoreAccountData::EmptyAccount => response.push(UnprocessedAccount::BurnMplCore(
                entities::models::BurntMetadataSlot {
                    slot_updated: account_update.slot,
                    write_version: account_update.write_version,
                },
            )),
            MplCoreAccountData::Asset(_) | MplCoreAccountData::Collection(_) => response.push(
                UnprocessedAccount::MplCore(entities::models::IndexableAssetWithAccountInfo {
                    indexable_asset: parsing_result.data.clone(),
                    slot_updated: account_update.slot,
                    write_version: account_update.write_version,
                    lamports: account_update.lamports,
                    executable: account_update.executable,
                    rent_epoch: account_update.rent_epoch,
                }),
            ),
            _ => debug!("Not implemented"),
        };
        match &parsing_result.data {
            MplCoreAccountData::Asset(_)
            | MplCoreAccountData::EmptyAccount
            | MplCoreAccountData::HashedAsset => response.push(UnprocessedAccount::MplCoreFee(
                entities::models::CoreAssetFee {
                    indexable_asset: parsing_result.data.clone(),
                    data: account_update.data.clone(),
                    slot_updated: account_update.slot,
                    write_version: account_update.write_version,
                    lamports: account_update.lamports,
                    rent_epoch: account_update.rent_epoch,
                },
            )),
            _ => {}
        };

        response
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

fn account_parsing_error(err: impl Debug, account_info: &plerkle::AccountInfo) {
    warn!(
        "Error while parsing account: {:?} {}",
        err, account_info.pubkey
    );
}
