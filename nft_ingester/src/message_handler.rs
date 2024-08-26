use async_trait::async_trait;
use std::{str::FromStr, sync::Arc};
use std::fmt::Debug;

use blockbuster::error::BlockbusterError;
use blockbuster::programs::mpl_core_program::{
    MplCoreAccountData, MplCoreAccountState, MplCoreParser,
};
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
use entities::models::BufferedTransaction;
use flatbuffers::FlatBufferBuilder;
use libreplex_inscriptions::instructions::legacy_inscriber;
use solana_program::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use tracing::{debug, error, warn};
use utils::flatbuffer::account_data_generated::account_data::root_as_account_data;

use rocks_db::columns::{Mint, TokenAccount};
use rocks_db::editions::TokenMetadataEdition;

use crate::buffer::{Buffer, FeesBuffer};
use crate::error::IngesterError;
use crate::error::IngesterError::MissingFlatbuffersFieldError;
use crate::mplx_updates_processor::{
    BurntMetadataSlot, CoreAssetFee, IndexableAssetWithAccountInfo, MetadataInfo, TokenMetadata,
};
use crate::plerkle;
use crate::plerkle::PlerkleAccountInfo;
use entities::models::{EditionV1, MasterEdition};
use interface::message_handler::MessageHandler;

const BYTE_PREFIX_TX_SIMPLE_FINALIZED: u8 = 22;
const BYTE_PREFIX_TX_FINALIZED: u8 = 12;
const BYTE_PREFIX_TX_PROCESSED: u8 = 2;
const BYTE_PREFIX_ACCOUNT_FINALIZED: u8 = 10;
const BYTE_PREFIX_ACCOUNT_PROCESSED: u8 = 0;

pub struct MessageHandlerIngester {
    buffer: Arc<Buffer>,
    token_acc_parser: Arc<TokenAccountParser>,
    mplx_acc_parser: Arc<TokenMetadataParser>,
    mpl_core_parser: Arc<MplCoreParser>,
}

macro_rules! update_or_insert {
    ($map:expr, $key:expr, $value:expr, $update_write_version:expr) => {{
        let mut map = $map.lock().await;
        let entry = map.entry($key);
        match entry {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                if o.get().write_version < $update_write_version {
                    o.insert($value);
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert($value);
            }
        }
    }};
}

#[async_trait]
impl MessageHandler for MessageHandlerIngester {
    async fn call(&self, msg: Vec<u8>) {
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
                let status = utils::flatbuffer::transaction_info_generated::transaction_info::root_as_transaction_info(
                    &data,
                ).ok().and_then(|tx| tx.transaction_meta().and_then(|meta| meta.status()));
                // status is None if there no error with tx
                if status.is_some() {
                    return;
                }
                let mut res = self.buffer.transactions.lock().await;
                res.push_back(BufferedTransaction {
                    transaction: data,
                    map_flatbuffer: true,
                });
            }
            _ => {}
        }
    }
}

impl MessageHandlerIngester {
    pub fn new(buffer: Arc<Buffer>) -> Self {
        let token_acc_parser = Arc::new(TokenAccountParser {});
        let mplx_acc_parser = Arc::new(TokenMetadataParser {});
        let mpl_core_parser = Arc::new(MplCoreParser {});

        Self {
            buffer,
            token_acc_parser,
            mplx_acc_parser,
            mpl_core_parser,
        }
    }

    async fn handle_account(&self, data: Vec<u8>) -> Result<(), IngesterError> {
        let account_update =
            utils::flatbuffer::account_info_generated::account_info::root_as_account_info(&data)?;

        let account_info_bytes = map_account_info_fb_bytes(account_update)?;

        let account_info = plerkle_serialization::root_as_account_info(account_info_bytes.as_ref())
            .map_err(|e| IngesterError::AccountParsingError(e.to_string()))
            .unwrap();

        let account_info: plerkle::AccountInfo = PlerkleAccountInfo(account_info).try_into()?;
        let account_owner = account_info.owner;

        // do not use match expression
        // because match cases cannot contain function calls like spl_token::id()
        if account_owner == spl_token::id() {
            self.handle_spl_token_account(&account_info).await?;
        } else if account_owner == blockbuster::programs::token_metadata::token_metadata_id() {
            self.handle_token_metadata_account(&account_info).await?;
        } else if account_owner == self.mpl_core_parser.key() {
            self.handle_mpl_core_account(&account_info).await;
        } else if account_owner == libreplex_inscriptions::id() {
            self.handle_inscription_account(&account_info).await;
        }

        Ok(())
    }

    async fn handle_spl_token_account(
        &self,
        account_info: &plerkle::AccountInfo,
    ) -> Result<(), IngesterError> {
        // skip processing of empty mints and multi-sig accounts
        if account_info.data.as_slice() == [0; spl_token::state::Mint::LEN] // empty mint
                || account_info.data.as_slice() == [0; spl_token::state::Account::LEN] // empty token account
                || account_info.data.len() == spl_token::state::Multisig::LEN
        {
            return Ok(());
        }
        let acc_parse_result = self
            .token_acc_parser
            .handle_account(account_info.data.as_slice());

        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();
                match concrete {
                    ProgramParseResult::TokenProgramAccount(parsing_result) => {
                        self.write_spl_accounts_models_to_buffer(account_info, parsing_result)
                            .await
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }

        Ok(())
    }

    async fn write_spl_accounts_models_to_buffer(
        &self,
        account_update: &plerkle::AccountInfo,
        parsing_result: &TokenProgramAccount,
    ) {
        let key = account_update.pubkey;
        match &parsing_result {
            TokenProgramAccount::TokenAccount(ta) => {
                let frozen = matches!(ta.state, spl_token::state::AccountState::Frozen);
                update_or_insert!(
                    self.buffer.token_accs,
                    key,
                    TokenAccount {
                        pubkey: key,
                        mint: ta.mint,
                        delegate: ta.delegate.into(),
                        owner: ta.owner,
                        frozen,
                        delegated_amount: ta.delegated_amount as i64,
                        slot_updated: account_update.slot as i64,
                        amount: ta.amount as i64,
                        write_version: account_update.write_version,
                    },
                    account_update.write_version
                );
            }
            TokenProgramAccount::Mint(m) => {
                update_or_insert!(
                    self.buffer.mints,
                    key.to_bytes().to_vec(),
                    Mint {
                        pubkey: key,
                        slot_updated: account_update.slot as i64,
                        supply: m.supply as i64,
                        decimals: m.decimals as i32,
                        mint_authority: m.mint_authority.into(),
                        freeze_authority: m.freeze_authority.into(),
                        write_version: account_update.write_version,
                    },
                    account_update.write_version
                );
            }
        };
    }

    async fn handle_token_metadata_account(
        &self,
        account_info: &plerkle::AccountInfo,
    ) -> Result<(), IngesterError> {
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
                                let mut buff = self.buffer.burnt_metadata_at_slot.lock().await;

                                buff.insert(
                                    key,
                                    BurntMetadataSlot {
                                        slot_updated: account_info.slot,
                                    },
                                );
                            }
                            TokenMetadataAccountData::MetadataV1(m) => {
                                update_or_insert!(
                                    self.buffer.mplx_metadata_info,
                                    key.to_bytes().to_vec(),
                                    MetadataInfo {
                                        metadata: m.clone(),
                                        slot_updated: account_info.slot,
                                        write_version: account_info.write_version,
                                        lamports: account_info.lamports,
                                        executable: account_info.executable,
                                        rent_epoch: account_info.rent_epoch,
                                        metadata_owner: Some(account_info.owner.to_string()),
                                    },
                                    account_info.write_version
                                );
                            }
                            TokenMetadataAccountData::MasterEditionV1(m) => {
                                update_or_insert!(
                                    self.buffer.token_metadata_editions,
                                    key,
                                    TokenMetadata {
                                        edition: TokenMetadataEdition::MasterEdition(
                                            MasterEdition {
                                                key,
                                                supply: m.supply,
                                                max_supply: m.max_supply,
                                                write_version: account_info.write_version
                                            },
                                        ),
                                        write_version: account_info.write_version,
                                        slot_updated: account_info.slot,
                                    },
                                    account_info.write_version
                                );
                            }
                            TokenMetadataAccountData::MasterEditionV2(m) => {
                                update_or_insert!(
                                    self.buffer.token_metadata_editions,
                                    key,
                                    TokenMetadata {
                                        edition: TokenMetadataEdition::MasterEdition(
                                            MasterEdition {
                                                key,
                                                supply: m.supply,
                                                max_supply: m.max_supply,
                                                write_version: account_info.write_version
                                            },
                                        ),
                                        write_version: account_info.write_version,
                                        slot_updated: account_info.slot,
                                    },
                                    account_info.write_version
                                );
                            }
                            TokenMetadataAccountData::EditionV1(e) => {
                                update_or_insert!(
                                    self.buffer.token_metadata_editions,
                                    key,
                                    TokenMetadata {
                                        edition: TokenMetadataEdition::EditionV1(EditionV1 {
                                            key,
                                            parent: e.parent,
                                            edition: e.edition,
                                            write_version: account_info.write_version
                                        },),
                                        write_version: account_info.write_version,
                                        slot_updated: account_info.slot,
                                    },
                                    account_info.write_version
                                );
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

        Ok(())
    }

    async fn handle_mpl_core_account<'a>(&self, account_info: &plerkle::AccountInfo) {
        let acc_parse_result = self
            .mpl_core_parser
            .handle_account(account_info.data.as_slice());
        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();
                match concrete {
                    ProgramParseResult::MplCore(parsing_result) => {
                        self.write_mpl_core_accounts_to_buffer(account_info, parsing_result)
                            .await
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }
    }

    async fn handle_inscription_account<'a>(&self, account_info: &plerkle::AccountInfo) {
        let acc_parse_result = crate::inscription_raw_parsing::handle_inscription_account(
            account_info.data.as_slice(),
        );
        match acc_parse_result {
            Ok(parsed_inscription) => {

            }
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }
    }

    async fn write_mpl_core_accounts_to_buffer(
        &self,
        account_update: &plerkle::AccountInfo,
        parsing_result: &MplCoreAccountState,
    ) {
        let key = account_update.pubkey;
        match &parsing_result.data {
            MplCoreAccountData::EmptyAccount => {
                let mut buff = self.buffer.burnt_mpl_core_at_slot.lock().await;

                buff.insert(
                    key,
                    BurntMetadataSlot {
                        slot_updated: account_update.slot,
                    },
                );
            }
            MplCoreAccountData::Asset(_) | MplCoreAccountData::Collection(_) => {
                update_or_insert!(
                    self.buffer.mpl_core_indexable_assets,
                    key,
                    IndexableAssetWithAccountInfo {
                        indexable_asset: parsing_result.data.clone(),
                        slot_updated: account_update.slot,
                        write_version: account_update.write_version,
                        lamports: account_update.lamports,
                        executable: account_update.executable,
                        rent_epoch: account_update.rent_epoch,
                    },
                    account_update.write_version
                );
            }
            _ => debug!("Not implemented"),
        };
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

pub struct MessageHandlerCoreIndexing {
    buffer: Arc<FeesBuffer>,
    mpl_core_parser: Arc<MplCoreParser>,
}

#[async_trait]
impl MessageHandler for MessageHandlerCoreIndexing {
    async fn call(&self, msg: Vec<u8>) {
        let prefix = msg[0];
        let data = msg[1..].to_vec();

        match prefix {
            BYTE_PREFIX_ACCOUNT_FINALIZED | BYTE_PREFIX_ACCOUNT_PROCESSED => {
                if let Err(err) = self.handle_account(data).await {
                    error!("handle_account: {:?}", err)
                }
            }
            _ => {}
        }
    }
}

impl MessageHandlerCoreIndexing {
    pub fn new(buffer: Arc<FeesBuffer>) -> Self {
        Self {
            buffer,
            mpl_core_parser: Arc::new(MplCoreParser {}),
        }
    }

    async fn handle_account(&self, data: Vec<u8>) -> Result<(), IngesterError> {
        let account_update =
            utils::flatbuffer::account_info_generated::account_info::root_as_account_info(&data)?;

        let account_info_bytes = map_account_info_fb_bytes(account_update)?;

        let account_info = plerkle_serialization::root_as_account_info(account_info_bytes.as_ref())
            .map_err(|e| IngesterError::AccountParsingError(e.to_string()))
            .unwrap();

        let account_info: plerkle::AccountInfo = PlerkleAccountInfo(account_info).try_into()?;
        let account_owner = account_info.owner;
        if account_owner == self.mpl_core_parser.key() {
            self.handle_mpl_core_account(&account_info).await;
        }

        Ok(())
    }

    async fn handle_mpl_core_account<'a>(&self, account_info: &plerkle::AccountInfo) {
        let acc_parse_result = self
            .mpl_core_parser
            .handle_account(account_info.data.as_slice());
        match acc_parse_result {
            Ok(acc_parsed) => {
                let concrete = acc_parsed.result_type();
                match concrete {
                    ProgramParseResult::MplCore(parsing_result) => {
                        self.write_mpl_core_accounts_to_buffer(account_info, parsing_result)
                            .await
                    }
                    _ => debug!("\nUnexpected message\n"),
                };
            }
            Err(e) => {
                account_parsing_error(e, account_info);
            }
        }
    }

    async fn write_mpl_core_accounts_to_buffer(
        &self,
        account_update: &plerkle::AccountInfo,
        parsing_result: &MplCoreAccountState,
    ) {
        let key = account_update.pubkey;
        match &parsing_result.data {
            MplCoreAccountData::Asset(_)
            | MplCoreAccountData::EmptyAccount
            | MplCoreAccountData::HashedAsset => {
                update_or_insert!(
                    self.buffer.mpl_core_fee_assets,
                    key,
                    CoreAssetFee {
                        indexable_asset: parsing_result.data.clone(),
                        data: account_update.data.clone(),
                        slot_updated: account_update.slot,
                        write_version: account_update.write_version,
                        lamports: account_update.lamports,
                        rent_epoch: account_update.rent_epoch,
                    },
                    account_update.write_version
                );
            }
            _ => debug!("Not implemented"),
        };
    }
}

#[test]
fn test_mint_uninitialized() {
    let unpack_res = spl_token::state::Mint::unpack(&[0; 82]);
    assert_eq!(
        Err(solana_program::program_error::ProgramError::UninitializedAccount),
        unpack_res
    )
}

#[test]
fn test_token_account_uninitialized() {
    let unpack_res = spl_token::state::Account::unpack(&[0; 165]);
    assert_eq!(
        Err(solana_program::program_error::ProgramError::UninitializedAccount),
        unpack_res
    )
}

// Test proofs that EditionV1 && MasterEditionV1 accounts addresses
// can be obtained using the same PDA function
#[cfg(feature = "rpc_tests")]
#[tokio::test]
async fn test_edition_pda() {
    use blockbuster::program_handler::ProgramParser;
    use blockbuster::programs::token_metadata::{TokenMetadataAccountData, TokenMetadataParser};
    use blockbuster::programs::ProgramParseResult;
    use mpl_token_metadata::accounts::MasterEdition;
    use plerkle_serialization::deserializer::PlerkleOptionalU8Vector;
    use plerkle_serialization::solana_geyser_plugin_interface_shims::ReplicaAccountInfoV2;
    use solana_client::nonblocking::rpc_client::RpcClient;

    let master_edition_pubkey =
        Pubkey::from_str("9Rfs2otkZpsLPomKUGku7DaFv9YvtkV9a87nqTUgMBhC").unwrap();
    let edition_pubkey = Pubkey::from_str("1163REUqapQVzoet7GLuWFrvRVTrmYBYu8FDEXmeUEz").unwrap();
    let client = RpcClient::new("https://docs-demo.solana-mainnet.quiknode.pro/".to_string());
    let edition_account = client.get_account(&edition_pubkey).await.unwrap();
    let master_edition_account = client.get_account(&master_edition_pubkey).await.unwrap();

    let ser_edition_account = plerkle_serialization::serializer::serialize_account(
        FlatBufferBuilder::new(),
        &ReplicaAccountInfoV2 {
            pubkey: edition_pubkey.to_bytes().as_ref(),
            lamports: edition_account.lamports,
            owner: edition_account.owner.to_bytes().as_ref(),
            executable: edition_account.executable,
            rent_epoch: edition_account.rent_epoch,
            data: edition_account.data.as_ref(),
            write_version: 0,
            txn_signature: None,
        },
        0,
        false,
    );
    let parser = TokenMetadataParser {};
    let binding = ser_edition_account.finished_data().to_owned();
    let binding = plerkle_serialization::root_as_account_info(binding.as_ref()).unwrap();
    let account_data: Vec<u8> = PlerkleOptionalU8Vector(binding.data()).try_into().unwrap();
    let binding = parser.handle_account(account_data.as_slice()).unwrap();
    let binding = binding.result_type();
    let binding = match binding {
        ProgramParseResult::TokenMetadata(c) => c,
        _ => {
            panic!()
        }
    };
    let edition = if let TokenMetadataAccountData::EditionV1(account) = &binding.data {
        Some(account)
    } else {
        None
    };
    // check that account is really EditionV1
    assert_ne!(None, edition);

    let ser_master_edition_account = plerkle_serialization::serializer::serialize_account(
        FlatBufferBuilder::new(),
        &ReplicaAccountInfoV2 {
            pubkey: master_edition_pubkey.to_bytes().as_ref(),
            lamports: master_edition_account.lamports,
            owner: master_edition_account.owner.to_bytes().as_ref(),
            executable: master_edition_account.executable,
            rent_epoch: master_edition_account.rent_epoch,
            data: master_edition_account.data.as_ref(),
            write_version: 0,
            txn_signature: None,
        },
        0,
        false,
    );
    let binding = ser_master_edition_account.finished_data().to_owned();
    let binding = plerkle_serialization::root_as_account_info(binding.as_ref()).unwrap();
    let account_data: Vec<u8> = PlerkleOptionalU8Vector(binding.data()).try_into().unwrap();
    let binding = parser.handle_account(account_data.as_slice()).unwrap();
    let binding = binding.result_type();
    let binding = match binding {
        ProgramParseResult::TokenMetadata(c) => c,
        _ => {
            panic!()
        }
    };

    let master_edition = if let TokenMetadataAccountData::MasterEditionV2(account) = &binding.data {
        Some(account)
    } else {
        None
    };
    // check that account is really MasterEditionV2
    assert_ne!(None, master_edition);
    assert_eq!(
        edition_pubkey,
        MasterEdition::find_pda(
            &Pubkey::from_str("iPZvpCFQeSjaC37WG2z5RfzTRFrMzcruwPaY3oeN9aN").unwrap()
        )
        .0
    );
    assert_eq!(
        master_edition_pubkey,
        MasterEdition::find_pda(
            &Pubkey::from_str("HHh5kp1XK9tTZsdmbuikVmNv2wmKStV4MQRa3vdXsL7S").unwrap()
        )
        .0
    );
}
