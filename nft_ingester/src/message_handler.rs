use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::{str::FromStr, sync::Arc};

use blockbuster::programs::mpl_core_program::{
    MplCoreAccountData, MplCoreAccountState, MplCoreParser,
};
use blockbuster::{
    program_handler::ProgramParser,
    programs::{
        token_metadata::{TokenMetadataAccountData, TokenMetadataParser},
        ProgramParseResult,
    },
};
use entities::enums::UnprocessedAccount;
use flatbuffers::FlatBufferBuilder;
use solana_program::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::buffer::Buffer;
use crate::error::IngesterError;
use crate::message_parser::MessageParser;
use crate::plerkle;
use interface::message_handler::MessageHandler;

const BYTE_PREFIX_TX_SIMPLE_FINALIZED: u8 = 22;
const BYTE_PREFIX_TX_FINALIZED: u8 = 12;
const BYTE_PREFIX_TX_PROCESSED: u8 = 2;
const BYTE_PREFIX_ACCOUNT_FINALIZED: u8 = 10;
const BYTE_PREFIX_ACCOUNT_PROCESSED: u8 = 0;

async fn update_or_insert_account(
    accounts_buffer: &Mutex<HashMap<Pubkey, UnprocessedAccount>>,
    account: UnprocessedAccount,
    key: Pubkey,
    write_version: u64,
) {
    let mut map = accounts_buffer.lock().await;
    let entry = map.entry(key);
    match entry {
        std::collections::hash_map::Entry::Occupied(mut o) => match o.get() {
            UnprocessedAccount::MetadataInfo(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::Token(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::Mint(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::Edition(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::BurnMetadata(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::BurnMplCore(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::MplCore(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::Inscription(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
            UnprocessedAccount::InscriptionData(entity) => {
                if entity.write_version < write_version {
                    o.insert(account);
                }
            }
        },
        std::collections::hash_map::Entry::Vacant(v) => {
            v.insert(account);
        }
    }
}

pub struct MessageHandlerIngester {
    buffer: Arc<Buffer>,
    message_parser: Arc<MessageParser>,
}

#[async_trait]
impl MessageHandler for MessageHandlerIngester {
    async fn call(&self, msg: Vec<u8>) {
        let prefix = msg[0];
        let data = msg[1..].to_vec();

        match prefix {
            BYTE_PREFIX_ACCOUNT_FINALIZED | BYTE_PREFIX_ACCOUNT_PROCESSED => {
                match self.message_parser.parse_account(data, true).await {
                    Ok(account) => {
                        if let Some(unprocessed_account) = account.unprocessed_account {
                            update_or_insert_account(
                                &self.buffer.accounts,
                                unprocessed_account,
                                account.pubkey,
                                account.write_version,
                            )
                            .await;
                        }
                    }
                    Err(err) => {
                        error!("parse_account: {:?}", err)
                    }
                }
            }
            BYTE_PREFIX_TX_SIMPLE_FINALIZED
            | BYTE_PREFIX_TX_FINALIZED
            | BYTE_PREFIX_TX_PROCESSED => {
                let Some(tx) = self.message_parser.parse_transaction(data, true).await else {
                    return;
                };
                let mut res = self.buffer.transactions.lock().await;
                res.push_back(tx);
            }
            _ => {}
        }
    }
}

impl MessageHandlerIngester {
    pub fn new(buffer: Arc<Buffer>) -> Self {
        let message_parser = Arc::new(MessageParser::new());

        Self {
            buffer,
            message_parser,
        }
    }
}

pub struct MessageHandlerCoreIndexing {
    mpl_core_parser: Arc<MplCoreParser>,
}

#[async_trait]
impl MessageHandler for MessageHandlerCoreIndexing {
    async fn call(&self, msg: Vec<u8>) {
        let prefix = msg[0];
        let data = msg[1..].to_vec();

        match prefix {
            BYTE_PREFIX_ACCOUNT_FINALIZED | BYTE_PREFIX_ACCOUNT_PROCESSED => {
                if let Err(err) = self.handle_account(data, true).await {
                    error!("handle_account: {:?}", err)
                }
            }
            _ => {}
        }
    }
}

impl Default for MessageHandlerCoreIndexing {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageHandlerCoreIndexing {
    pub fn new() -> Self {
        Self {
            mpl_core_parser: Arc::new(MplCoreParser {}),
        }
    }

    async fn handle_account(
        &self,
        data: Vec<u8>,
        map_flatbuffer: bool,
    ) -> Result<(), IngesterError> {
        // let account_info_bytes = if map_flatbuffer {
        //     let account_update =
        //         utils::flatbuffer::account_info_generated::account_info::root_as_account_info(
        //             &data,
        //         )?;
        //     map_account_info_fb_bytes(account_update)?
        // } else {
        //     data
        // };
        // let account_info = plerkle_serialization::root_as_account_info(account_info_bytes.as_ref())
        //     .map_err(|e| IngesterError::AccountParsingError(e.to_string()))
        //     .unwrap();
        //
        // let account_info: plerkle::AccountInfo = PlerkleAccountInfo(account_info).try_into()?;
        // let account_owner = account_info.owner;
        // if account_owner == self.mpl_core_parser.key() {
        //     self.handle_mpl_core_account(&account_info).await;
        // }

        Ok(())
    }

    async fn handle_mpl_core_account<'a>(&self, account_info: &plerkle::AccountInfo) {
        // let acc_parse_result = self
        //     .mpl_core_parser
        //     .handle_account(account_info.data.as_slice());
        // match acc_parse_result {
        //     Ok(acc_parsed) => {
        //         let concrete = acc_parsed.result_type();
        //         match concrete {
        //             ProgramParseResult::MplCore(parsing_result) => {
        //                 self.write_mpl_core_accounts_to_buffer(account_info, parsing_result)
        //                     .await
        //             }
        //             _ => debug!("\nUnexpected message\n"),
        //         };
        //     }
        //     Err(e) => {
        //         account_parsing_error(e, account_info);
        //     }
        // }
    }

    async fn write_mpl_core_accounts_to_buffer(
        &self,
        account_update: &plerkle::AccountInfo,
        parsing_result: &MplCoreAccountState,
    ) {
        let _key = account_update.pubkey;
        match &parsing_result.data {
            MplCoreAccountData::Asset(_)
            | MplCoreAccountData::EmptyAccount
            | MplCoreAccountData::HashedAsset => {
                // update_or_insert_account(
                //     &self.buffer.accounts,
                //     UnprocessedAccount::MplCoreFee(entities::models::CoreAssetFee {
                //         indexable_asset: parsing_result.data.clone(),
                //         data: account_update.data.clone(),
                //         slot_updated: account_update.slot,
                //         write_version: account_update.write_version,
                //         lamports: account_update.lamports,
                //         rent_epoch: account_update.rent_epoch,
                //     }),
                //     key,
                //     account_update.write_version,
                // )
                // .await;
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
