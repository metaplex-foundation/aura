use crate::{
    error::BlockbusterError,
    program_handler::{ParseResult, ProgramParser},
    programs::ProgramParseResult,
};
use borsh::BorshDeserialize;
use mpl_core::{
    accounts::HashedAsset,
    fetch_asset_compression_proof, fetch_collection_compression_proof,
    types::{CompressionProof, Key},
};
use plerkle_serialization::AccountInfo;
use solana_sdk::{pubkey::Pubkey, pubkeys};

pubkeys!(mpl_core_id, "CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d");

#[derive(Clone)]
pub enum MplCoreAccountData {
    FullAsset(CompressionProof),
    Collection(CompressionProof),
    HashedAsset(HashedAsset),
    EmptyAccount,
}

pub struct MplCoreAccountState {
    pub key: Key,
    pub data: MplCoreAccountData,
}

impl ParseResult for MplCoreAccountState {
    fn result(&self) -> &Self
    where
        Self: Sized,
    {
        self
    }
    fn result_type(&self) -> ProgramParseResult {
        ProgramParseResult::MplCore(self)
    }
}

pub struct MplCoreParser;

impl ProgramParser for MplCoreParser {
    fn key(&self) -> Pubkey {
        mpl_core_id()
    }
    fn key_match(&self, key: &Pubkey) -> bool {
        key == &mpl_core_id()
    }

    fn handles_account_updates(&self) -> bool {
        true
    }

    fn handles_instructions(&self) -> bool {
        false
    }

    fn handle_account(
        &self,
        account_info: &AccountInfo,
    ) -> Result<Box<(dyn ParseResult + 'static)>, BlockbusterError> {
        let account_data = if let Some(account_info) = account_info.data() {
            account_info.iter().collect::<Vec<_>>()
        } else {
            // I don't know if all this empty account assumptions are correct, just using similar
            // approach to the the token metadata code.
            return Ok(Box::new(MplCoreAccountState {
                key: Key::Uninitialized,
                data: MplCoreAccountData::EmptyAccount,
            }));
        };
        if account_data.is_empty() {
            return Ok(Box::new(MplCoreAccountState {
                key: Key::Uninitialized,
                data: MplCoreAccountData::EmptyAccount,
            }));
        }
        let key = Key::try_from_slice(&account_data[0..1])?;
        let token_metadata_account_state = match key {
            Key::Asset => {
                let compression_proof = fetch_asset_compression_proof(&account_data)?;

                MplCoreAccountState {
                    key,
                    data: MplCoreAccountData::FullAsset(compression_proof),
                }
            }
            Key::Collection => {
                let compression_proof = fetch_collection_compression_proof(&account_data)?;

                MplCoreAccountState {
                    key,
                    data: MplCoreAccountData::Collection(compression_proof),
                }
            }
            Key::Uninitialized => MplCoreAccountState {
                key: Key::Uninitialized,
                data: MplCoreAccountData::EmptyAccount,
            },
            _ => {
                return Err(BlockbusterError::AccountTypeNotImplemented);
            }
        };

        Ok(Box::new(token_metadata_account_state))
    }
}
