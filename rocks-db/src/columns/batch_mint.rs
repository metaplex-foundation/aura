use serde::{Deserialize, Serialize};
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::{
    column::TypedColumn,
    key_encoders::{decode_string, encode_string},
    Result,
};

#[deprecated]
#[derive(Debug, Clone)]
pub struct BatchMintWithState {
    pub file_name: String,
    pub state: BatchMintState,
    pub error: Option<String>,
    pub url: Option<String>,
    pub created_at: u64,
}
#[deprecated]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BatchMintState {
    Uploaded,
    ValidationFail,
    ValidationComplete,
    UploadedToArweave,
    FailUploadToArweave,
    FailSendingTransaction,
    Complete,
}

#[deprecated]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BatchMintToVerify {
    pub file_hash: String,
    pub url: String,
    pub created_at_slot: u64,
    pub signature: Signature,
    pub download_attempts: u8,
    pub persisting_state: PersistingBatchMintState,
    pub staker: Pubkey,
    pub collection_mint: Option<Pubkey>,
}
#[deprecated]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PersistingBatchMintState {
    ReceivedTransaction,
    FailedToPersist,
    SuccessfullyDownload,
    SuccessfullyValidate,
    StoredUpdate,
}

#[deprecated]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FailedBatchMint {
    pub status: FailedBatchMintState,
    pub file_hash: String,
    pub url: String,
    pub created_at_slot: u64,
    pub signature: Signature,
    pub download_attempts: u8,
    pub staker: Pubkey,
}
#[deprecated]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum FailedBatchMintState {
    DownloadFailed,
    ChecksumVerifyFailed,
    BatchMintVerifyFailed,
    FileSerialization,
}

impl TypedColumn for BatchMintToVerify {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "BATCH_MINT_TO_VERIFY"; // Name of the column family
    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }
    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}
impl TypedColumn for BatchMintWithStaker {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "BATCH_MINTS"; // Name of the column family
    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }
    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}
impl TypedColumn for FailedBatchMint {
    type KeyType = String;
    type ValueType = Self;
    const NAME: &'static str = "FAILED_BATCH_MINT"; // Name of the column family
    fn encode_key(key: String) -> Vec<u8> {
        encode_string(key)
    }
    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        decode_string(bytes)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BatchMint {}

#[deprecated]
#[derive(Serialize, Deserialize, Clone)]
pub struct BatchMintWithStaker {
    pub batch_mint: BatchMint,
    pub staker: Pubkey,
}
