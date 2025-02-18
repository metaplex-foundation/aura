use std::fmt::Display;

use plerkle_serialization::error::PlerkleSerializationError;
use reqwest::StatusCode;
use solana_client::client_error::ClientError;
use solana_program::pubkey::ParsePubkeyError;
use solana_sdk::signature::ParseSignatureError;
use solana_storage_bigtable::Error;
use thiserror::Error;

// TODO: rename the error enum as soon as it gets at least 3 errors
#[derive(Error, Debug, PartialEq)]
pub enum UsecaseError {
    #[error("The range is invalid. Start slot {0} is greater than end slot {1}.")]
    InvalidRange(u64, u64),
    #[error(
        "The range is too wide. Start slot {0} and end slot {1} are more than {2} slots apart."
    )]
    InvalidRangeTooWide(u64, u64, u64),
    #[error("Solana RPC error {0}")]
    SolanaRPC(String),
    #[error("ParseSignature {0}")]
    ParseSignature(#[from] ParseSignatureError),
    #[error("PlerkleSerialization {0}")]
    PlerkleSerialization(#[from] PlerkleSerializationError),
    #[error("Pubkey Validation Err: {0} is invalid")]
    PubkeyValidationError(String),
    #[error("Invalid Grouping Key: {0}")]
    InvalidGroupingKey(String),
    #[error("Bigtable: {0}")]
    Bigtable(String),
    #[error("InvalidParameters: {0}")]
    InvalidParameters(String),
    #[error("Storage: {0}")]
    Storage(String),
    #[error("Reqwest: {0}")]
    Reqwest(String),
    #[error("Json: {0}")]
    Json(String),
    #[error("HashMismatch: expected {0}, actual file hash {1}")]
    HashMismatch(String, String),
    #[error("Serialization: {0}")]
    Serialization(String),
    #[error("Anchor {0}")]
    Anchor(String),
    #[error("EmptyPriceFetcherResponse {0}")]
    EmptyPriceFetcherResponse(String),
    #[error("Messenger {0}")]
    Messenger(MessengerError),
    #[error("Unexpected tree depth={0} and max size={1}")]
    UnexpectedTreeSize(u32, u32),
}

#[derive(Debug, Error, PartialEq)]
pub enum MessengerError {
    #[error("Stream {0} is empty")]
    Empty(String),
    #[error("Redis error: {0}")]
    Redis(String),
}

impl From<ClientError> for UsecaseError {
    fn from(value: ClientError) -> Self {
        Self::SolanaRPC(value.kind.to_string())
    }
}
impl From<reqwest::Error> for UsecaseError {
    fn from(value: reqwest::Error) -> Self {
        Self::Reqwest(value.to_string())
    }
}
impl From<serde_json::Error> for UsecaseError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}
impl From<anchor_lang::error::Error> for UsecaseError {
    fn from(value: anchor_lang::error::Error) -> Self {
        Self::Anchor(value.to_string())
    }
}

// TODO: Probably need to expand this to cover all error cases.
//       e.g. by making fully compliant with rocks_db::error::StorageError
#[derive(Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("Common error: {0}")]
    Common(String),
    #[error("Serialize error: {0}")]
    Serialize(String),
    #[error("Deserialize error: {0}")]
    Deserialize(String),
    #[error("Database specific error: {0}")]
    DatabaseSpecificErr(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("CannotServiceRequest")]
    CannotServiceRequest,
    #[error("Unknown error: {0}")]
    Unknown(String),
    #[error("Request execution time exceeded the limit.")]
    QueryTimedOut,
}

impl From<Error> for UsecaseError {
    fn from(value: Error) -> Self {
        Self::Bigtable(value.to_string())
    }
}

#[derive(Error, Debug)]
pub enum IntegrityVerificationError {
    #[error("Json {0}")]
    Json(#[from] serde_json::Error),
    #[error("Reqwest {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("IO {0}")]
    IO(#[from] std::io::Error),
    #[error("FetchKeys {0}")]
    FetchKeys(String),
    #[error("RPC {0}")]
    Rpc(#[from] ClientError),
    #[error("Cannot get response field {0}")]
    CannotGetResponseField(String),
    #[error("ParsePubkey {0}")]
    ParsePubkey(#[from] ParsePubkeyError),
    #[error("Anchor {0}")]
    Anchor(String),
    #[error("RollupValidation: {0}")]
    RollupValidation(String),
    #[error("TreeAccountNotFound {0}")]
    TreeAccountNotFound(String),
}

impl From<anchor_lang::error::Error> for IntegrityVerificationError {
    fn from(value: anchor_lang::error::Error) -> Self {
        Self::Anchor(value.to_string())
    }
}

#[derive(Error, Debug)]
pub enum JsonDownloaderError {
    #[error("Didn't get a JSON file")]
    GotNoJsonFile,
    #[error("Could not deserialize JSON")]
    CouldNotDeserialize,
    #[error("Could not read header")]
    CouldNotReadHeader,
    #[error("Received {0} status code")]
    ErrorStatusCode(StatusCode),
    #[error("Error downloading: {0}")]
    ErrorDownloading(JsonDownloadErrors),
    #[error("Index Storage Error happened: {0}")]
    IndexStorageError(String),
    #[error("Main Storage Error happened: {0}")]
    MainStorageError(String),
}

#[derive(Debug)]
pub enum JsonDownloadErrors {
    FailedToCreateClient(reqwest::Error),
    FailedToParseIpfsUrl(url::ParseError),
    FailedToParseUrl(url::ParseError),
    FailedToMakeRequest(reqwest_middleware::Error),
}

impl Display for JsonDownloadErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonDownloadErrors::FailedToCreateClient(e) => {
                write!(f, "Failed to create client: {}", e)
            },
            JsonDownloadErrors::FailedToParseIpfsUrl(e) => {
                write!(f, "Failed to parse IPFS URL: {}", e)
            },
            JsonDownloadErrors::FailedToParseUrl(e) => {
                write!(f, "Failed to parse URL: {}", e)
            },
            JsonDownloadErrors::FailedToMakeRequest(e) => {
                write!(f, "Failed to make request {}", e)
            },
        }
    }
}

/// Errors that may occur during the block consuming.
#[derive(Error, Debug)]
pub enum BlockConsumeError {
    #[error("{0}")]
    PersistenceErr(#[from] StorageError),
    // TODO: think of other possible scenarios
}
