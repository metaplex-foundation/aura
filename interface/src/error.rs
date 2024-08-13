use plerkle_serialization::error::PlerkleSerializationError;
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
    Anchor(#[from] anchor_lang::error::Error),
    #[error("BatchMintValidation {0}")]
    BatchMintValidation(
        #[from] bubblegum_batch_sdk::batch_mint_validations::BatchMintValidationError,
    ),
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
    Anchor(#[from] anchor_lang::error::Error),
    #[error("RollupValidation: {0}")]
    RollupValidation(String),
    #[error("TreeAccountNotFound {0}")]
    TreeAccountNotFound(String),
}

#[derive(Debug, Clone)]
pub enum JsonDownloaderError {
    GotNotJsonFile,
    CouldNotDeserialize,
    CouldNotReadHeader,
    ErrorStatusCode(String),
    ErrorDownloading(String),
    IndexStorageError(String),
    MainStorageError(String),
}

/// Errors that may occur during the block consuming.
#[derive(Error, Debug)]
pub enum BlockConsumeError {
    #[error("{0}")]
    PersistenceErr(#[from] StorageError),
    // TODO: think of other possible scenarios
}
