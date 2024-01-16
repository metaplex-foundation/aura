use plerkle_serialization::error::PlerkleSerializationError;
use solana_client::client_error::ClientError;
use solana_sdk::signature::ParseSignatureError;
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
}

impl From<ClientError> for UsecaseError {
    fn from(value: ClientError) -> Self {
        Self::SolanaRPC(value.kind.to_string())
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("common error: {0}")]
    Common(String),
}
