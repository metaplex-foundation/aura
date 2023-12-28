use std::net::AddrParseError;

use blockbuster::error::BlockbusterError;
use flatbuffers::InvalidFlatbuffer;
use plerkle_messenger::MessengerError;
use plerkle_serialization::error::PlerkleSerializationError;
use sea_orm::{DbErr, TransactionError};
use solana_sdk::pubkey::ParsePubkeyError;
use solana_sdk::signature::ParseSignatureError;
use solana_transaction_status::EncodeError;
use thiserror::Error;

use rocks_db::errors::{BackupServiceError, StorageError};

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IngesterError {
    #[error("ChangeLog Event Malformed")]
    ChangeLogEventMalformed,
    #[error("Compressed Asset Event Malformed")]
    CompressedAssetEventMalformed,
    #[error("Network Error: {0}")]
    BatchInitNetworkingError(String),
    #[error("Error writing batch files")]
    BatchInitIOError,
    #[error("Storage listener error: ({msg})")]
    StorageListenerError { msg: String },
    #[error("Storage Write Error: {0}")]
    StorageWriteError(String),
    #[error("NotImplemented")]
    NotImplemented,
    #[error("Deserialization Error: {0}")]
    DeserializationError(String),
    #[error("Task Manager Error: {0}")]
    TaskManagerError(String),
    #[error("Missing or invalid configuration: ({msg})")]
    ConfigurationError { msg: String },
    #[error("Error getting RPC data: {0}")]
    RpcGetDataError(String),
    #[error("RPC returned data in unsupported format: {0}")]
    RpcDataUnsupportedFormat(String),
    #[error("Data serializaton error: {0}")]
    SerializatonError(String),
    #[error("Messenger error; {0}")]
    MessengerError(String),
    #[error("Blockbuster Parsing error: {0}")]
    ParsingError(String),
    #[error("Database Error: {0}")]
    DatabaseError(String),
    #[error("Unknown Task Type: {0}")]
    UnknownTaskType(String),
    #[error("BG Task Manager Not Started")]
    TaskManagerNotStarted,
    #[error("Unrecoverable task error: {0}")]
    UnrecoverableTaskError(String),
    #[error("Cache Storage Write Error: {0}")]
    CacheStorageWriteError(String),
    #[error("HttpError {status_code}")]
    HttpError { status_code: String },
    #[error("AssetIndex Error {0}")]
    AssetIndexError(String),
    #[error("Backfill sender error: {0}")]
    BackfillSenderError(String),
    #[error("Slot doesn't have tree signatures {0}")]
    SlotDoesntHaveTreeSignatures(String),
    #[error("DB error {0}")]
    DbError(String),
    #[error("Error getting data from BigTable {0}")]
    BigTableError(String),
    #[error("Missing flatbuffers field {0}")]
    MissingFlatbuffersFieldError(String),
    #[error("parse pubkey {0}")]
    ParsePubkeyError(String),
    #[error("invalid flatbuffer {0}")]
    InvalidFlatbufferError(String),
    #[error("bincode {0}")]
    BincodeError(String),
    #[error("geyser {0}")]
    GeyserError(String),
    #[error("encode tx {0}")]
    SolanaEncodeTxError(String),
    #[error("parse signature {0}")]
    ParseSignatureError(String),
    #[error("empty before signature")]
    EmptyBeforeSignature,
    #[error("sqlx {0}")]
    SqlxError(String),
    #[error("Error to deserialise account with plerkle {0}")]
    AccountParsingError(String),
    #[error("Error to deserialise transaction {0}")]
    TransactionParsingError(String),
    #[error("Error to convert data into PubKey {0}")]
    PubKeyParsingError(String),
    #[error("Transaction was not processed {0}")]
    TransactionNotProcessedError(String),
    #[error("backup service {0}")]
    BackupError(String),
    #[error("Trying to run services with empty DB. Please restart app with added --restore-rocks-db flag")]
    EmptyDataBase,
    #[error("Error on parsing {0}")]
    ConfigurationParsingError(String),
    #[error("Error on GRPC {0}")]
    GrpcError(String),
}

impl From<reqwest::Error> for IngesterError {
    fn from(err: reqwest::Error) -> Self {
        IngesterError::BatchInitNetworkingError(err.to_string())
    }
}

impl From<stretto::CacheError> for IngesterError {
    fn from(err: stretto::CacheError) -> Self {
        IngesterError::CacheStorageWriteError(err.to_string())
    }
}

impl From<serde_json::Error> for IngesterError {
    fn from(_err: serde_json::Error) -> Self {
        IngesterError::SerializatonError("JSON ERROR".to_string())
    }
}

impl From<BlockbusterError> for IngesterError {
    fn from(err: BlockbusterError) -> Self {
        IngesterError::ParsingError(err.to_string())
    }
}

impl From<std::io::Error> for IngesterError {
    fn from(_err: std::io::Error) -> Self {
        IngesterError::BatchInitIOError
    }
}

impl From<DbErr> for IngesterError {
    fn from(e: DbErr) -> Self {
        IngesterError::StorageWriteError(e.to_string())
    }
}

impl From<TransactionError<IngesterError>> for IngesterError {
    fn from(e: TransactionError<IngesterError>) -> Self {
        IngesterError::StorageWriteError(e.to_string())
    }
}

impl From<MessengerError> for IngesterError {
    fn from(e: MessengerError) -> Self {
        IngesterError::MessengerError(e.to_string())
    }
}

impl From<PlerkleSerializationError> for IngesterError {
    fn from(e: PlerkleSerializationError) -> Self {
        IngesterError::SerializatonError(e.to_string())
    }
}

impl From<ParsePubkeyError> for IngesterError {
    fn from(e: ParsePubkeyError) -> Self {
        IngesterError::ParsePubkeyError(e.to_string())
    }
}

impl From<InvalidFlatbuffer> for IngesterError {
    fn from(e: InvalidFlatbuffer) -> Self {
        IngesterError::InvalidFlatbufferError(e.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for IngesterError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        IngesterError::BincodeError(e.to_string())
    }
}

impl From<EncodeError> for IngesterError {
    fn from(e: EncodeError) -> Self {
        IngesterError::SolanaEncodeTxError(e.to_string())
    }
}

impl From<solana_storage_bigtable::Error> for IngesterError {
    fn from(e: solana_storage_bigtable::Error) -> Self {
        IngesterError::BigTableError(e.to_string())
    }
}

impl From<ParseSignatureError> for IngesterError {
    fn from(e: ParseSignatureError) -> Self {
        IngesterError::ParseSignatureError(e.to_string())
    }
}

impl From<sqlx::Error> for IngesterError {
    fn from(e: sqlx::Error) -> Self {
        IngesterError::SqlxError(e.to_string())
    }
}

impl From<BackupServiceError> for IngesterError {
    fn from(value: BackupServiceError) -> Self {
        IngesterError::BackupError(value.to_string())
    }
}

impl From<StorageError> for IngesterError {
    fn from(e: StorageError) -> Self {
        IngesterError::DatabaseError(e.to_string())
    }
}

// TODO: refactor to use the real errors from the postgres package
impl From<String> for IngesterError {
    fn from(e: String) -> Self {
        IngesterError::DatabaseError(e)
    }
}

impl From<AddrParseError> for IngesterError {
    fn from(e: AddrParseError) -> Self {
        IngesterError::ConfigurationParsingError(e.to_string())
    }
}

impl From<tonic::transport::Error> for IngesterError {
    fn from(e: tonic::transport::Error) -> Self {
        IngesterError::GrpcError(e.to_string())
    }
}
