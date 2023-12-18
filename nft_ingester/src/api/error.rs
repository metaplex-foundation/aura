use jsonrpc_core::ErrorCode;
use log::error;

use rocks_db::errors::StorageError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DasApiError {
    #[error("Config Missing or Error: {0}")]
    ConfigurationError(String),
    #[error("Database Connection Failed")]
    DatabaseConnectionError(#[from] sqlx::Error),
    #[error("Pubkey Validation Err: {0} is invalid")]
    PubkeyValidationError(String),
    #[error("Database Error: {0}")]
    DatabaseError(#[from] sea_orm::DbErr),
    #[error("Pagination Error. Only one pagination parameter supported per query.")]
    PaginationError,
    #[error("Pagination Error. No Pagination Method Selected")]
    PaginationEmptyError,
    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] serde_json::Error),
    #[error("Batch Size Error. Batch size should not be greater than {0}.")]
    BatchSizeError(usize),
    #[error("RocksDB error: {0}")]
    RocksError(#[from] StorageError),
    #[error("Internal server error: {0}")]
    InternalServerError(String),
    #[error("No data found.")]
    NoDataFoundError,
    #[error("Invalid Grouping Key: {0}")]
    InvalidGroupingKey(String),
}

impl From<DasApiError> for jsonrpc_core::Error {
    fn from(value: DasApiError) -> Self {
        match value {
            DasApiError::PubkeyValidationError { .. } => jsonrpc_core::Error {
                code: ErrorCode::InvalidParams,
                message: "Pubkey Validation Error.".to_string(),
                data: None,
            },
            DasApiError::PaginationError { .. } => jsonrpc_core::Error {
                code: ErrorCode::InvalidParams,
                message: "Pagination parameters parsing error.".to_string(),
                data: None,
            },
            DasApiError::PaginationEmptyError { .. } => jsonrpc_core::Error {
                code: ErrorCode::InvalidParams,
                message: "No pagination parameters provided.".to_string(),
                data: None,
            },
            DasApiError::NoDataFoundError => jsonrpc_core::Error {
                code: ErrorCode::InvalidParams,
                message: "No data found.".to_string(),
                data: None,
            },
            _ => jsonrpc_core::Error::new(ErrorCode::InternalError),
        }
    }
}
