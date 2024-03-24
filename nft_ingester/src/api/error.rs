use jsonrpc_core::ErrorCode;
use log::error;

use interface::error::UsecaseError;
use rocks_db::errors::StorageError;
use thiserror::Error;

const STANDARD_ERROR_CODE: i64 = -32000;

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
    #[error("Batch Size Error. Batch size should not be greater than {0}.")]
    BatchSizeError(usize),
    #[error("RocksDB error: {0}")]
    RocksError(#[from] StorageError),
    #[error("No data found.")]
    NoDataFoundError,
    #[error("Invalid Grouping Key: {0}")]
    InvalidGroupingKey(String),
    #[error("Usecase: {0}")]
    Usecase(String),
    #[error("ProofNotFound")]
    ProofNotFound,
    #[error("Validation: {0}")]
    Validation(String),
    #[error("Page number is too big. Up to {0} pages are supported with this kind of pagination. Please use a different pagination(before/after/cursor).")]
    PageTooBig(usize),
    #[error("Internal DB error")]
    InternalDdError,
}

impl From<DasApiError> for jsonrpc_core::Error {
    fn from(value: DasApiError) -> Self {
        match value {
            DasApiError::PubkeyValidationError(key) => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: format!("Pubkey Validation Error: {key} is invalid"),
                data: None,
            },
            DasApiError::PaginationError => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: "Pagination Error. Only one pagination parameter supported per query."
                    .to_string(),
                data: None,
            },
            DasApiError::PaginationEmptyError => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: "Pagination Error. No Pagination Method Selected".to_string(),
                data: None,
            },
            DasApiError::NoDataFoundError => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: "Database Error: RecordNotFound Error: Asset Not Found".to_string(),
                data: None,
            },
            DasApiError::InvalidGroupingKey(key) => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: format!("Invalid Grouping Key: {key}"),
                data: None,
            },
            DasApiError::BatchSizeError(size) => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: format!("Batch Size Error. Batch size should not be greater than {size}."),
                data: None,
            },
            DasApiError::ProofNotFound => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: "Database Error: RecordNotFound Error: Asset Proof Not Found".to_string(),
                data: None,
            },
            DasApiError::Validation(msg) => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: format!("Validation Error: {msg}"),
                data: None,
            },
            _ => jsonrpc_core::Error::new(ErrorCode::InternalError),
        }
    }
}

impl From<UsecaseError> for DasApiError {
    fn from(value: UsecaseError) -> Self {
        match value {
            UsecaseError::PubkeyValidationError(e) => Self::PubkeyValidationError(e),
            UsecaseError::InvalidGroupingKey(e) => Self::InvalidGroupingKey(e),
            e => Self::Usecase(e.to_string()),
        }
    }
}
