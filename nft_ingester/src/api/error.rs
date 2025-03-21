use interface::error::UsecaseError;
use jsonrpc_core::ErrorCode;
use rocks_db::errors::StorageError;
use thiserror::Error;
use tracing::error;

const STANDARD_ERROR_CODE: i64 = -32000;
const QUERY_TIME_OUT_CODE: i64 = -32800;
pub const CANNOT_SERVICE_REQUEST_ERROR_CODE: i64 = -32050;

#[derive(Error, Debug)]
pub enum DasApiError {
    #[error("Config Missing or Error: {0}")]
    ConfigurationError(String),
    #[error("Database Error: {0}")]
    DatabaseError(#[from] sqlx::Error),
    #[error("Pubkey Validation Err: {0} is invalid")]
    PubkeyValidationError(String),
    #[error("Pagination Error. Only one pagination parameter supported per query.")]
    PaginationError,
    #[error("Database Error: {0}")]
    DatabaseErrorOther(String),
    #[error("Pagination Error. No Pagination Method Selected")]
    PaginationEmptyError,
    #[error("Batch Size Error. Batch size should not be greater than {0}.")]
    BatchSizeError(usize),
    #[error("RocksDB error: {0}")]
    RocksError(String),
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
    InternalDbError,
    #[error("CannotServiceRequest")]
    CannotServiceRequest,
    #[error("MissingOwnerAddress")]
    MissingOwnerAddress,
    #[error("Request execution time exceeded the limit.")]
    QueryTimedOut,
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
                message: "Asset Not Found".to_string(),
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
                message: "Asset Proof Not Found".to_string(),
                data: None,
            },
            DasApiError::Validation(msg) => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: format!("Validation Error: {msg}"),
                data: None,
            },
            DasApiError::MissingOwnerAddress => jsonrpc_core::Error {
                code: ErrorCode::ServerError(STANDARD_ERROR_CODE),
                message: "Need to specify \"ownerAddress\" when using \"showNativeBalance\""
                    .to_string(),
                data: None,
            },
            DasApiError::QueryTimedOut => jsonrpc_core::Error {
                code: ErrorCode::ServerError(QUERY_TIME_OUT_CODE),
                message: "Request execution time exceeded the limit.".to_string(),
                data: None,
            },
            DasApiError::CannotServiceRequest => cannot_service_request_error(),
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

impl From<StorageError> for DasApiError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::CannotServiceRequest => Self::CannotServiceRequest,
            StorageError::QueryTimedOut => Self::QueryTimedOut,
            e => Self::RocksError(e.to_string()),
        }
    }
}

pub fn cannot_service_request_error() -> jsonrpc_core::types::error::Error {
    jsonrpc_core::types::error::Error {
        code: ErrorCode::ServerError(CANNOT_SERVICE_REQUEST_ERROR_CODE),
        message: "Cannot service request".to_string(),
        data: None,
    }
}
