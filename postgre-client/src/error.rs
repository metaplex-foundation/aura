use thiserror::Error;

/// Indexing database (currently PostgreSQL) related errors.
#[derive(Error, Debug)]
pub enum IndexDbError {
    #[error("Failed to decode Base64")]
    Base64DecodingErr,
    #[error("Invalid sorting key")]
    InvalidSortingKeyErr,
    #[error("Query execution error: {0}")]
    QueryExecErr(#[from] sqlx::Error),
    #[error("Malformed PubKey: {0}")]
    PubkeyParsingError(String),
    #[error("No implemented: {0}")]
    NotImplemented(String),
    #[error("Bad argument: {0}")]
    BadArgument(String),
    #[error("Join Error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

impl From<IndexDbError> for String {
    fn from(value: IndexDbError) -> Self {
        value.to_string()
    }
}
