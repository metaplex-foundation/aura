use reqwest;
use std::array::TryFromSliceError;
use std::io;
use std::io::Error;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum BackupServiceError {
    #[error("Backup engine info is empty")]
    BackupEngineInfoIsEmpty {},
    #[error("Backup error for {0}: {1}")]
    BackupEngineInfo(u32, String),
    #[error("Backup size is zero for: {0}")]
    BackupEngineInfoSizeIsZero(u32),
    #[error("{0}")]
    DatabaseError(String),
    #[error("Config Missing or Error: {0}")]
    ConfigurationError(String),
    #[error("{0}")]
    StdError(String),
    #[error("reqwest: HTTP request failed with status code {0}")]
    ReqwestError(String),
}

impl From<rocksdb::Error> for BackupServiceError {
    fn from(err: rocksdb::Error) -> Self {
        BackupServiceError::DatabaseError(err.to_string())
    }
}

impl From<io::Error> for BackupServiceError {
    fn from(value: Error) -> Self {
        BackupServiceError::StdError(value.to_string())
    }
}

impl From<reqwest::Error> for BackupServiceError {
    fn from(value: reqwest::Error) -> Self {
        BackupServiceError::ReqwestError(value.to_string())
    }
}

// TODO-XXX: probably it is good to come up with a common StorageError for pg and rocks
#[derive(Error, Debug)]
pub enum StorageError {
    Common(String),
    RocksDb(#[from] rocksdb::Error),
    Serialize(#[from] Box<bincode::ErrorKind>),
    TryFromSliceError(#[from] TryFromSliceError),
    NoAssetOwner(String),
    InvalidKeyLength,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage error: {:?}", self)
    }
}

impl From<StorageError> for interface::error::StorageError {
    fn from(val: StorageError) -> Self {
        use interface::error::StorageError as InterfaceStorageError;
        match val {
            StorageError::Common(s) => InterfaceStorageError::Common(s),
            StorageError::RocksDb(e) => InterfaceStorageError::Common(e.to_string()),
            StorageError::Serialize(e) => InterfaceStorageError::Common(e.to_string()),
            StorageError::TryFromSliceError(e) => InterfaceStorageError::Common(e.to_string()),
            StorageError::NoAssetOwner(s) => InterfaceStorageError::Common(s),
            StorageError::InvalidKeyLength => {
                InterfaceStorageError::Common(String::from("InvalidKeyLength"))
            }
        }
    }
}
