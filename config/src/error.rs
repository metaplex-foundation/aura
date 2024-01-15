use envious::EnvDeserializationError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConfigError {
    #[error("Invalid config file: {0}")]
    InvalidFileName(String),
    #[error("Deserialize {0}")]
    Deserialize(#[from] EnvDeserializationError),
}
