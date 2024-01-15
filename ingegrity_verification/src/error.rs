use config::error::ConfigError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IntegrityVerificationError {
    #[error("Config {0}")]
    Config(#[from] ConfigError),
}
