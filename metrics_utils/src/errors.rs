use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum MetricsError {
    #[error("{0}")]
    Unexpected(String),
    #[error("Metrics server isn't started: {0}")]
    MetricsServer(String),
}
