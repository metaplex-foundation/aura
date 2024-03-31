use crate::{api::error::DasApiError, config::default_max_page_limit};
use {
    figment::{providers::Env, Figment},
    serde::Deserialize,
};

const fn default_synchronization_api_threshold() -> u64 {
    1_000_000
}

const fn default_consistence_backfilling_slots_threshold() -> u64 {
    100
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub database_url: String,
    pub metrics_port: Option<u16>,
    pub metrics_host: Option<String>,
    pub server_port: u16,
    pub env: Option<String>,
    pub archives_dir: String,
    #[serde(default = "default_max_page_limit")]
    pub max_page_limit: usize,
    #[serde(default = "default_synchronization_api_threshold")]
    pub consistence_synchronization_api_threshold: u64,
    #[serde(default = "default_consistence_backfilling_slots_threshold")]
    pub consistence_backfilling_slots_threshold: u64,
}

pub fn load_config() -> Result<Config, DasApiError> {
    Figment::new()
        .join(Env::prefixed("APP_"))
        .extract()
        .map_err(|config_error| DasApiError::ConfigurationError(config_error.to_string()))
}
