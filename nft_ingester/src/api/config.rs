use crate::{api::error::DasApiError, config::default_max_page_limit};
use {
    figment::{providers::Env, Figment},
    serde::Deserialize,
};

#[derive(Deserialize)]
pub struct Config {
    pub database_url: String,
    pub metrics_port: Option<u16>,
    pub metrics_host: Option<String>,
    pub server_port: u16,
    pub batch_mint_service_port: u16,
    pub file_storage_path_container: String,
    pub env: Option<String>,
    pub archives_dir: String,
    #[serde(default = "default_max_page_limit")]
    pub max_page_limit: usize,
}

pub fn load_config() -> Result<Config, DasApiError> {
    Figment::new()
        .join(Env::prefixed("APP_"))
        .extract()
        .map_err(|config_error| DasApiError::ConfigurationError(config_error.to_string()))
}
