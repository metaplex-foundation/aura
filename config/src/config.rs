use crate::error::ConfigError;
use serde::de::DeserializeOwned;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct IntegrityVerificationConfig {
    pub metrics_port: u16,
}

pub fn parse_config<T>(path: &str) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    if !path.is_empty() {
        dotenvy::from_filename(path).map_err(|_| ConfigError::InvalidFileName(path.to_string()))?;
    }

    let config: T = envious::Config::default().build_from_env()?;

    Ok(config)
}
