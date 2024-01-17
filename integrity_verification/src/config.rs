use figment::providers::Env;
use figment::Figment;
use serde_derive::Deserialize;

#[derive(Deserialize, Debug)]
pub struct IntegrityVerificationConfig {
    pub metrics_port: u16,
    pub reference_host: String,
    pub tested_host: String,
    pub database_url: String,
    pub sql_log_level: Option<String>,
}

impl IntegrityVerificationConfig {
    pub fn get_sql_log_level(&self) -> String {
        self.sql_log_level.clone().unwrap_or("error".to_string())
    }
}

// Use unwraps because it just config-setup stage
// and we need to stop processing if we cannot get it
pub fn setup_config(path: &str) -> IntegrityVerificationConfig {
    if !path.is_empty() {
        // load envs from file
        dotenvy::from_filename(path).unwrap();
    }

    let figment = Figment::new()
        .join(Env::prefixed("INTEGRITY_VERIFICATION_"))
        .join(Env::raw());

    figment.extract().unwrap()
}
