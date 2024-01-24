use figment::providers::Env;
use figment::Figment;
use serde_derive::Deserialize;

#[derive(Deserialize, Default, PartialEq, Debug, Clone)]
pub enum TestSourceMode {
    File,
    #[default]
    Database,
}

#[derive(Deserialize, Debug)]
pub struct IntegrityVerificationConfig {
    pub metrics_port: u16,
    pub reference_host: String,
    pub testing_host: String,
    pub database_url: Option<String>,
    pub run_secondary_indexes_tests: Option<bool>,
    pub run_proofs_tests: Option<bool>,
    pub run_assets_tests: Option<bool>,
    pub test_source_mode: TestSourceMode,
    pub test_file_path: Option<String>,
}

impl IntegrityVerificationConfig {
    pub fn get_run_secondary_indexes_tests(&self) -> bool {
        self.run_secondary_indexes_tests.unwrap_or_default()
    }
    pub fn get_run_proofs_tests(&self) -> bool {
        self.run_proofs_tests.unwrap_or_default()
    }
    pub fn get_run_assets_tests(&self) -> bool {
        self.run_assets_tests.unwrap_or_default()
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
