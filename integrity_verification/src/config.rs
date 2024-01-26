use figment::providers::Env;
use figment::Figment;
use serde_derive::Deserialize;

#[derive(Deserialize, Default, PartialEq, Debug, Clone)]
pub enum TestSourceMode {
    File,
    #[default]
    Database,
}

fn default_bool() -> bool {
    false
}

#[derive(Deserialize, Debug)]
pub struct IntegrityVerificationConfig {
    pub metrics_port: u16,
    pub reference_host: String,
    pub testing_host: String,
    pub database_url: Option<String>,
    #[serde(default = "default_bool")]
    pub run_secondary_indexes_tests: bool,
    #[serde(default = "default_bool")]
    pub run_proofs_tests: bool,
    #[serde(default = "default_bool")]
    pub run_assets_tests: bool,
    pub test_source_mode: TestSourceMode,
    pub test_file_path_container: Option<String>,
    pub big_table_creds_path: Option<String>,
    pub slots_collect_path_container: Option<String>,
    #[serde(default = "default_bool")]
    pub collect_slots: bool,
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
