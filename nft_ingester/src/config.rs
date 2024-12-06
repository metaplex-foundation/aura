use core::time;
use std::net::{SocketAddr, ToSocketAddrs};

use figment::{providers::Env, Figment};
use interface::asset_streaming_and_discovery::PeerDiscovery;
use plerkle_messenger::MessengerConfig;
use serde::Deserialize;
use solana_sdk::commitment_config::CommitmentLevel;
use tracing_subscriber::fmt;

use crate::error::IngesterError;

pub const INGESTER_BACKUP_NAME: &str = "snapshot.tar.lz4";

pub const INGESTER_CONFIG_PREFIX: &str = "INGESTER_";
pub const SYNCHRONIZER_CONFIG_PREFIX: &str = "SYNCHRONIZER_";
pub const JSON_MIGRATOR_CONFIG_PREFIX: &str = "JSON_MIGRATOR_";

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct BackfillerConfig {
    #[serde(default)]
    pub backfiller_source_mode: BackfillerSourceMode,
    pub big_table_config: BigTableConfig,
    pub rpc_host: String,
    pub slot_until: Option<u64>,
    pub slot_start_from: u64,
    #[serde(default)]
    pub backfiller_mode: BackfillerMode,
    #[serde(default = "default_workers_count")]
    pub workers_count: usize,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    #[serde(default = "default_permitted_tasks")]
    pub permitted_tasks: usize,
    #[serde(default = "default_wait_period_sec")]
    pub wait_period_sec: u64,
    #[serde(default)]
    pub should_reingest: bool,
}
const fn default_wait_period_sec() -> u64 {
    60
}

const fn default_workers_count() -> usize {
    100
}

const fn default_chunk_size() -> usize {
    5
}

const fn default_permitted_tasks() -> usize {
    500
}

const fn default_mpl_core_fees_buffer_size() -> usize {
    50
}

const fn default_price_monitoring_interval_sec() -> u64 {
    30
}

const fn default_snapshot_parsing_workers() -> u32 {
    1
}

const fn default_snapshot_parsing_batch_size() -> usize {
    500
}

fn default_rocks_backup_url() -> String {
    String::from("127.0.0.1:3051/snapshot")
}

fn default_rocks_backup_archives_dir() -> String {
    String::from("/rocksdb/_rocks_backup_archives")
}

fn default_rocks_backup_dir() -> String {
    String::from("/rocksdb/_rocksdb_backup")
}

fn default_gapfiller_peer_addr() -> String {
    String::from("0.0.0.0")
}

impl BackfillerConfig {
    pub fn get_slot_until(&self) -> u64 {
        self.slot_until.unwrap_or_default()
    }
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone)]
pub enum BackfillerMode {
    None,
    Persist,
    PersistAndIngest,
    IngestPersisted,
    #[default]
    IngestDirectly,
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone, Copy)]
pub enum BackfillerSourceMode {
    Bigtable,
    #[default]
    RPC,
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone, Copy)]
pub enum MessageSource {
    #[default]
    Redis,
    TCP,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct RawBackfillConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub metrics_port: Option<u16>,
    pub rocks_db_path_container: Option<String>,
    #[serde(default)]
    pub run_profiling: bool,
    pub profiling_file_path_container: Option<String>,
    #[serde(default = "default_heap_path")]
    pub heap_path: String,
    pub migration_storage_path: String,
}
#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct IngesterConfig {
    pub database_config: DatabaseConfig,
    pub tcp_config: TcpConfig,
    pub redis_messenger_config: MessengerConfig,
    pub message_source: MessageSource,
    pub accounts_buffer_size: usize,
    #[serde(default = "default_snapshot_parsing_workers")]
    pub snapshot_parsing_workers: u32,
    #[serde(default = "default_snapshot_parsing_batch_size")]
    pub snapshot_parsing_batch_size: usize,
    pub accounts_parsing_workers: u32,
    pub transactions_parsing_workers: u32,
    #[serde(default = "default_mpl_core_fees_buffer_size")]
    pub mpl_core_fees_buffer_size: usize,
    pub metrics_port: Option<u16>,
    pub rocks_db_path_container: Option<String>,
    #[serde(default = "default_rocks_backup_url")]
    pub rocks_backup_url: String,
    #[serde(default = "default_rocks_backup_archives_dir")]
    pub rocks_backup_archives_dir: String,
    #[serde(default = "default_rocks_backup_dir")]
    pub rocks_backup_dir: String,
    pub run_bubblegum_backfiller: bool,
    #[serde(default = "default_gapfiller_peer_addr")]
    pub gapfiller_peer_addr: String,
    pub peer_grpc_port: u16,
    pub peer_grpc_max_gap_slots: u64,
    pub log_level: Option<String>,
    pub backfill_rpc_address: String,
    pub run_profiling: Option<bool>,
    pub profiling_file_path_container: Option<String>,
    pub store_db_backups: Option<bool>,
    #[serde(default)]
    pub rpc_retry_interval_millis: u64,
    #[serde(default)]
    pub run_sequence_consistent_checker: bool,
    #[serde(default)]
    pub run_fork_cleaner: bool,
    #[serde(default = "default_sequence_consistent_checker_wait_period_sec")]
    pub sequence_consistent_checker_wait_period_sec: u64,
    pub rpc_host: String,
    #[serde(default)]
    pub check_proofs: bool,
    #[serde(default = "default_check_proofs_probability")]
    pub check_proofs_probability: f64,
    #[serde(default = "default_check_proofs_commitment")]
    pub check_proofs_commitment: CommitmentLevel,
    #[serde(default)]
    pub backfiller_source_mode: BackfillerSourceMode,
    #[serde(default = "default_synchronizer_parallel_tasks")]
    pub synchronizer_parallel_tasks: usize,
    #[serde(default)]
    pub run_temp_sync_during_dump: bool,
    #[serde(default = "default_parallel_json_downloaders")]
    pub parallel_json_downloaders: i32,
    pub json_middleware_config: Option<JsonMiddlewareConfig>,
    #[serde(default = "default_heap_path")]
    pub heap_path: String,
    pub migration_storage_path: String,
    #[serde(default = "default_price_monitoring_interval_sec")]
    pub price_monitoring_interval_sec: u64,
}

pub const fn default_parallel_json_downloaders() -> i32 {
    100
}

const fn default_synchronizer_parallel_tasks() -> usize {
    20
}

const fn default_dump_sync_threshold() -> i64 {
    100_000_000
}
fn default_dump_path() -> String {
    "/tmp/sync_dump".to_string()
}

const fn default_dump_synchronizer_batch_size() -> usize {
    200_000
}

const fn default_sequence_consistent_checker_wait_period_sec() -> u64 {
    60
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct JsonMigratorConfig {
    pub log_level: Option<String>,
    pub json_source_db: String,
    pub json_target_db: String,
    pub database_config: DatabaseConfig,
    pub metrics_port: u16,
    pub work_mode: JsonMigratorMode,
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone)]
pub enum JsonMigratorMode {
    #[default]
    Full,
    JsonsOnly,
    TasksOnly,
}

impl JsonMigratorConfig {
    pub fn get_log_level(&self) -> String {
        self.log_level.clone().unwrap_or("warn".to_string())
    }
}

fn default_log_level() -> String {
    "warn".to_string()
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct SynchronizerConfig {
    pub database_config: DatabaseConfig,
    pub rocks_db_path_container: Option<String>,
    pub rocks_db_secondary_path_container: Option<String>,
    pub metrics_port: Option<u16>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub run_profiling: bool,
    pub profiling_file_path_container: Option<String>,
    #[serde(default = "default_dump_synchronizer_batch_size")]
    pub dump_synchronizer_batch_size: usize,
    #[serde(default = "default_dump_path")]
    pub dump_path: String,
    #[serde(default = "default_dump_sync_threshold")]
    pub dump_sync_threshold: i64,
    #[serde(default)]
    pub timeout_between_syncs_sec: u64,
    #[serde(default = "default_synchronizer_parallel_tasks")]
    pub parallel_tasks: usize,
    #[serde(default)]
    pub run_temp_sync_during_dump: bool,
    #[serde(default = "default_heap_path")]
    pub heap_path: String,
}

fn default_native_mint() -> String {
    String::from("So11111111111111111111111111111111111111112")
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct ApiConfig {
    pub database_config: DatabaseConfig,
    pub rocks_db_path_container: Option<String>,
    pub rocks_db_secondary_path_container: Option<String>,
    pub rocks_sync_interval_seconds: u64,
    pub metrics_port: Option<u16>,
    pub server_port: u16,
    pub batch_mint_service_port: Option<u16>,
    pub file_storage_path_container: String,
    pub log_level: Option<String>,
    pub peer_grpc_port: u16,
    pub peer_grpc_max_gap_slots: u64,
    #[serde(default)]
    pub run_profiling: bool,
    pub profiling_file_path_container: Option<String>,
    pub rpc_host: String,
    #[serde(default)]
    pub check_proofs: bool,
    #[serde(default = "default_check_proofs_probability")]
    pub check_proofs_probability: f64,
    #[serde(default = "default_check_proofs_commitment")]
    pub check_proofs_commitment: CommitmentLevel,
    #[serde(default = "default_max_page_limit")]
    pub max_page_limit: usize,
    pub json_middleware_config: Option<JsonMiddlewareConfig>,
    pub archives_dir: String,
    pub consistence_synchronization_api_threshold: Option<u64>,
    #[serde(default = "default_heap_path")]
    pub heap_path: String,
    pub consistence_backfilling_slots_threshold: Option<u64>,
    pub storage_service_base_url: Option<String>,
    #[serde(default)]
    pub skip_check_tree_gaps: bool,
    #[serde(default = "default_native_mint")]
    pub native_mint_pubkey: String,
}

fn default_heap_path() -> String {
    "/usr/src/app/heaps".to_string()
}

#[derive(Deserialize, PartialEq, Debug, Clone, Default)]
pub struct JsonMiddlewareConfig {
    pub is_enabled: bool,
    pub max_urls_to_parse: usize,
}

const fn default_check_proofs_probability() -> f64 {
    0.1
}

const fn default_check_proofs_commitment() -> CommitmentLevel {
    CommitmentLevel::Finalized
}

pub const fn default_max_page_limit() -> usize {
    50
}

impl ApiConfig {
    pub fn get_log_level(&self) -> String {
        self.log_level.clone().unwrap_or("warn".to_string())
    }
}

impl IngesterConfig {
    pub fn get_log_level(&self) -> String {
        self.log_level.clone().unwrap_or("warn".to_string())
    }

    pub fn get_is_run_profiling(&self) -> bool {
        self.run_profiling.unwrap_or_default()
    }
    pub fn store_db_backups(&self) -> bool {
        self.store_db_backups.unwrap_or_default()
    }
}

// Types and constants used for Figment configuration items.
#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct DatabaseConfig(figment::value::Dict);

pub const DATABASE_URL_KEY: &str = "url";
pub const MAX_POSTGRES_CONNECTIONS: &str = "max_postgres_connections";

impl DatabaseConfig {
    pub fn get_database_url(&self) -> Result<String, IngesterError> {
        self.0
            .get(DATABASE_URL_KEY)
            .and_then(|u| u.clone().into_string())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("Database connection string missing: {}", DATABASE_URL_KEY),
            })
    }

    pub fn get_max_postgres_connections(&self) -> Option<u32> {
        self.0
            .get(MAX_POSTGRES_CONNECTIONS)
            .and_then(|a| a.to_u128().map(|res| res as u32))
    }
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct BigTableConfig(figment::value::Dict);

pub const BIG_TABLE_CREDS_KEY: &str = "creds";
pub const BIG_TABLE_TIMEOUT_KEY: &str = "timeout";

impl BigTableConfig {
    pub fn get_big_table_creds_key(&self) -> Result<String, IngesterError> {
        Ok(self
            .0
            .get(BIG_TABLE_CREDS_KEY)
            .and_then(|v| v.as_str())
            .ok_or(IngesterError::ConfigurationError {
                msg: "BIG_TABLE_CREDS_KEY missing".to_string(),
            })?
            .to_string())
    }

    pub fn get_big_table_timeout_key(&self) -> Result<u32, IngesterError> {
        Ok(self
            .0
            .get(BIG_TABLE_TIMEOUT_KEY)
            .and_then(|v| v.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: "BIG_TABLE_TIMEOUT_KEY missing".to_string(),
            })? as u32)
    }
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct TcpConfig(figment::value::Dict);

pub const TCP_RECEIVER_ADDR: &str = "receiver_addr";
pub const TCP_RECEIVER_RECONNECT_INTERVAL: &str = "receiver_reconnect_interval";
pub const TCP_SNAPSHOT_RECEIVER_ADDR: &str = "snapshot_receiver_addr";

impl TcpConfig {
    pub fn get_tcp_receiver_reconnect_interval(&self) -> Result<time::Duration, IngesterError> {
        self.get_duration(TCP_RECEIVER_RECONNECT_INTERVAL)
    }

    pub fn get_tcp_receiver_addr_ingester(&self) -> Result<SocketAddr, IngesterError> {
        self.get_addr(TCP_RECEIVER_ADDR)
    }

    pub fn get_snapshot_addr_ingester(&self) -> Result<SocketAddr, IngesterError> {
        self.get_addr(TCP_SNAPSHOT_RECEIVER_ADDR)
    }

    fn get_duration(&self, key: &str) -> Result<time::Duration, IngesterError> {
        Ok(time::Duration::from_secs(
            self.0
                .get(key)
                .and_then(|a| a.to_u128())
                .ok_or(IngesterError::ConfigurationError {
                    msg: format!("Config key is missing: {}", key),
                })? as u64,
        ))
    }

    fn get_addr(&self, key: &str) -> Result<SocketAddr, IngesterError> {
        self.0
            .get(key)
            .and_then(|a| a.as_str())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("TCP receiver address missing: {}", key),
            })?
            .to_socket_addrs()?
            .next()
            .ok_or(IngesterError::ConfigurationError {
                msg: "SocketAddr".to_string(),
            })
    }
}

impl PeerDiscovery for IngesterConfig {
    fn get_gapfiller_peer_addr(&self) -> String {
        self.gapfiller_peer_addr.clone()
    }
}

pub fn setup_config<'a, T: Deserialize<'a>>(config_prefix: &str) -> T {
    let figment = Figment::new()
        .join(Env::prefixed(config_prefix))
        .join(Env::raw());

    figment
        .extract()
        .map_err(|config_error| IngesterError::ConfigurationError {
            msg: format!("{}", config_error),
        })
        .unwrap()
}

pub fn init_logger(log_level: &str) {
    let t = tracing_subscriber::fmt().with_env_filter(log_level);
    t.event_format(fmt::format::json()).init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use lazy_static::lazy_static;
    use std::sync::Mutex;

    lazy_static! {
        static ref ENV_MUTEX: Mutex<()> = Mutex::new(());
    }

    #[test]
    fn test_setup_default_backfiller_config() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("INGESTER_DATABASE_CONFIG", "{}");
        std::env::set_var("INGESTER_TCP_CONFIG", "{}");
        std::env::set_var("INGESTER_BIG_TABLE_CONFIG", "{}");
        std::env::set_var("INGESTER_SLOT_START_FROM", "0");
        std::env::set_var("INGESTER_RPC_HOST", "f");
        let config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);
        assert_eq!(
            config,
            BackfillerConfig {
                backfiller_source_mode: BackfillerSourceMode::RPC,
                big_table_config: BigTableConfig(figment::value::Dict::new()),
                rpc_host: "f".to_string(),
                slot_until: None,
                slot_start_from: 0,
                backfiller_mode: BackfillerMode::IngestDirectly,
                workers_count: 100,
                chunk_size: 5,
                permitted_tasks: 500,
                wait_period_sec: 60,
                should_reingest: false,
            }
        );
        std::env::remove_var("INGESTER_DATABASE_CONFIG");
        std::env::remove_var("INGESTER_TCP_CONFIG");
        std::env::remove_var("INGESTER_BIG_TABLE_CONFIG");
        std::env::remove_var("INGESTER_SLOT_START_FROM");
        std::env::remove_var("INGESTER_RPC_HOST");
    }

    #[test]
    fn test_setup_backfiller_config_backfill_mode() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("INGESTER_DATABASE_CONFIG", "{}");
        std::env::set_var("INGESTER_TCP_CONFIG", "{}");
        std::env::set_var("INGESTER_BIG_TABLE_CONFIG", "{}");
        std::env::set_var("INGESTER_SLOT_START_FROM", "0");
        std::env::set_var("INGESTER_BACKFILLER_MODE", "Persist");
        std::env::set_var("INGESTER_RPC_HOST", "f");
        let config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);
        assert_eq!(
            config,
            BackfillerConfig {
                backfiller_source_mode: BackfillerSourceMode::RPC,
                big_table_config: BigTableConfig(figment::value::Dict::new()),
                rpc_host: "f".to_string(),
                slot_until: None,
                slot_start_from: 0,
                backfiller_mode: BackfillerMode::Persist,
                workers_count: 100,
                chunk_size: 5,
                permitted_tasks: 500,
                wait_period_sec: 60,
                should_reingest: false,
            }
        );
        std::env::remove_var("INGESTER_DATABASE_CONFIG");
        std::env::remove_var("INGESTER_TCP_CONFIG");
        std::env::remove_var("INGESTER_BIG_TABLE_CONFIG");
        std::env::remove_var("INGESTER_SLOT_START_FROM");
        std::env::remove_var("INGESTER_BACKFILLER_MODE");
        std::env::remove_var("INGESTER_RPC_HOST");
    }
}
