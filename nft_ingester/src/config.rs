use clap::{Parser, ValueEnum};
use figment::{providers::Env, Figment};
use interface::asset_streaming_and_discovery::PeerDiscovery;
use serde::Deserialize;
use solana_sdk::commitment_config::CommitmentLevel;
use std::fmt::{Display, Formatter};
use std::net::ToSocketAddrs;
use figment::value::Dict;
use tracing_subscriber::fmt;

use crate::error::IngesterError;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IngesterClapArgs {
    #[clap(short('d'), long, help = "example: postgres://solana:solana@localhost:5432/aura_db")]
    pub pg_database_url: String,

    #[clap(short('m'), long, default_value = "./my_rocksdb", help = "Rocks db path container")]
    pub rocks_db_path_container: String,

    #[clap(short('f'), long, default_value = "./temp_file_storage", help = "File storage path")]
    pub file_storage_path_container:String,

    #[clap(short('p'), long, help = "example: https://mainnet-aura.metaplex.com")]
    pub rpc_host: String,

    #[clap(long, default_value = "100")]
    pub pg_max_db_connections: u32,

    #[clap(short('r'), long, default_value="{redis_connection_str=\"redis://127.0.0.1:6379/0\"}", value_parser = parse_json_to_dict)]
    pub redis_connection_config: Dict,

    #[clap(long, default_value = "20")]
    pub redis_accounts_parsing_workers: u32,

    #[clap(long, default_value = "20")]
    pub redis_transactions_parsing_workers: u32,

    #[clap(long, default_value = "60")]
    pub sequence_consistent_checker_wait_period_sec: u64,

    #[clap(long, default_value = "250")]
    pub account_processor_buffer_size:usize,

    #[clap(long, default_value = "50")]
    pub account_processor_mpl_fees_buffer_size: usize,

    #[clap(long, default_value = "100")]
    pub parallel_json_downloaders: i32,

    #[clap(long("run_api"), default_value_t=false, help = "Run API (default: false)")]
    pub is_run_api: bool,

    #[clap(long("run_gapfiller"), default_value_t=false, help = "Start gapfiller",
        requires = "gapfiller_peer_addr",
    )]
    pub is_run_gapfiller: bool,

    #[clap(long, default_value = "0.0.0.0", help = "Gapfiller peer address")]
    pub gapfiller_peer_addr: String,

    #[clap(long("run_profiling"), default_value_t=false, help = "Start profiling (default: false)")]
    pub is_run_profiling: bool,


    #[clap(long, value_parser = parse_json_to_json_middleware_config,  help = "Example: {'is_enabled':true, 'max_urls_to_parse':10} ",)]
    pub json_middleware_config: Option<JsonMiddlewareConfig>,

    // Group: Rocks DB Configuration
    #[clap(long("restore_rocks_db"), default_value_t=false, help = "Try restore rocks (default: false)",
        requires = "rocks_backup_url",
        requires = "rocks_backup_archives_dir",
    )]
    pub is_restore_rocks_db: bool,
    #[clap(long, help = "Rocks backup url")]
    pub rocks_backup_url: Option<String>,
    #[clap(long, help = "Rocks backup archives dir")]
    pub rocks_backup_archives_dir: Option<String>,

    #[clap(long, default_value_t=false, help = "Start onsistent checker (default: false)")]
    pub run_sequence_consistent_checker: bool,


    #[clap(long, default_value_t=false, help = "Check proofs (default: false)")]
    pub check_proofs: bool,
    #[clap(long, default_value = "0.1", help = "Check proofs probability" )]
    pub check_proofs_probability: f64,
    #[clap(long, default_value = "finalized", value_enum, help = "Check proofs commitment. [possible values: max, recent, root, single, singleGossip, processed, confirmed, finalized]" )]
    pub check_proofs_commitment: CommitmentLevel,

    #[clap(long, default_value = "/rocksdb/_rocks_backup_archives", help ="#api Archives directory")]
    pub archives_dir: String,
    #[clap(long, default_value_t=false, help = "#api Skip check tree gaps. (default: false)")]
    pub skip_check_tree_gaps: bool,
    #[clap(long, default_value = "8990", help ="#api Server port")]
    pub server_port: u16,
    #[clap(long, default_value = "50", help ="#api Max page limit")]
    pub max_page_limit: usize,
    #[clap(long, default_value = "So11111111111111111111111111111111111111112", help ="#api Native mint pubkey")]
    pub native_mint_pubkey: String,
    #[clap(long, help ="#api Consistence synchronization api threshold")]
    pub consistence_synchronization_api_threshold: Option<u64>,
    #[clap(long, help ="#api Consistence backfilling slots threshold")]
    pub consistence_backfilling_slots_threshold: Option<u64>,
    #[clap(long, help ="#api Batch mint service port")]
    pub batch_mint_service_port: Option<u16>,
    #[clap(long, help ="#api Storage service base url")]
    pub storage_service_base_url: Option<String>,

    #[clap(long, default_value_t=false, help = "Start backfiller (default: false)",
        requires = "slots_db_path",
        requires = "secondary_slots_db_path",
    )]
    pub is_run_backfiller: bool,
    #[clap(long, help ="#backfiller Path to the RocksDB instance with slots (required for the backfiller to work)")]
    pub slots_db_path: Option<String>,
    #[clap(long, help ="#backfiller Path to the secondary RocksDB instance with slots (required for the backfiller to work)")]
    pub secondary_slots_db_path: Option<String>,
    #[clap(long, help ="#backfiller Backfill rpc address")]
    pub backfill_rpc_address: Option<String>,
    #[clap(long, default_value = "rpc", help ="#backfiller Backfill source mode.")]
    pub backfiller_source_mode: BackfillerSourceMode,
    #[clap(long, value_parser = parse_json_to_big_table_config, help ="#backfiller Big table config")]
    pub big_table_config: Option<BigTableConfig>,

    #[clap(long, default_value_t=false, help = "#bubbl Start bubblegum backfiller (default: false)")]
    pub is_run_bubblegum_backfiller: bool,
    #[clap(long, default_value_t=false, help = "#bubbl Should reingest, deleting last backfilled slot. (default: false)")]
    pub should_reingest: bool,
    #[clap(long, default_value = "ingest-directly", help ="#bubbl Backfiller mode.")]
    pub backfiller_mode: BackfillerMode,

    #[clap(long, default_value = "1000000", help ="#grpc Max gap slots")]
    pub peer_grpc_max_gap_slots: u64,
    #[clap(long, default_value = "9099", help ="#grpc Grpc port")]
    pub peer_grpc_port: u16,
    #[clap(long, default_value = "500", help ="#grpc retry interval millis")]
    pub rpc_retry_interval_millis: u64,


    #[clap(long, help ="Metrics port")]
    pub metrics_port: Option<u16>,
    pub profiling_file_path_container: Option<String>,
    #[clap(long, default_value = "/usr/src/app/heaps", help ="Heap path")]
    pub heap_path: String,

    #[clap(long, default_value = "info", help = "info|debug")]
    pub log_level: String,
}

fn parse_json_to_dict(s: &str) -> Result<Dict, String> {
    parse_json(s)
}

fn parse_json_to_json_middleware_config(s: &str) -> Result<JsonMiddlewareConfig, String> {
    parse_json(s)
}

fn parse_json_to_big_table_config(s: &str) -> Result<BigTableConfig, String> {
    parse_json(s)
}

fn parse_json<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, String> {
    serde_json::from_str(s).map_err(|e| format!("Failed to parse JSON: {}", e))
}

pub const INGESTER_BACKUP_NAME: &str = "snapshot.tar.lz4";
pub const INGESTER_CONFIG_PREFIX: &str = "INGESTER_";
pub const SYNCHRONIZER_CONFIG_PREFIX: &str = "SYNCHRONIZER_";
fn default_rocks_backup_archives_dir() -> String {
    String::from("/rocksdb/_rocks_backup_archives")
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone, ValueEnum)]
pub enum BackfillerMode {
    None,
    Persist,
    PersistAndIngest,
    IngestPersisted,
    #[default]
    IngestDirectly,
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone, Copy, ValueEnum)]
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

impl Display for MessageSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageSource::Redis => write!(f, "Redis"),
            MessageSource::TCP => write!(f, "TCP"),
        }
    }
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
    pub rpc_host: String,
    pub database_config: DatabaseConfig,
    pub file_storage_path_container: String,

    pub log_level: Option<String>,
    pub metrics_port: Option<u16>,
    pub batch_mint_service_port: Option<u16>,
    pub storage_service_base_url: Option<String>,
    pub rocks_db_path_container: Option<String>,
    pub profiling_file_path_container: Option<String>,
    pub json_middleware_config: Option<JsonMiddlewareConfig>,
    pub consistence_synchronization_api_threshold: Option<u64>,
    pub consistence_backfilling_slots_threshold: Option<u64>,
    pub rocks_db_secondary_path_container: Option<String>,
    
    #[serde(default)]
    pub run_profiling: bool,
    #[serde(default)]
    pub check_proofs: bool,
    #[serde(default)]
    pub skip_check_tree_gaps: bool,
    #[serde(default = "default_peer_grpc_port")]
    pub peer_grpc_port: u16,
    #[serde(default = "default_server_port")]
    pub server_port: u16,
    #[serde(default = "default_max_page_limit")]
    pub max_page_limit: usize,
    #[serde(default = "default_heap_path")]
    pub heap_path: String,
    #[serde(default = "default_native_mint")]
    pub native_mint_pubkey: String,
    #[serde(default = "default_rocks_backup_archives_dir")]
    pub archives_dir: String,
    #[serde(default = "default_check_proofs_probability")]
    pub check_proofs_probability: f64,
    #[serde(default = "default_check_proofs_commitment")]
    pub check_proofs_commitment: CommitmentLevel,
    #[serde(default = "default_grpc_max_gap_slots")]
    pub peer_grpc_max_gap_slots: u64,
    #[serde(default = "default_rocks_sync_interval_seconds")]
    pub rocks_sync_interval_seconds: u64,
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
const fn default_rocks_sync_interval_seconds() -> u64 {
    2
}
const fn default_server_port() -> u16 {
    8990
}
const fn default_grpc_max_gap_slots() -> u64 {
    1000000
}
const fn default_peer_grpc_port() -> u16 {
    9099
}
const fn default_check_proofs_commitment() -> CommitmentLevel {
    CommitmentLevel::Finalized
}

pub const fn default_max_page_limit() -> usize {
    50
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


pub fn setup_config<'a, T: Deserialize<'a>>(config_prefix: &str) -> T {
    dotenvy::dotenv().ok();

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
    use clap::Parser;

    #[test]
    fn test_default_values() {
        let args = IngesterClapArgs::parse_from(&[
            "test",
            "--pg_database_url", "postgres://solana:solana@localhost:5432/aura_db",
            "--rpc_host", "https://mainnet-aura.metaplex.com",
        ]);

        assert_eq!(args.rocks_db_path_container, "./my_rocksdb");
        assert_eq!(args.file_storage_path_container, "./temp_file_storage");
        assert_eq!(args.pg_max_db_connections, 100);
        assert_eq!(args.redis_accounts_parsing_workers, 20);
        assert_eq!(args.redis_transactions_parsing_workers, 20);
        assert_eq!(args.sequence_consistent_checker_wait_period_sec, 60);
        assert_eq!(args.account_processor_buffer_size, 250);
        assert_eq!(args.account_processor_mpl_fees_buffer_size, 50);
        assert_eq!(args.parallel_json_downloaders, 100);
        assert_eq!(args.is_run_api, false);
        assert_eq!(args.is_run_gapfiller, false);
        assert_eq!(args.is_run_profiling, false);
        assert_eq!(args.is_restore_rocks_db, false);
        assert_eq!(args.is_run_bubblegum_backfiller, false);
        assert_eq!(args.run_sequence_consistent_checker, false);
        assert_eq!(args.should_reingest, false);
        assert_eq!(args.check_proofs, false);
        assert_eq!(args.check_proofs_commitment, CommitmentLevel::Finalized);
        assert_eq!(args.archives_dir, "/rocksdb/_rocks_backup_archives");
        assert_eq!(args.skip_check_tree_gaps, false);
        assert_eq!(args.is_run_backfiller, false);
        assert_eq!(args.backfiller_source_mode, BackfillerSourceMode::RPC);
        assert_eq!(args.backfiller_mode, BackfillerMode::IngestDirectly);
    }
}