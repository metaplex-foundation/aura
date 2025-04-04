use std::{fs::File, io::Read, path::PathBuf};

use clap::{Parser, ValueEnum};
// TODO: replace String paths with PathBuf
use figment::value::Dict;
use serde::Deserialize;
use solana_sdk::commitment_config::CommitmentLevel;
use tracing_subscriber::fmt;

use crate::error::IngesterError;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IngesterClapArgs {
    #[clap(long, env, help = "Node name to simplify monitoring staff")]
    pub node_name: Option<String>,

    #[clap(
        short('d'),
        long,
        env,
        help = "example: postgres://solana:solana@localhost:5432/aura_db"
    )]
    pub pg_database_url: String,

    #[clap(
        short('m'),
        long,
        env,
        default_value = "./my_rocksdb",
        help = "Rocks db path container"
    )]
    pub rocks_db_path: String,

    #[clap(
        short('f'),
        long,
        env,
        default_value = "./tmp/file_storage",
        help = "File storage path"
    )]
    pub file_storage_path_container: String,

    #[clap(short('p'), long, env, help = "example: https://mainnet-aura.metaplex.com")]
    pub rpc_host: String,

    #[clap(long, env, default_value = "100")]
    pub pg_max_db_connections: u32,
    #[clap(long, env = "INGESTER_PG_MAX_QUERY_TIMEOUT_SECS", default_value = "120")]
    pub pg_max_query_statement_timeout_secs: u32,

    #[clap(short('r'), long, env, help="example: {\"redis_connection_str\": \"redis://127.0.0.1:6379/0\"}", value_parser = parse_json::<Dict>)]
    pub redis_connection_config: Dict,

    #[clap(long, env, default_value = "5")]
    pub redis_accounts_parsing_workers: u32,

    #[clap(long, env, default_value = "5")]
    pub redis_account_backfill_parsing_workers: u32,

    #[clap(long, env, default_value = "2")]
    pub redis_transactions_parsing_workers: u32,

    #[clap(long, env, default_value = "60")]
    pub sequence_consistent_checker_wait_period_sec: u64,

    #[clap(long, env, default_value = "100")]
    pub account_processor_buffer_size: usize,

    #[clap(long, env, default_value = "1000")]
    pub account_backfill_processor_buffer_size: usize,

    #[clap(long, env, default_value = "100")]
    pub tx_processor_buffer_size: usize,

    #[clap(long, env, default_value = "50")]
    pub account_processor_mpl_fees_buffer_size: usize,

    #[clap(long, env, default_value = "100")]
    pub parallel_json_downloaders: i32,

    #[clap(
        long,
        env,
        default_value = "true",
        help = "Skip inline json refreshes if the metadata may be stale"
    )]
    pub api_skip_inline_json_refresh: Option<bool>,

    #[clap(
        long("run-api"),
        default_value = "true",
        env = "RUN_API",
        help = "Run API (default: true)"
    )]
    pub run_api: Option<bool>,

    #[clap(
        long("run-gapfiller"),
        default_value_t = false,
        env = "RUN_GAPFILLER",
        help = "Start gapfiller.  (default: false)",
        requires = "gapfiller_peer_addr"
    )]
    pub run_gapfiller: bool,

    #[clap(long, env, default_value = "0.0.0.0", help = "Gapfiller peer address")]
    pub gapfiller_peer_addr: Option<String>,

    #[clap(
        long,
        default_value_t = false,
        env = "INGESTER_RUN_PROFILING",
        help = "Start profiling (default: false)"
    )]
    pub run_profiling: bool,

    #[clap(long, env, value_parser = parse_json::<JsonMiddlewareConfig>, default_value = "{\"is_enabled\":true, \"max_urls_to_parse\":10}",  help = "Example: {'is_enabled':true, 'max_urls_to_parse':10} ")]
    pub json_middleware_config: Option<JsonMiddlewareConfig>,

    // Group: Rocks DB Configuration
    #[clap(
        long("restore-rocks-db"),
        default_value_t = false,
        env = "RESTORE_ROCKS_DB",
        help = "Try restore rocks (default: false)",
        requires = "rocks_backup_url",
        requires = "rocks_backup_archives_dir"
    )]
    pub is_restore_rocks_db: bool,
    #[clap(long, env, help = "Rocks backup url")]
    pub rocks_backup_url: Option<String>,
    #[clap(long, env, help = "Rocks backup archives dir")]
    pub rocks_backup_archives_dir: Option<String>,

    #[clap(
        long,
        env = "ENABLE_ROCKS_MIGRATION",
        default_value = "true",
        help = "Enable migration for rocksdb (default: true) requires: rocks_migration_storage_path"
    )]
    pub enable_rocks_migration: Option<bool>,
    #[clap(
        long,
        env,
        requires_if("true", "enable_rocks_migration"),
        help = "Migration storage path dir"
    )]
    pub rocks_migration_storage_path: Option<String>,

    #[clap(long, env, help = "Start consistent checker (default: false)")]
    pub run_sequence_consistent_checker: bool,

    #[clap(
        long,
        env = "CHECK_PROOFS",
        default_value_t = false,
        help = "#api Check proofs (default: false)"
    )]
    pub check_proofs: bool,
    #[clap(long, env, default_value = "0.1", help = "#api Check proofs probability")]
    pub check_proofs_probability: f64,
    #[clap(
        long,
        env,
        default_value = "finalized",
        value_enum,
        help = "#api Check proofs commitment. [possible values: max, recent, root, single, singleGossip, processed, confirmed, finalized]"
    )]
    pub check_proofs_commitment: CommitmentLevel,

    #[clap(
        long,
        env,
        default_value = "/rocksdb/_rocks_backup_archives",
        help = "#api Archives directory"
    )]
    pub archives_dir: String,
    #[clap(
        long,
        env = "SKIP_CHECK_TREE_GAPS",
        default_value_t = false,
        help = "#api Skip check tree gaps. (default: false)"
    )]
    pub skip_check_tree_gaps: bool,
    #[clap(long, env = "INGESTER_SERVER_PORT", default_value = "9092", help = "#api Server port")]
    pub server_port: u16,
    #[clap(long, env = "INGESTER_MANAGEMENT_API_PORT", help = "#management Management API port")]
    pub management_api_port: Option<u16>,
    #[clap(long, env, default_value = "50", help = "#api Max page limit")]
    pub max_page_limit: usize,
    #[clap(
        long,
        env,
        default_value = "So11111111111111111111111111111111111111112",
        help = "#api Native mint pubkey"
    )]
    pub native_mint_pubkey: String,
    #[clap(long, env, help = "#api Consistence synchronization api threshold")]
    pub consistence_synchronization_api_threshold: Option<u64>,
    #[clap(long, env, help = "#api Consistence backfilling slots threshold")]
    pub consistence_backfilling_slots_threshold: Option<u64>,
    #[clap(long, env, help = "#api Batch mint service port")]
    pub batch_mint_service_port: Option<u16>,
    #[clap(long, env, help = "#api Storage service base url")]
    pub storage_service_base_url: Option<String>,

    #[clap(
        long,
        env = "RUN_BACKFILLER",
        default_value = "true",
        help = "Run backfiller. (default: true) requires: rocks_slots_db_path"
    )]
    pub run_backfiller: Option<bool>,
    #[clap(
        long,
        env,
        requires_if("true", "run_backfiller"),
        help = "#backfiller Path to the RocksDB instance with slots (required for the backfiller to work)"
    )]
    pub rocks_slots_db_path: Option<String>,

    #[clap(
        long,
        env,
        default_value = "./tmp/file_storage/rocks/secondary/ingester-slots",
        help = "#backfiller Path to the secondary RocksDB instance with slots (required for the backfiller to work)"
    )]
    pub rocks_secondary_slots_db_path: String,

    #[clap(long, env, help = "#backfiller Backfill rpc address")]
    pub backfill_rpc_address: Option<String>,
    #[clap(long, env, default_value = "rpc", help = "#backfiller Backfill source mode.")]
    pub backfiller_source_mode: BackfillerSourceMode,
    #[clap(long, env, value_parser = parse_json::<BigTableConfig>, help ="#backfiller Big table config")]
    pub big_table_config: Option<BigTableConfig>,

    #[clap(
        long,
        env = "RUN_BUBBLEGUM_BACKFILLER",
        default_value = "true",
        help = "#bubbl Run bubblegum backfiller (default: true)"
    )]
    pub run_bubblegum_backfiller: Option<bool>,
    #[clap(
        long,
        env = "SHOULD_REINGEST",
        default_value_t = false,
        help = "#bubbl Should reingest, deleting last backfilled slot. (default: false)"
    )]
    pub should_reingest: bool,

    #[clap(long, env, default_value = "9099", help = "#grpc Grpc port")]
    pub peer_grpc_port: u16,
    #[clap(long, env, default_value = "1000000", help = "#grpc Max gap slots")]
    pub peer_grpc_max_gap_slots: u64,
    #[clap(long, env, default_value = "500", help = "#grpc retry interval millis")]
    pub rpc_retry_interval_millis: u64,

    #[clap(
        long,
        env = "INGESTER_METRICS_PORT",
        help = "Metrics port. Start HTTP server to report metrics if port exist."
    )]
    pub metrics_port: Option<u16>,

    pub profiling_file_path_container: Option<String>,
    #[clap(long, env, default_value = "/usr/src/app/heaps", help = "Heap path")]
    pub heap_path: String,

    #[clap(long, env, default_value = "info", help = "info|debug")]
    pub log_level: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct SynchronizerClapArgs {
    #[clap(
        short('d'),
        long,
        env,
        help = "example: postgres://solana:solana@localhost:5432/aura_db"
    )]
    pub pg_database_url: String,
    #[clap(long, env, default_value = "100")]
    pub pg_max_db_connections: u32,
    // 24 hour = 86400 secs
    #[clap(long, env = "SYNCHRONIZER_PG_MAX_QUERY_TIMEOUT_SECS", default_value = "86400")]
    pub pg_max_query_statement_timeout_secs: u32,

    #[clap(
        short('m'),
        long,
        env,
        default_value = "./my_rocksdb",
        help = "Rocks db path container"
    )]
    pub rocks_db_path: String,
    #[clap(
        long,
        env,
        default_value = "./my_rocksdb_secondary",
        help = "Rocks db secondary path container"
    )]
    pub rocks_db_secondary_path: String,
    #[clap(long, env, default_value = "./tmp/rocks_dump", help = "RocksDb dump path")]
    pub rocks_dump_path: String,

    #[clap(
        long("run-profiling"),
        env = "SYNCHRONIZER_RUN_PROFILING",
        default_value_t = false,
        help = "Start profiling (default: false)"
    )]
    pub run_profiling: bool,
    #[clap(long, env, default_value = "/usr/src/app/heaps", help = "Heap path")]
    pub heap_path: String,

    #[clap(
        long,
        env = "SYNCHRONIZER_METRICS_PORT",
        help = "Metrics port. Start HTTP server to report metrics if port exist."
    )]
    pub metrics_port: Option<u16>,
    pub profiling_file_path_container: Option<String>,

    #[clap(long, env, default_value = "10000")]
    pub dump_synchronizer_batch_size: usize,
    #[clap(
        long,
        env,
        default_value = "150000000",
        help = "Threshold on the number of updates not being synchronized for the synchronizer to dump-load on start. 150M - that's a rough threshold after which the synchronizer will likely complete a full dymp-load cycle faster then doing an incremental sync "
    )]
    pub dump_sync_threshold: i64,
    #[clap(long, env, default_value = "30")]
    pub synchronizer_parallel_tasks: usize,
    #[clap(long, env, default_value = "0")]
    pub timeout_between_syncs_sec: u64,

    #[clap(long, env, default_value = "info", help = "warn|info|debug")]
    pub log_level: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct RocksDbBackupServiceClapArgs {
    #[clap(long, env, default_value = "./my_rocksdb", help = "Rocks db path container")]
    pub rocks_db_path: PathBuf,
    #[clap(long, env, default_value = "./my_rocksdb_secondary", help = "Rocks db secondary path")]
    pub rocks_db_secondary_path: PathBuf,
    #[clap(long, env = "ROCKS_BACKUP_ARCHIVES_DIR", help = "Rocks backup archives dir")]
    pub backup_archives_dir: PathBuf,
    #[clap(long, env = "ROCKS_BACKUP_DIR", help = "Rocks backup dir")]
    pub backup_dir: PathBuf,
    #[clap(
        long,
        env = "ROCKS_FLUSH_BEFORE_BACKUP",
        help = "Whether to flush RocksDb before backup"
    )]
    pub flush_before_backup: bool,

    #[clap(long, env, default_value = "info", help = "warn|info|debug")]
    pub log_level: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct MigratorClapArgs {
    #[clap(long, env, help = "Rocks json source db dir")]
    pub rocks_json_source_db: String,
    #[clap(long, env, help = "Rocks json target db dir")]
    pub rocks_json_target_db: String,
    #[clap(long, env, default_value = "./tmp/rocks_dump", help = "RocksDb dump path")]
    pub rocks_dump_path: String,

    #[clap(long, env, default_value = "full", help = "Json migrator mode")]
    pub migrator_mode: JsonMigratorMode,

    #[clap(
        short('d'),
        long,
        env,
        help = "example: postgres://solana:solana@localhost:5432/aura_db"
    )]
    pub pg_database_url: String,
    #[clap(long, env, default_value = "10")]
    pub pg_min_db_connections: u32,
    #[clap(long, env, default_value = "250")]
    pub pg_max_db_connections: u32,
    // 24 hour = 86400 secs
    #[clap(long, env = "MIGRATOR_PG_MAX_QUERY_TIMEOUT_SECS", default_value = "86400")]
    pub pg_max_query_statement_timeout_secs: u32,

    #[clap(
        long,
        env = "MIGRATOR_METRICS_PORT",
        help = "Metrics port. Start HTTP server to report metrics if port exist."
    )]
    pub metrics_port: Option<u16>,

    #[clap(long, env, default_value = "info", help = "warn|info|debug")]
    pub log_level: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct ApiClapArgs {
    #[clap(long, env, help = "Node name to simplify monitoring staff")]
    pub node_name: Option<String>,

    #[clap(short('p'), long, env, help = "example: https://mainnet-aura.metaplex.com")]
    pub rpc_host: String,

    #[clap(
        short('d'),
        long,
        env,
        help = "example: postgres://solana:solana@localhost:5432/aura_db"
    )]
    pub pg_database_url: String,
    #[clap(long, env, default_value = "10")]
    pub pg_min_db_connections: u32,
    #[clap(long, env, default_value = "250")]
    pub pg_max_db_connections: u32,
    #[clap(long, env = "API_PG_MAX_QUERY_TIMEOUT_SECS", default_value = "120")]
    pub pg_max_query_statement_timeout_secs: u32,

    #[clap(
        short('m'),
        long,
        env,
        default_value = "./my_rocksdb",
        help = "Rocks db path container"
    )]
    pub rocks_db_path: String,
    #[clap(
        long,
        env,
        default_value = "./my_rocksdb_secondary",
        help = "Rocks db secondary path container"
    )]
    pub rocks_db_secondary_path: String,
    #[clap(long, env, default_value = "2")]
    pub rocks_sync_interval_seconds: u64,
    #[clap(long, env, default_value = "./tmp/rocks_dump", help = "RocksDb dump path")]
    pub rocks_dump_path: String,

    #[clap(long, env, default_value = "/usr/src/app/heaps", help = "Heap path")]
    pub heap_path: String,

    #[clap(
        long,
        env = "API_METRICS_PORT",
        help = "Metrics port. Start HTTP server to report metrics if port exist."
    )]
    pub metrics_port: Option<u16>,
    pub profiling_file_path_container: Option<String>,
    #[clap(
        long,
        env = "SKIP_CHECK_TREE_GAPS",
        default_value_t = false,
        help = "#api Skip check tree gaps. (default: false) It will check if asset which was requested is from the tree which has gaps in sequences, gap in sequences means missed transactions and  as a result incorrect asset data."
    )]
    pub skip_check_tree_gaps: bool,
    #[clap(
        env = "API_RUN_PROFILING",
        long("run-profiling"),
        default_value_t = false,
        help = "Start profiling. (default: false)"
    )]
    pub run_profiling: bool,

    #[clap(
        long,
        env = "CHECK_PROOFS",
        default_value_t = false,
        help = "Check proofs (default: false)"
    )]
    pub check_proofs: bool,
    #[clap(long, env, default_value = "0.1", help = "Check proofs probability")]
    pub check_proofs_probability: f64,
    #[clap(
        long,
        env,
        default_value = "finalized",
        value_enum,
        help = "Check proofs commitment. [possible values: max, recent, root, single, singleGossip, processed, confirmed, finalized]"
    )]
    pub check_proofs_commitment: CommitmentLevel,

    #[clap(
        short('f'),
        long,
        env,
        default_value = "./tmp/file_storage",
        help = "File storage path"
    )]
    pub file_storage_path_container: String,
    #[clap(
        long,
        env,
        default_value = "/rocksdb/_rocks_backup_archives",
        help = "#api Archives directory"
    )]
    pub rocks_archives_dir: String,
    #[clap(long, env = "API_SERVER_PORT", default_value = "8990", help = "#api Server port")]
    pub server_port: u16,
    #[clap(long, env, default_value = "50", help = "#api Max page limit")]
    pub max_page_limit: usize,
    #[clap(
        long,
        env,
        default_value = "So11111111111111111111111111111111111111112",
        help = "#api Native mint pubkey"
    )]
    pub native_mint_pubkey: String,
    #[clap(long, env, help = "#api Consistence synchronization api threshold")]
    pub consistence_synchronization_api_threshold: Option<u64>,
    #[clap(long, env, help = "#api Consistence backfilling slots threshold")]
    pub consistence_backfilling_slots_threshold: Option<u64>,
    #[clap(long, env, help = "#api Batch mint service port")]
    pub batch_mint_service_port: Option<u16>,
    #[clap(long, env, help = "#api Storage service base url")]
    pub storage_service_base_url: Option<String>,

    #[clap(long, env, value_parser = parse_json::<JsonMiddlewareConfig>, default_value = "{\"is_enabled\":true, \"max_urls_to_parse\":10}", help = "Example: {'is_enabled':true, 'max_urls_to_parse':10} ",)]
    pub json_middleware_config: Option<JsonMiddlewareConfig>,
    #[clap(long, env, default_value = "100")]
    pub parallel_json_downloaders: i32,
    #[clap(
        long,
        env,
        default_value = "true",
        help = "Skip inline json refreshes if the metadata may be stale"
    )]
    pub api_skip_inline_json_refresh: Option<bool>,
    #[clap(long, env, default_value = "info", help = "info|debug")]
    pub log_level: String,
}

pub fn parse_json<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, String> {
    serde_json::from_str(s).map_err(|e| format!("Failed to parse JSON: {}", e))
}

pub const INGESTER_BACKUP_NAME: &str = "snapshot.tar.lz4";

#[derive(Deserialize, Default, PartialEq, Debug, Clone, Copy, ValueEnum)]
pub enum BackfillerSourceMode {
    Bigtable,
    #[default]
    RPC,
}

#[derive(Deserialize, Default, PartialEq, Debug, Clone, ValueEnum)]
pub enum JsonMigratorMode {
    #[default]
    Full,
    JsonsOnly,
    TasksOnly,
}

#[derive(Deserialize, PartialEq, Debug, Clone, Default)]
pub struct JsonMiddlewareConfig {
    pub is_enabled: bool,
    pub max_urls_to_parse: usize,
}

/// Information that we pass to the API level
#[derive(Deserialize, PartialEq, Debug, Clone, Default)]
pub struct HealthCheckInfo {
    pub app_version: String,
    pub node_name: Option<String>,
    pub image_info: Option<String>,
}

pub const DATABASE_URL_KEY: &str = "url";
pub const MAX_POSTGRES_CONNECTIONS: &str = "max_postgres_connections";

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct BigTableConfig(serde_json::Value);

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
        Ok(self.0.get(BIG_TABLE_TIMEOUT_KEY).and_then(|v| v.as_u64()).ok_or(
            IngesterError::ConfigurationError { msg: "BIG_TABLE_TIMEOUT_KEY missing".to_string() },
        )? as u32)
    }
}

pub fn init_logger(log_level: &str) {
    let t = tracing_subscriber::fmt().with_env_filter(log_level);
    t.event_format(fmt::format::json().with_line_number(true).with_file(true)).init();
}

pub fn read_version_info(file_path: &str) -> Option<String> {
    let mut file = File::open(file_path).ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;

    Some(contents)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn test_default_values() {
        let args = IngesterClapArgs::parse_from(&[
            "test",
            "--pg-database-url",
            "postgres://solana:solana@localhost:5432/aura_db",
            "--rpc-host",
            "https://mainnet-aura.metaplex.com",
            "--redis-connection-config",
            r#"{"redis_connection_str": "foo"}"#,
        ]);

        assert_eq!(args.rocks_db_path, "./my_rocksdb");
        assert_eq!(args.file_storage_path_container, "./tmp/file_storage");
        assert_eq!(args.pg_max_db_connections, 100);
        assert_eq!(args.sequence_consistent_checker_wait_period_sec, 60);
        assert_eq!(args.parallel_json_downloaders, 100);
        assert!(args.run_api.unwrap_or(false));
        assert_eq!(args.run_gapfiller, false);
        assert_eq!(args.run_profiling, false);
        assert_eq!(args.is_restore_rocks_db, false);
        assert!(args.run_bubblegum_backfiller.unwrap_or(false));
        assert_eq!(args.run_sequence_consistent_checker, false);
        assert_eq!(args.should_reingest, false);
        assert_eq!(args.check_proofs, false);
        assert_eq!(args.check_proofs_commitment, CommitmentLevel::Finalized);
        assert_eq!(args.archives_dir, "/rocksdb/_rocks_backup_archives");
        assert_eq!(args.skip_check_tree_gaps, false);
        assert!(args.run_backfiller.unwrap_or(false));
        assert_eq!(args.backfiller_source_mode, BackfillerSourceMode::RPC);
        assert_eq!(args.heap_path, "/usr/src/app/heaps");
        assert_eq!(args.log_level, "info");
        assert_eq!(args.redis_account_backfill_parsing_workers, 5);
        assert_eq!(args.account_backfill_processor_buffer_size, 1000);
    }

    #[test]
    fn test_default_values_synchronizer() {
        let args = SynchronizerClapArgs::parse_from(&[
            "test",
            "--pg-database-url",
            "postgres://solana:solana@localhost:5432/aura_db",
        ]);

        assert_eq!(args.rocks_dump_path, "./tmp/rocks_dump");
        assert_eq!(args.pg_max_db_connections, 100);
        assert_eq!(args.rocks_db_path, "./my_rocksdb");
        assert_eq!(args.rocks_db_secondary_path, "./my_rocksdb_secondary");
        assert_eq!(args.run_profiling, false);
        assert_eq!(args.heap_path, "/usr/src/app/heaps");
        assert_eq!(args.dump_synchronizer_batch_size, 10000);
        assert_eq!(args.dump_sync_threshold, 150000000);
        assert_eq!(args.synchronizer_parallel_tasks, 30);
        assert_eq!(args.timeout_between_syncs_sec, 0);
        assert_eq!(args.log_level, "info");
    }

    #[test]
    fn test_default_values_api() {
        let args = ApiClapArgs::parse_from(&[
            "test",
            "--rpc-host",
            "https://mainnet-aura.metaplex.com",
            "--pg-database-url",
            "postgres://solana:solana@localhost:5432/aura_db",
        ]);

        assert_eq!(args.rocks_db_path, "./my_rocksdb");
        assert_eq!(args.rocks_db_secondary_path, "./my_rocksdb_secondary");
        assert_eq!(args.rocks_sync_interval_seconds, 2);
        assert_eq!(args.heap_path, "/usr/src/app/heaps");
        assert_eq!(args.skip_check_tree_gaps, false);
        assert_eq!(args.run_profiling, false);
        assert_eq!(args.check_proofs, false);
        assert_eq!(args.check_proofs_probability, 0.1);
        assert_eq!(args.check_proofs_commitment, CommitmentLevel::Finalized);
        assert_eq!(args.rocks_archives_dir, "/rocksdb/_rocks_backup_archives");
        assert_eq!(args.server_port, 8990);
        assert_eq!(args.max_page_limit, 50);
        assert_eq!(args.native_mint_pubkey, "So11111111111111111111111111111111111111112");
        assert_eq!(args.parallel_json_downloaders, 100);
        assert_eq!(args.log_level, "info");
    }
}
