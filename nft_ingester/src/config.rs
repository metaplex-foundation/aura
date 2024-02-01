use core::time;
use std::{
    env,
    net::{SocketAddr, ToSocketAddrs},
};

use figment::{providers::Env, Figment};
use interface::asset_streaming_and_discovery::PeerDiscovery;
use serde::Deserialize;
use tracing_subscriber::fmt;

use crate::error::IngesterError;

const INGESTER_CONSUMERS_COUNT: usize = 2;
pub const INGESTER_BACKUP_NAME: &str = "snapshot.tar.lz4";

pub const INGESTER_CONFIG_PREFIX: &str = "INGESTER_";
pub const JSON_MIGRATOR_CONFIG_PREFIX: &str = "JSON_MIGRATOR_";

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct BackgroundTaskRunnerConfig {
    pub delete_interval: Option<u64>,
    pub retry_interval: Option<u64>,
    pub purge_time: Option<u64>,
    pub batch_size: Option<u64>,
    pub lock_duration: Option<i64>,
    pub max_attempts: Option<i16>,
    pub timeout: Option<u64>,
}

impl Default for BackgroundTaskRunnerConfig {
    fn default() -> Self {
        BackgroundTaskRunnerConfig {
            delete_interval: Some(5),
            retry_interval: Some(5),
            purge_time: Some(5),
            batch_size: Some(5),
            lock_duration: Some(5),
            max_attempts: Some(5),
            timeout: Some(3),
        }
    }
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct BackgroundTaskConfig {
    pub background_task_runner_config: Option<BackgroundTaskRunnerConfig>,
    pub database_config: DatabaseConfig,
    pub bg_task_runner_metrics_port: Option<u16>,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct BackfillerConfig {
    pub big_table_config: BigTableConfig,
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
}
fn default_wait_period_sec() -> u64 {
    60
}

fn default_workers_count() -> usize {
    100
}

fn default_chunk_size() -> usize {
    5
}

fn default_permitted_tasks() -> usize {
    500
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

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct RawBackfillConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub metrics_port: Option<u16>,
    pub rocks_db_path_container: Option<String>,
    #[serde(default)]
    pub run_profiling: bool,
    pub profiling_file_path_container: Option<String>,
}
#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct IngesterConfig {
    pub database_config: DatabaseConfig,
    pub old_database_config: Option<DatabaseConfig>,
    pub tcp_config: TcpConfig,
    pub tx_background_savers: u32,
    pub backfill_background_savers: u32,
    pub mplx_buffer_size: usize,
    pub mplx_workers: u32,
    pub spl_buffer_size: usize,
    pub spl_workers: u32,
    pub env: Option<String>,
    pub metrics_port_first_consumer: Option<u16>,
    pub metrics_port_second_consumer: Option<u16>,
    pub backfill_consumer_metrics_port: Option<u16>,
    pub background_task_runner_config: Option<BackgroundTaskRunnerConfig>,
    pub is_snapshot: Option<bool>,
    pub consumer_number: Option<usize>,
    pub migration_batch_size: Option<u32>,
    pub migrator_workers: Option<u32>,
    pub rocks_db_path_container: Option<String>,
    pub rocks_backup_url: String,
    pub rocks_backup_archives_dir: String,
    pub rocks_backup_dir: String,
    pub run_bubblegum_backfiller: bool,
    pub synchronizer_batch_size: usize,
    pub gapfiller_peer_addr: String,
    pub peer_grpc_port: u16,
    pub peer_grpc_max_gap_slots: u64,
    pub rust_log: Option<String>,
    pub backfill_rpc_address: String,
    pub run_profiling: Option<bool>,
    pub profiling_file_path_container: Option<String>,
    pub store_db_backups: Option<bool>,
    #[serde(default)]
    pub run_sequence_consistent_checker: bool,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct JsonMigratorConfig {
    pub rust_log: Option<String>,
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
        self.rust_log.clone().unwrap_or("warn".to_string())
    }
}

fn default_log_level() -> String {
    "warn".to_string()
}

impl IngesterConfig {
    pub fn get_metrics_port(
        &self,
        consumer_number: Option<usize>,
    ) -> Result<Option<u16>, IngesterError> {
        let consumer_number = match consumer_number {
            Some(consumer_number) => consumer_number,
            None => {
                return Err(IngesterError::ConfigurationError {
                    msg: "missing consumer number".to_string(),
                });
            }
        };

        match consumer_number {
            0 => Ok(self.metrics_port_first_consumer),
            1 => Ok(self.metrics_port_second_consumer),
            _ => Err(IngesterError::ConfigurationError {
                msg: format!(
                    "invalid consumer number: {}, expected to be lower than: {}",
                    consumer_number, INGESTER_CONSUMERS_COUNT
                ),
            }),
        }
    }

    pub fn get_is_snapshot(&self) -> bool {
        self.is_snapshot.unwrap_or_default()
    }

    pub fn get_log_level(&self) -> String {
        self.rust_log.clone().unwrap_or("warn".to_string())
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
pub struct MetricsConfig(figment::value::Dict);

pub const METRICS_HOST_KEY: &str = "metrics_host";
pub const METRICS_PORT_KEY: &str = "metrics_port";

impl MetricsConfig {
    pub fn get_metrics_port(&self) -> Result<u16, IngesterError> {
        self.0
            .get(METRICS_PORT_KEY)
            .and_then(|a| a.to_u128().map(|res| res as u16))
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("get_metrics_port missing: {}", METRICS_PORT_KEY),
            })
    }

    pub fn get_metrics_host(&self) -> Result<String, IngesterError> {
        self.0
            .get(METRICS_HOST_KEY)
            .and_then(|a| a.clone().into_string())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("get_metrics_host missing: {}", METRICS_HOST_KEY),
            })
    }
}

pub const CODE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub struct TcpConfig(figment::value::Dict);

pub const TCP_RECEIVER_ADDR: &str = "receiver_addr";
pub const TCP_RECEIVER_RECONNECT_INTERVAL: &str = "receiver_reconnect_interval";

pub const TCP_RECEIVER_BACKFILLER_ADDR: &str = "backfiller_receiver_addr";
pub const BACKFILLER_RECONNECT_INTERVAL: &str = "backfiller_receiver_reconnect_interval";
pub const TCP_SENDER_BACKFILLER_PORT: &str = "backfiller_sender_port";
pub const TCP_SENDER_BACKFILLER_BATCH_MAX_BYTES: &str = "backfiller_sender_batch_max_bytes";
pub const TCP_SENDER_BACKFILLER_BUFFER_SIZE: &str = "backfiller_sender_buffer_size";

pub const TCP_SNAPSHOT_RECEIVER_ADDR: &str = "snapshot_receiver_addr";

impl TcpConfig {
    pub fn get_tcp_receiver_reconnect_interval(&self) -> Result<time::Duration, IngesterError> {
        self.get_duration(TCP_RECEIVER_RECONNECT_INTERVAL)
    }

    pub fn get_backfiller_reconnect_interval(&self) -> Result<time::Duration, IngesterError> {
        self.get_duration(BACKFILLER_RECONNECT_INTERVAL)
    }

    pub fn get_tcp_receiver_addr_backfiller(&self) -> Result<SocketAddr, IngesterError> {
        self.get_addr(TCP_RECEIVER_BACKFILLER_ADDR)
    }

    pub fn get_tcp_receiver_addr_ingester(
        &self,
        consumer_number: Option<usize>,
    ) -> Result<SocketAddr, IngesterError> {
        let tcp_senders = self.get_addrs(TCP_RECEIVER_ADDR)?;
        if tcp_senders.len() != INGESTER_CONSUMERS_COUNT {
            return Err(IngesterError::ConfigurationError {
                msg: format!(
                    "invalid len of tcp_senders: {}, expected: {}",
                    tcp_senders.len(),
                    INGESTER_CONSUMERS_COUNT
                ),
            });
        };
        let consumer_number = match consumer_number {
            Some(consumer_number) => consumer_number,
            None => {
                return Err(IngesterError::ConfigurationError {
                    msg: "missing consumer number".to_string(),
                });
            }
        };
        if consumer_number >= INGESTER_CONSUMERS_COUNT {
            return Err(IngesterError::ConfigurationError {
                msg: format!(
                    "invalid consumer number: {}, expected to be lower than: {}",
                    consumer_number, INGESTER_CONSUMERS_COUNT
                ),
            });
        };

        Ok(tcp_senders[consumer_number])
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

    fn get_addrs(&self, key: &str) -> Result<Vec<SocketAddr>, IngesterError> {
        self.0
            .get(key)
            .and_then(|a| a.as_array())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!("TCP receiver address missing: {}", key),
            })?
            .iter()
            .map(|a| {
                a.as_str()
                    .ok_or(IngesterError::ConfigurationError {
                        msg: format!("TCP receiver address missing: {}", key),
                    })?
                    .to_socket_addrs()?
                    .next()
                    .ok_or(IngesterError::ConfigurationError {
                        msg: "SocketAddr".to_string(),
                    })
            })
            .collect()
    }

    pub fn get_tcp_sender_backfiller_port(&self) -> Result<u16, IngesterError> {
        Ok(self
            .0
            .get(TCP_SENDER_BACKFILLER_PORT)
            .and_then(|a| a.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "TCP sender port for backfiller missing: {}",
                    TCP_SENDER_BACKFILLER_PORT
                ),
            })? as u16)
    }

    pub fn get_tcp_sender_backfiller_buffer_size(&self) -> Result<usize, IngesterError> {
        Ok(self
            .0
            .get(TCP_SENDER_BACKFILLER_BUFFER_SIZE)
            .and_then(|a| a.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "TCP sender buffer size for backfiller missing: {}",
                    TCP_SENDER_BACKFILLER_BUFFER_SIZE
                ),
            })? as usize)
    }

    pub fn get_tcp_sender_backfiller_batch_max_bytes(&self) -> Result<usize, IngesterError> {
        Ok(self
            .0
            .get(TCP_SENDER_BACKFILLER_BATCH_MAX_BYTES)
            .and_then(|a| a.to_u128())
            .ok_or(IngesterError::ConfigurationError {
                msg: format!(
                    "TCP sender batch max bytes for backfiller missing: {}",
                    TCP_SENDER_BACKFILLER_BATCH_MAX_BYTES
                ),
            })? as usize)
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
        let config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);
        assert_eq!(
            config,
            BackfillerConfig {
                big_table_config: BigTableConfig(figment::value::Dict::new()),
                slot_until: None,
                slot_start_from: 0,
                backfiller_mode: BackfillerMode::IngestDirectly,
                workers_count: 100,
                chunk_size: 5,
                permitted_tasks: 500,
                wait_period_sec: 60,
            }
        );
        std::env::remove_var("INGESTER_DATABASE_CONFIG");
        std::env::remove_var("INGESTER_TCP_CONFIG");
        std::env::remove_var("INGESTER_BIG_TABLE_CONFIG");
        std::env::remove_var("INGESTER_SLOT_START_FROM");
    }

    #[test]
    fn test_setup_backfiller_config_backfill_mode() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var("INGESTER_DATABASE_CONFIG", "{}");
        std::env::set_var("INGESTER_TCP_CONFIG", "{}");
        std::env::set_var("INGESTER_BIG_TABLE_CONFIG", "{}");
        std::env::set_var("INGESTER_SLOT_START_FROM", "0");
        std::env::set_var("INGESTER_BACKFILLER_MODE", "Persist");
        let config: BackfillerConfig = setup_config(INGESTER_CONFIG_PREFIX);
        assert_eq!(
            config,
            BackfillerConfig {
                big_table_config: BigTableConfig(figment::value::Dict::new()),
                slot_until: None,
                slot_start_from: 0,
                backfiller_mode: BackfillerMode::Persist,
                workers_count: 100,
                chunk_size: 5,
                permitted_tasks: 500,
                wait_period_sec: 60,
            }
        );
        std::env::remove_var("INGESTER_DATABASE_CONFIG");
        std::env::remove_var("INGESTER_TCP_CONFIG");
        std::env::remove_var("INGESTER_BIG_TABLE_CONFIG");
        std::env::remove_var("INGESTER_SLOT_START_FROM");
        std::env::remove_var("INGESTER_BACKFILLER_MODE");
    }
}
