pub mod errors;
pub mod utils;
use chrono::Utc;
use std::fmt;
use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

pub struct IntegrityVerificationMetrics {
    pub integrity_verification_metrics: Arc<IntegrityVerificationMetricsConfig>,
    pub registry: Registry,
}

impl IntegrityVerificationMetrics {
    pub fn new(integrity_verification_metrics: IntegrityVerificationMetricsConfig) -> Self {
        Self {
            integrity_verification_metrics: Arc::new(integrity_verification_metrics),
            registry: Registry::default(),
        }
    }
}

#[derive(Debug)]
pub struct MetricState {
    pub ingester_metrics: Arc<IngesterMetricsConfig>,
    pub api_metrics: Arc<ApiMetricsConfig>,
    pub json_downloader_metrics: Arc<JsonDownloaderMetricsConfig>,
    pub backfiller_metrics: Arc<BackfillerMetricsConfig>,
    pub rpc_backfiller_metrics: Arc<RpcBackfillerMetricsConfig>,
    pub synchronizer_metrics: Arc<SynchronizerMetricsConfig>,
    pub json_migrator_metrics: Arc<JsonMigratorMetricsConfig>,
    pub registry: Registry,
}

impl MetricState {
    pub fn new(
        ingester_metrics: IngesterMetricsConfig,
        api_metrics: ApiMetricsConfig,
        json_downloader_metrics: JsonDownloaderMetricsConfig,
        backfiller_metrics: BackfillerMetricsConfig,
        rpc_backfiller_metrics: RpcBackfillerMetricsConfig,
        synchronizer_metrics: SynchronizerMetricsConfig,
        json_migrator_metrics: JsonMigratorMetricsConfig,
    ) -> Self {
        Self {
            ingester_metrics: Arc::new(ingester_metrics),
            api_metrics: Arc::new(api_metrics),
            json_downloader_metrics: Arc::new(json_downloader_metrics),
            registry: Registry::default(),
            backfiller_metrics: Arc::new(backfiller_metrics),
            rpc_backfiller_metrics: Arc::new(rpc_backfiller_metrics),
            synchronizer_metrics: Arc::new(synchronizer_metrics),
            json_migrator_metrics: Arc::new(json_migrator_metrics),
        }
    }
}

pub trait MetricsTrait {
    fn register_metrics(&mut self);
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodLabel {
    pub method_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MetricLabel {
    pub name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum MetricStatus {
    SUCCESS,
    FAILURE,
    DELETED,
}

impl fmt::Display for MetricStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MetricStatus::SUCCESS => write!(f, "success"),
            MetricStatus::FAILURE => write!(f, "failure"),
            MetricStatus::DELETED => write!(f, "deleted"),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MetricLabelWithStatus {
    pub name: String,
    pub status: MetricStatus,
}

#[derive(Debug, Clone)]
pub struct BackfillerMetricsConfig {
    slots_collected: Family<MetricLabelWithStatus, Counter>,
    data_processed: Family<MetricLabel, Counter>, // slots & transactions
    last_processed_slot: Family<MetricLabel, Gauge>,
}

impl Default for BackfillerMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl BackfillerMetricsConfig {
    pub fn new() -> Self {
        Self {
            slots_collected: Family::<MetricLabelWithStatus, Counter>::default(),
            data_processed: Family::<MetricLabel, Counter>::default(),
            last_processed_slot: Family::<MetricLabel, Gauge>::default(),
        }
    }

    pub fn inc_slots_collected(&self, label: &str, status: MetricStatus) -> u64 {
        self.slots_collected
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_owned(),
                status,
            })
            .inc()
    }

    pub fn inc_data_processed(&self, label: &str) -> u64 {
        self.data_processed
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .inc()
    }

    pub fn set_last_processed_slot(&self, label: &str, slot: i64) -> i64 {
        self.last_processed_slot
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .set(slot)
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "backfiller_slots_collected",
            "The number of slots backfiller collected and prepared to parse",
            self.slots_collected.clone(),
        );

        registry.register(
            "backfiller_data_processed",
            "The number of data processed by backfiller",
            self.data_processed.clone(),
        );

        registry.register(
            "backfiller_last_processed_slot",
            "The last processed slot by backfiller",
            self.last_processed_slot.clone(),
        );
    }
}

#[derive(Debug, Clone)]
pub struct RpcBackfillerMetricsConfig {
    fetch_signatures: Family<MetricLabelWithStatus, Counter>,
    fetch_transactions: Family<MetricLabelWithStatus, Counter>,
    transactions_processed: Family<MetricLabelWithStatus, Counter>,
    run_fetch_signatures: Family<MetricLabelWithStatus, Counter>,
}

impl Default for RpcBackfillerMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcBackfillerMetricsConfig {
    pub fn new() -> Self {
        Self {
            fetch_signatures: Family::<MetricLabelWithStatus, Counter>::default(),
            fetch_transactions: Family::<MetricLabelWithStatus, Counter>::default(),
            transactions_processed: Family::<MetricLabelWithStatus, Counter>::default(),
            run_fetch_signatures: Family::<MetricLabelWithStatus, Counter>::default(),
        }
    }

    pub fn inc_transactions_processed(&self, label: &str, status: MetricStatus) -> u64 {
        self.transactions_processed
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_string(),
                status,
            })
            .inc()
    }

    pub fn inc_fetch_transactions(&self, label: &str, status: MetricStatus) -> u64 {
        self.fetch_transactions
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_string(),
                status,
            })
            .inc()
    }

    pub fn inc_fetch_signatures(&self, label: &str, status: MetricStatus) -> u64 {
        self.fetch_signatures
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_string(),
                status,
            })
            .inc()
    }

    pub fn inc_run_fetch_signatures(&self, label: &str, status: MetricStatus) -> u64 {
        self.run_fetch_signatures
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_string(),
                status,
            })
            .inc()
    }
}

#[derive(Debug, Clone)]
pub struct SynchronizerMetricsConfig {
    number_of_records_synchronized: Family<MetricLabel, Counter>,
    last_synchronized_slot: Family<MetricLabel, Gauge>,
}

impl Default for SynchronizerMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SynchronizerMetricsConfig {
    pub fn new() -> Self {
        Self {
            number_of_records_synchronized: Family::<MetricLabel, Counter>::default(),
            last_synchronized_slot: Family::<MetricLabel, Gauge>::default(),
        }
    }

    pub fn inc_number_of_records_synchronized(&self, label: &str, num_of_records: u64) -> u64 {
        self.number_of_records_synchronized
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .inc_by(num_of_records)
    }

    pub fn set_last_synchronized_slot(&self, label: &str, slot: i64) -> i64 {
        self.last_synchronized_slot
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .set(slot)
    }
}

#[derive(Debug, Clone)]
pub struct ApiMetricsConfig {
    requests: Family<MethodLabel, Counter>,
    search_asset_requests: Family<MethodLabel, Counter>,
    start_time: Gauge,
    latency: Family<MethodLabel, Histogram>,
}

impl ApiMetricsConfig {
    pub fn new() -> Self {
        Self {
            requests: Family::<MethodLabel, Counter>::default(),
            search_asset_requests: Family::<MethodLabel, Counter>::default(),
            start_time: Default::default(),
            latency: Family::<MethodLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(20.0, 1.8, 10))
            }),
        }
    }

    pub fn inc_requests(&self, label: &str) -> u64 {
        self.requests
            .get_or_create(&MethodLabel {
                method_name: label.to_owned(),
            })
            .inc()
    }

    pub fn inc_search_asset_requests(&self, label: &str) -> u64 {
        self.search_asset_requests
            .get_or_create(&MethodLabel {
                method_name: label.to_owned(),
            })
            .inc()
    }

    pub fn start_time(&self) -> i64 {
        self.start_time.set(Utc::now().timestamp())
    }

    pub fn set_latency(&self, label: &str, duration: f64) {
        self.latency
            .get_or_create(&MethodLabel {
                method_name: label.to_owned(),
            })
            .observe(duration);
    }
}

impl Default for ApiMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTrait for MetricState {
    fn register_metrics(&mut self) {
        self.api_metrics.start_time();
        self.json_downloader_metrics.start_time();

        self.registry.register(
            "api_http_requests",
            "The number of HTTP requests made",
            self.api_metrics.requests.clone(),
        );
        self.registry.register(
            "api_http_search_asset_requests",
            "The number of searchAsset requests made",
            self.api_metrics.search_asset_requests.clone(),
        );
        self.registry.register(
            "api_call_latency",
            "A histogram of the request duration",
            self.api_metrics.latency.clone(),
        );
        self.registry.register(
            "api_start_time",
            "Binary start time",
            self.api_metrics.start_time.clone(),
        );

        self.ingester_metrics.register(&mut self.registry);

        self.registry.register(
            "json_downloader_latency_task",
            "A histogram of task execution time",
            self.json_downloader_metrics.latency_task_executed.clone(),
        );

        self.registry.register(
            "json_downloader_tasks_count",
            "The total number of tasks made",
            self.json_downloader_metrics.tasks.clone(),
        );

        self.registry.register(
            "json_downloader_tasks_to_execute",
            "The number of tasks that need to be executed",
            self.json_downloader_metrics.tasks_to_execute.clone(),
        );

        self.registry.register(
            "json_downloader_start_time",
            "Binary start time",
            self.json_downloader_metrics.start_time.clone(),
        );

        self.backfiller_metrics.register(&mut self.registry);

        self.registry.register(
            "rpc_backfiller_transactions_processed",
            "Count of transactions, processed by RPC backfiller",
            self.rpc_backfiller_metrics.transactions_processed.clone(),
        );

        self.registry.register(
            "rpc_backfiller_fetch_signatures",
            "Count of RPC fetch_signatures calls",
            self.rpc_backfiller_metrics.fetch_signatures.clone(),
        );

        self.registry.register(
            "rpc_backfiller_fetch_transactions",
            "Count of RPC fetch_transactions calls",
            self.rpc_backfiller_metrics.fetch_transactions.clone(),
        );

        self.registry.register(
            "rpc_backfiller_run_fetch_signatures",
            "Count of fetch_signatures restarts",
            self.rpc_backfiller_metrics.run_fetch_signatures.clone(),
        );

        self.registry.register(
            "synchronizer_number_of_records_synchronized",
            "Count of records, synchronized by synchronizer",
            self.synchronizer_metrics
                .number_of_records_synchronized
                .clone(),
        );

        self.registry.register(
            "synchronizer_last_synchronized_slot",
            "The last synchronized slot by synchronizer",
            self.synchronizer_metrics.last_synchronized_slot.clone(),
        );

        self.json_migrator_metrics.register(&mut self.registry);
    }
}

impl MetricsTrait for IntegrityVerificationMetrics {
    fn register_metrics(&mut self) {
        self.integrity_verification_metrics.start_time();

        self.registry.register(
            "tests_start_time",
            "Binary start time",
            self.integrity_verification_metrics.start_time.clone(),
        );

        self.registry.register(
            "total_get_asset_tested",
            "Count of total getAsset method`s tests",
            self.integrity_verification_metrics
                .total_get_asset_tested
                .clone(),
        );
        self.registry.register(
            "total_get_asset_proof_tested",
            "Count of total getAssetProof method`s tests",
            self.integrity_verification_metrics
                .total_get_asset_proof_tested
                .clone(),
        );
        self.registry.register(
            "total_get_assets_by_authority_tested",
            "Count of total getAssetsByAuthority method`s tests",
            self.integrity_verification_metrics
                .total_get_assets_by_authority_tested
                .clone(),
        );
        self.registry.register(
            "total_get_assets_by_creator_tested",
            "Count of total getAssetsByCreator method`s tests",
            self.integrity_verification_metrics
                .total_get_assets_by_creator_tested
                .clone(),
        );
        self.registry.register(
            "total_get_assets_by_owner_tested",
            "Count of total getAssetsByOwner method`s tests",
            self.integrity_verification_metrics
                .total_get_assets_by_owner_tested
                .clone(),
        );
        self.registry.register(
            "total_get_assets_by_group_tested",
            "Count of total getAssetsByGroup method`s tests",
            self.integrity_verification_metrics
                .total_get_assets_by_group_tested
                .clone(),
        );

        self.registry.register(
            "failed_get_asset_tested",
            "Fail count of getAsset method`s tests",
            self.integrity_verification_metrics
                .failed_get_asset_tested
                .clone(),
        );
        self.registry.register(
            "failed_get_asset_proof_tested",
            "Fail count of getAssetProof method`s tests",
            self.integrity_verification_metrics
                .failed_get_asset_proof_tested
                .clone(),
        );
        self.registry.register(
            "failed_get_assets_by_authority_tested",
            "Fail count of getAssetsByAuthority method`s tests",
            self.integrity_verification_metrics
                .failed_get_assets_by_authority_tested
                .clone(),
        );
        self.registry.register(
            "failed_get_assets_by_creator_tested",
            "Fail count of getAssetsByCreator method`s tests",
            self.integrity_verification_metrics
                .failed_get_assets_by_creator_tested
                .clone(),
        );
        self.registry.register(
            "failed_get_assets_by_owner_tested",
            "Fail count of getAssetsByOwner method`s tests",
            self.integrity_verification_metrics
                .failed_get_assets_by_owner_tested
                .clone(),
        );
        self.registry.register(
            "failed_get_assets_by_group_tested",
            "Fail count of getAssetsByGroup method`s tests",
            self.integrity_verification_metrics
                .failed_get_assets_by_group_tested
                .clone(),
        );

        self.registry.register(
            "network_errors_testing_host",
            "Count of network errors from testing host",
            self.integrity_verification_metrics
                .network_errors_testing_host
                .clone(),
        );
        self.registry.register(
            "network_errors_reference_host",
            "Count of network errors from reference host",
            self.integrity_verification_metrics
                .network_errors_reference_host
                .clone(),
        );

        self.registry.register(
            "fetch_keys_errors",
            "Count of DB errors while fetching keys for tests",
            self.integrity_verification_metrics
                .fetch_keys_errors
                .clone(),
        );
    }
}

#[derive(Debug, Clone)]
pub struct JsonMigratorMetricsConfig {
    start_time: Gauge,
    tasks_buffer: Family<MetricLabel, Gauge>,
    jsons_migrated: Family<MetricLabelWithStatus, Counter>,
    tasks_set: Family<MetricLabelWithStatus, Counter>,
}

impl JsonMigratorMetricsConfig {
    pub fn new() -> Self {
        Self {
            start_time: Default::default(),
            tasks_buffer: Family::<MetricLabel, Gauge>::default(),
            jsons_migrated: Family::<MetricLabelWithStatus, Counter>::default(),
            tasks_set: Family::<MetricLabelWithStatus, Counter>::default(),
        }
    }

    pub fn start_time(&self) -> i64 {
        self.start_time.set(Utc::now().timestamp())
    }

    pub fn set_tasks_buffer(&self, label: &str, buffer_size: i64) {
        self.tasks_buffer
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .set(buffer_size);
    }

    pub fn inc_jsons_migrated(&self, label: &str, status: MetricStatus) -> u64 {
        self.jsons_migrated
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_owned(),
                status,
            })
            .inc()
    }

    pub fn inc_tasks_set(&self, label: &str, status: MetricStatus, inc_by: u64) -> u64 {
        self.tasks_set
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_owned(),
                status,
            })
            .inc_by(inc_by)
    }

    pub fn register(&self, registry: &mut Registry) {
        self.start_time();

        registry.register(
            "json_migrator_start_time",
            "Binary start time",
            self.start_time.clone(),
        );

        registry.register(
            "json_migrator_tasks_buffer",
            "Buffer size",
            self.tasks_buffer.clone(),
        );

        registry.register(
            "json_migrator_jsons_migrated",
            "Total amount of migrated jsons",
            self.jsons_migrated.clone(),
        );

        registry.register(
            "json_migrator_tasks_set",
            "Total amount of tasks set",
            self.tasks_set.clone(),
        );
    }
}

#[derive(Debug, Clone)]
pub struct IngesterMetricsConfig {
    start_time: Gauge,
    latency: Family<MetricLabel, Histogram>,
    parsers: Family<MetricLabelWithStatus, Counter>,
    process: Family<MetricLabelWithStatus, Counter>,
    buffers: Family<MetricLabel, Gauge>,
    retries: Family<MetricLabel, Counter>,
    rocksdb_backup_latency: Histogram,
    instructions: Family<MetricLabel, Counter>,
    last_processed_slot: Family<MetricLabel, Gauge>,
}

impl IngesterMetricsConfig {
    pub fn new() -> Self {
        Self {
            start_time: Default::default(),
            latency: Family::<MetricLabel, Histogram>::new_with_constructor(|| {
                Histogram::new([1.0, 10.0, 50.0, 100.0].into_iter())
            }),
            parsers: Family::<MetricLabelWithStatus, Counter>::default(),
            process: Family::<MetricLabelWithStatus, Counter>::default(),
            buffers: Family::<MetricLabel, Gauge>::default(),
            retries: Family::<MetricLabel, Counter>::default(),
            rocksdb_backup_latency: Histogram::new(
                [
                    60.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0, 5400.0, 7200.0, 9000.0, 10800.0,
                ]
                .into_iter(),
            ),
            instructions: Family::<MetricLabel, Counter>::default(),
            last_processed_slot: Family::<MetricLabel, Gauge>::default(),
        }
    }

    pub fn start_time(&self) -> i64 {
        self.start_time.set(Utc::now().timestamp())
    }

    pub fn set_latency(&self, label: &str, duration: f64) {
        self.latency
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .observe(duration);
    }

    pub fn set_rocksdb_backup_latency(&self, duration: f64) {
        self.rocksdb_backup_latency.observe(duration);
    }
    pub fn set_buffer(&self, label: &str, buffer_size: i64) {
        self.buffers
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .set(buffer_size);
    }

    pub fn inc_parser(&self, label: &str, status: MetricStatus) -> u64 {
        self.parsers
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_owned(),
                status,
            })
            .inc()
    }

    pub fn inc_process(&self, label: &str, status: MetricStatus) -> u64 {
        self.process
            .get_or_create(&MetricLabelWithStatus {
                name: label.to_owned(),
                status,
            })
            .inc()
    }

    pub fn inc_query_db_retries(&self, label: &str) -> u64 {
        self.retries
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .inc()
    }

    pub fn inc_instructions(&self, label: &str) -> u64 {
        self.instructions
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .inc()
    }

    pub fn set_last_processed_slot(&self, label: &str, slot: i64) -> i64 {
        self.last_processed_slot
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .set(slot)
    }

    pub fn register(&self, registry: &mut Registry) {
        self.start_time();
        registry.register(
            "ingester_start_time",
            "Binary start time",
            self.start_time.clone(),
        );

        registry.register(
            "ingester_parsed_data",
            "Total amount of parsed data",
            self.parsers.clone(),
        );
        registry.register(
            "ingester_processed",
            "Total amount of processed data",
            self.process.clone(),
        );
        registry.register(
            "ingester_parsed_data_latency",
            "Histogram of data parsing duration",
            self.latency.clone(),
        );
        registry.register("ingester_buffers", "Buffer size", self.buffers.clone());
        registry.register(
            "ingester_query_retries",
            "Total amount of query retries data",
            self.retries.clone(),
        );
        registry.register(
            "ingester_bublegum_instructions",
            "Total number of bubblegum instructions processed",
            self.instructions.clone(),
        );
        registry.register(
            "ingester_rocksdb_backup_latency",
            "Histogram of rocksdb backup duration",
            self.rocksdb_backup_latency.clone(),
        );
        registry.register(
            "ingester_last_processed_slot",
            "The last processed slot by ingester",
            self.last_processed_slot.clone(),
        );
    }
}

impl Default for IngesterMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct JsonDownloaderMetricsConfig {
    start_time: Gauge,
    latency_task_executed: Family<MetricLabel, Histogram>,
    tasks: Family<MetricLabel, Counter>,
    tasks_to_execute: Gauge,
}

impl JsonDownloaderMetricsConfig {
    pub fn new() -> Self {
        Self {
            tasks: Family::<MetricLabel, Counter>::default(),
            start_time: Default::default(),
            tasks_to_execute: Default::default(),
            latency_task_executed: Family::<MetricLabel, Histogram>::new_with_constructor(|| {
                Histogram::new([100.0, 500.0, 1000.0, 2000.0].into_iter())
            }),
        }
    }

    pub fn inc_tasks(&self, label: MetricStatus) -> u64 {
        self.tasks
            .get_or_create(&MetricLabel {
                name: label.to_string(),
            })
            .inc()
    }

    pub fn set_tasks_to_execute(&self, count: i64) -> i64 {
        self.tasks_to_execute.set(count)
    }

    pub fn start_time(&self) -> i64 {
        self.start_time.set(Utc::now().timestamp())
    }

    pub fn set_latency_task_executed(&self, label: &str, duration: f64) {
        self.latency_task_executed
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .observe(duration);
    }
}

impl Default for JsonDownloaderMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct IntegrityVerificationMetricsConfig {
    start_time: Gauge,

    total_get_asset_tested: Counter,
    total_get_asset_proof_tested: Counter,
    total_get_assets_by_owner_tested: Counter,
    total_get_assets_by_creator_tested: Counter,
    total_get_assets_by_authority_tested: Counter,
    total_get_assets_by_group_tested: Counter,

    failed_get_asset_tested: Counter,
    failed_get_asset_proof_tested: Counter,
    failed_get_assets_by_owner_tested: Counter,
    failed_get_assets_by_creator_tested: Counter,
    failed_get_assets_by_authority_tested: Counter,
    failed_get_assets_by_group_tested: Counter,

    network_errors_testing_host: Counter,
    network_errors_reference_host: Counter,

    fetch_keys_errors: Family<MetricLabel, Counter>,
}

impl Default for IntegrityVerificationMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl IntegrityVerificationMetricsConfig {
    pub fn new() -> Self {
        Self {
            start_time: Default::default(),
            total_get_asset_tested: Default::default(),
            total_get_asset_proof_tested: Default::default(),
            total_get_assets_by_owner_tested: Default::default(),
            total_get_assets_by_creator_tested: Default::default(),
            total_get_assets_by_authority_tested: Default::default(),
            total_get_assets_by_group_tested: Default::default(),
            failed_get_asset_tested: Default::default(),
            failed_get_asset_proof_tested: Default::default(),
            failed_get_assets_by_owner_tested: Default::default(),
            failed_get_assets_by_creator_tested: Default::default(),
            failed_get_assets_by_authority_tested: Default::default(),
            failed_get_assets_by_group_tested: Default::default(),
            network_errors_testing_host: Default::default(),
            network_errors_reference_host: Default::default(),
            fetch_keys_errors: Default::default(),
        }
    }

    pub fn start_time(&self) -> i64 {
        self.start_time.set(Utc::now().timestamp())
    }

    pub fn inc_total_get_asset_tested(&self) -> u64 {
        self.total_get_asset_tested.inc()
    }
    pub fn inc_total_get_asset_proof_tested(&self) -> u64 {
        self.total_get_asset_proof_tested.inc()
    }
    pub fn inc_total_get_assets_by_owner_tested(&self) -> u64 {
        self.total_get_assets_by_owner_tested.inc()
    }
    pub fn inc_total_get_assets_by_creator_tested(&self) -> u64 {
        self.total_get_assets_by_creator_tested.inc()
    }
    pub fn inc_total_get_assets_by_authority_tested(&self) -> u64 {
        self.total_get_assets_by_authority_tested.inc()
    }
    pub fn inc_total_get_assets_by_group_tested(&self) -> u64 {
        self.total_get_assets_by_group_tested.inc()
    }

    pub fn inc_failed_get_asset_tested(&self) -> u64 {
        self.failed_get_asset_tested.inc()
    }
    pub fn inc_failed_get_asset_proof_tested(&self) -> u64 {
        self.failed_get_asset_proof_tested.inc()
    }
    pub fn inc_failed_get_assets_by_owner_tested(&self) -> u64 {
        self.failed_get_assets_by_owner_tested.inc()
    }
    pub fn inc_failed_get_assets_by_creator_tested(&self) -> u64 {
        self.failed_get_assets_by_creator_tested.inc()
    }
    pub fn inc_failed_get_assets_by_authority_tested(&self) -> u64 {
        self.failed_get_assets_by_authority_tested.inc()
    }
    pub fn inc_failed_failed_get_assets_by_group_tested(&self) -> u64 {
        self.failed_get_assets_by_group_tested.inc()
    }

    pub fn inc_network_errors_testing_host(&self) -> u64 {
        self.network_errors_testing_host.inc()
    }
    pub fn inc_network_errors_reference_host(&self) -> u64 {
        self.network_errors_reference_host.inc()
    }

    pub fn inc_fetch_keys_errors(&self, label: &str) -> u64 {
        self.fetch_keys_errors
            .get_or_create(&MetricLabel {
                name: label.to_owned(),
            })
            .inc()
    }
}
