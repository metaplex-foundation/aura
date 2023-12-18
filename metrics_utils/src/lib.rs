pub mod errors;
pub mod utils;
use chrono::Utc;
use std::fmt;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

#[derive(Debug)]
pub struct MetricState<T> {
    pub metrics: T,
    pub registry: Registry,
}

impl<T> MetricState<T> {
    pub fn new(metrics: T) -> Self {
        Self {
            metrics,
            registry: Registry::default(),
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
pub struct ParserLabel {
    pub name: String,
    pub status: MetricStatus,
}

#[derive(Debug, Clone)]
pub struct ApiMetricsConfig {
    requests: Family<MethodLabel, Counter>,
    start_time: Gauge,
    latency: Family<MethodLabel, Histogram>,
}

impl ApiMetricsConfig {
    pub fn new() -> Self {
        Self {
            requests: Family::<MethodLabel, Counter>::default(),
            start_time: Default::default(),
            latency: Family::<MethodLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 2.0, 10))
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

impl MetricsTrait for MetricState<ApiMetricsConfig> {
    fn register_metrics(&mut self) {
        self.registry.register(
            "api_http_requests",
            "The number of HTTP requests made",
            self.metrics.requests.clone(),
        );
        self.registry.register(
            "api_call_latency",
            "A histogram of the request duration",
            self.metrics.latency.clone(),
        );
        self.registry.register(
            "api_start_time",
            "Binary start time",
            self.metrics.start_time.clone(),
        );

        self.metrics.start_time();
    }
}

#[derive(Debug, Clone)]
pub struct IngesterMetricsConfig {
    start_time: Gauge,
    latency: Family<MetricLabel, Histogram>,
    parsers: Family<ParserLabel, Counter>,
    process: Family<ParserLabel, Counter>,
    buffers: Family<MetricLabel, Gauge>,
    retries: Family<MetricLabel, Counter>,
    rocksdb_backup_latency: Histogram,
    instructions: Family<MetricLabel, Counter>,
}

impl IngesterMetricsConfig {
    pub fn new() -> Self {
        Self {
            start_time: Default::default(),
            latency: Family::<MetricLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 1.0, 10))
            }),
            parsers: Family::<ParserLabel, Counter>::default(),
            process: Family::<ParserLabel, Counter>::default(),
            buffers: Family::<MetricLabel, Gauge>::default(),
            retries: Family::<MetricLabel, Counter>::default(),
            rocksdb_backup_latency: Histogram::new(
                [
                    60.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0, 5400.0, 7200.0, 9000.0, 10800.0,
                ]
                .into_iter(),
            ),
            instructions: Family::<MetricLabel, Counter>::default(),
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
            .get_or_create(&ParserLabel {
                name: label.to_owned(),
                status,
            })
            .inc()
    }

    pub fn inc_process(&self, label: &str, status: MetricStatus) -> u64 {
        self.process
            .get_or_create(&ParserLabel {
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
}

impl Default for IngesterMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTrait for MetricState<IngesterMetricsConfig> {
    fn register_metrics(&mut self) {
        self.registry.register(
            "ingester_start_time",
            "Binary start time",
            self.metrics.start_time.clone(),
        );
        self.metrics.start_time();

        self.registry.register(
            "ingester_parsed_data",
            "Total amount of parsed data",
            self.metrics.parsers.clone(),
        );
        self.registry.register(
            "ingester_processed",
            "Total amount of processed data",
            self.metrics.process.clone(),
        );
        self.registry.register(
            "ingester_parsed_data_latency",
            "Histogram of data parsing duration",
            self.metrics.latency.clone(),
        );
        self.registry.register(
            "ingester_buffers",
            "Buffer size",
            self.metrics.buffers.clone(),
        );
        self.registry.register(
            "ingester_query_retries",
            "Total amount of query retries data",
            self.metrics.retries.clone(),
        );
        self.registry.register(
            "ingester_bublegum_instructions",
            "Total number of bubblegum instructions processed",
            self.metrics.instructions.clone(),
        );
        self.registry.register(
            "ingester_rocksdb_backup_latency",
            "Histogram of rocksdb backup duration",
            self.metrics.rocksdb_backup_latency.clone(),
        );
    }
}

#[derive(Debug, Clone)]
pub struct BgTaskRunnerMetricsConfig {
    start_time: Gauge,
    latency_task_executed: Family<MetricLabel, Histogram>,
    tasks: Family<MetricLabel, Counter>,
    tasks_to_execute: Gauge,
}

impl BgTaskRunnerMetricsConfig {
    pub fn new() -> Self {
        Self {
            tasks: Family::<MetricLabel, Counter>::default(),
            start_time: Default::default(),
            tasks_to_execute: Default::default(),
            latency_task_executed: Family::<MetricLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 2.0, 10))
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

impl Default for BgTaskRunnerMetricsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTrait for MetricState<BgTaskRunnerMetricsConfig> {
    fn register_metrics(&mut self) {
        self.registry.register(
            "bg_task_runner_latency_task",
            "A histogram of task execution time",
            self.metrics.latency_task_executed.clone(),
        );

        self.registry.register(
            "bg_task_runner_tasks_count",
            "The total number of tasks made",
            self.metrics.tasks.clone(),
        );

        self.registry.register(
            "bg_task_runner_tasks_to_execute",
            "The number of tasks that need to be executed",
            self.metrics.tasks_to_execute.clone(),
        );

        self.registry.register(
            "bg_task_runner_start_time",
            "Binary start time",
            self.metrics.start_time.clone(),
        );

        self.metrics.start_time();
    }
}
