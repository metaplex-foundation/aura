use metrics_utils::{IngesterMetricsConfig, MetricStatus};
use std::fmt::Display;
use std::sync::Arc;
use tracing::log::error;

pub fn result_to_metrics<T, E: Display>(
    metrics: Arc<IngesterMetricsConfig>,
    result: &Result<T, E>,
    metric_name: &str,
) {
    match result {
        Err(e) => {
            metrics.inc_process(metric_name, MetricStatus::FAILURE);
            error!("Error {}: {}", metric_name, e);
        }
        Ok(_) => {
            metrics.inc_process(metric_name, MetricStatus::SUCCESS);
        }
    }
}
