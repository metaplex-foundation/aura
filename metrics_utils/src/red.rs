use chrono::{DateTime, Utc};
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ExtendedLabel {
    pub component: String,
    pub action: String,
    pub endpoint: String,
}

#[derive(Debug, Clone)]
pub struct RequestErrorDurationMetrics {
    requests: Family<ExtendedLabel, Counter>,
    errors: Family<ExtendedLabel, Counter>,
    durations: Family<ExtendedLabel, Histogram>,
}

impl Default for RequestErrorDurationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestErrorDurationMetrics {
    pub fn new() -> Self {
        Self {
            requests: Family::<ExtendedLabel, Counter>::default(),
            errors: Family::<ExtendedLabel, Counter>::default(),
            durations: Family::<ExtendedLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(1.0, 2.0, 13))
            }),
        }
    }

    pub fn observe_request(
        &self,
        component: &str,
        action: &str,
        endpoint: &str,
        start_time: DateTime<Utc>,
    ) {
        let duration = chrono::Utc::now().signed_duration_since(start_time);
        let label = ExtendedLabel {
            component: component.to_string(),
            action: action.to_string(),
            endpoint: endpoint.to_string(),
        };
        self.requests.get_or_create(&label).inc();
        self.durations
            .get_or_create(&label)
            .observe(duration.num_milliseconds() as f64);
    }

    pub fn observe_error(&self, component: &str, action: &str, endpoint: &str) {
        let label = ExtendedLabel {
            component: component.to_string(),
            action: action.to_string(),
            endpoint: endpoint.to_string(),
        };
        self.errors.get_or_create(&label).inc();
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "component_requests",
            "Number of request per component, action and endpoint",
            self.requests.clone(),
        );
        registry.register(
            "component_errors",
            "Number of errors per component, action and endpoint",
            self.errors.clone(),
        );
        registry.register(
            "component_request_duration",
            "Duration of requests per component, action and endpoint in milliseconds",
            self.durations.clone(),
        );
    }
}
