use crate::config::setup_config;
use crate::error::IntegrityVerificationError;
use clap::Parser;
use metrics_utils::utils::start_metrics;
use metrics_utils::{
    IntegrityVerificationMetrics, IntegrityVerificationMetricsConfig, MetricsTrait,
};
use tokio::task::JoinSet;

mod api;
mod config;
mod diff_checker;
pub mod error;
mod params;
mod requests;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value_t = String::new())]
    env_file: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), IntegrityVerificationError> {
    let args = Args::parse();
    let config = setup_config(args.env_file.as_str());
    env_logger::init();

    let mut metrics_stake =
        IntegrityVerificationMetrics::new(IntegrityVerificationMetricsConfig::new());
    metrics_stake.register_metrics();
    start_metrics(metrics_stake.registry, Some(config.metrics_port)).await;

    let mut tasks = JoinSet::new();

    tasks.spawn(tokio::spawn(async move {}));

    usecase::graceful_stop::listen_shutdown().await;
    usecase::graceful_stop::graceful_stop(&mut tasks).await;

    Ok(())
}
