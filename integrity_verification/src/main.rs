use crate::config::setup_config;
use crate::error::IntegrityVerificationError;
use clap::Parser;
use metrics_utils::utils::setup_metrics;
use metrics_utils::{
    IntegrityVerificationMetrics, IntegrityVerificationMetricsConfig, MetricsTrait,
};
use tokio::task::JoinSet;
use tracing::{error, info};

mod api;
mod config;
pub mod error;

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

    let mut metrics_state =
        IntegrityVerificationMetrics::new(IntegrityVerificationMetricsConfig::new());
    metrics_state.register_metrics();

    tokio::spawn(async move {
        match setup_metrics(metrics_state.registry, Some(config.metrics_port)).await {
            Ok(_) => {
                info!("Setup metrics successfully")
            }
            Err(e) => {
                error!("Setup metrics failed: {:?}", e)
            }
        }
    });

    let mut tasks = JoinSet::new();

    tasks.spawn(tokio::spawn(async move {}));

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(_) => {
                info!("One of the tasks was finished")
            }
            Err(err) if err.is_panic() => {
                let err = err.into_panic();
                error!("Task panic: {:?}", err);
            }
            Err(err) => {
                let err = err.to_string();
                error!("Task error: {}", err);
            }
        }
    }

    Ok(())
}
