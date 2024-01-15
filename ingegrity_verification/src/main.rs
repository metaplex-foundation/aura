use crate::error::IntegrityVerificationError;
use ::config::config::{parse_config, IntegrityVerificationConfig};
use clap::Parser;
use metrics_utils::utils::setup_metrics;
use metrics_utils::{
    IntegrityVerificationMetrics, IntegrityVerificationMetricsConfig, MetricsTrait,
};
use tracing::{error, info};
// use tokio::task::JoinSet;

pub mod error;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = String::new())]
    env_file: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), IntegrityVerificationError> {
    let args = Args::parse();
    let config = parse_config::<IntegrityVerificationConfig>(args.env_file.as_str())?;
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

    // let tasks = JoinSet::new();

    Ok(())
}
