use crate::config::setup_config;
use crate::diff_checker::{
    DiffChecker, GET_ASSET_BY_AUTHORITY_METHOD, GET_ASSET_BY_CREATOR_METHOD,
    GET_ASSET_BY_GROUP_METHOD, GET_ASSET_BY_OWNER_METHOD, GET_ASSET_METHOD, GET_ASSET_PROOF_METHOD,
};
use crate::error::IntegrityVerificationError;
use clap::Parser;
use metrics_utils::utils::start_metrics;
use metrics_utils::{
    IntegrityVerificationMetrics, IntegrityVerificationMetricsConfig, MetricsTrait,
};
use postgre_client::PgClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

mod api;
mod config;
mod diff_checker;
mod error;
mod params;
mod requests;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value_t = String::new())]
    env_file: String,
}

const TESTS_INTERVAL_SEC: u64 = 15;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), IntegrityVerificationError> {
    let args = Args::parse();
    let config = setup_config(args.env_file.as_str());
    env_logger::init();
    info!("IntegrityVerification start");

    let mut metrics = IntegrityVerificationMetrics::new(IntegrityVerificationMetricsConfig::new());
    metrics.register_metrics();
    start_metrics(metrics.registry, Some(config.metrics_port)).await;

    let keys_fetcher =
        PgClient::new(&config.database_url, &config.get_sql_log_level(), 100, 500).await;
    let diff_checker = DiffChecker::new(
        config.reference_host.clone(),
        config.testing_host.clone(),
        keys_fetcher,
        metrics.integrity_verification_metrics.clone(),
    );

    let mut tasks = JoinSet::new();
    let cancel_token = CancellationToken::new();
    run_tests(
        &mut tasks,
        config.get_run_secondary_indexes_tests(),
        config.get_run_proofs_tests(),
        diff_checker,
        metrics.integrity_verification_metrics.clone(),
        cancel_token.clone(),
    )
    .await;

    usecase::graceful_stop::listen_shutdown().await;
    cancel_token.cancel();
    usecase::graceful_stop::graceful_stop(&mut tasks).await;

    Ok(())
}

macro_rules! spawn_test {
    ($tasks:ident, $diff_checker:ident, $metrics:ident, $method:ident, $metric_label:expr, $cancel_token:expr) => {{
        info!("{}", format!("{} tests start", &$metric_label));
        let diff_checker_clone = $diff_checker.clone();
        let metrics_clone = $metrics.clone();
        let cancel_token_clone = $cancel_token.clone();
        $tasks.spawn(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = async {
                        if let Err(e) = diff_checker_clone.$method().await {
                            error!("Fetch keys: {}", e);
                            metrics_clone.inc_fetch_keys_errors($metric_label);
                        }
                        tokio::time::sleep(Duration::from_secs(TESTS_INTERVAL_SEC)).await
                    } => {},
                    _ = cancel_token_clone.cancelled() => {
                        break;
                    }
                };
            }
        }));
    }};
}

async fn run_tests(
    tasks: &mut JoinSet<Result<(), JoinError>>,
    run_secondary_indexes_tests: bool,
    run_proofs_tests: bool,
    diff_checker: DiffChecker<PgClient>,
    metrics: Arc<IntegrityVerificationMetricsConfig>,
    cancel_token: CancellationToken,
) {
    let diff_checker = Arc::new(diff_checker);
    spawn_test!(
        tasks,
        diff_checker,
        metrics,
        check_get_asset,
        GET_ASSET_METHOD,
        cancel_token
    );
    if run_proofs_tests {
        spawn_test!(
            tasks,
            diff_checker,
            metrics,
            check_get_asset_proof,
            GET_ASSET_PROOF_METHOD,
            cancel_token
        );
    }
    if !run_secondary_indexes_tests {
        // All tests below test GetBySecondaryIndex methods
        return;
    }

    spawn_test!(
        tasks,
        diff_checker,
        metrics,
        check_get_asset_by_owner,
        GET_ASSET_BY_OWNER_METHOD,
        cancel_token
    );
    spawn_test!(
        tasks,
        diff_checker,
        metrics,
        check_get_asset_by_authority,
        GET_ASSET_BY_AUTHORITY_METHOD,
        cancel_token
    );
    spawn_test!(
        tasks,
        diff_checker,
        metrics,
        check_get_asset_by_creator,
        GET_ASSET_BY_CREATOR_METHOD,
        cancel_token
    );
    spawn_test!(
        tasks,
        diff_checker,
        metrics,
        check_get_asset_by_group,
        GET_ASSET_BY_GROUP_METHOD,
        cancel_token
    );
}
