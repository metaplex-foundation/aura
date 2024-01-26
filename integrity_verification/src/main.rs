use crate::config::{setup_config, TestSourceMode};
use crate::diff_checker::{
    DiffChecker, GET_ASSET_BY_AUTHORITY_METHOD, GET_ASSET_BY_CREATOR_METHOD,
    GET_ASSET_BY_GROUP_METHOD, GET_ASSET_BY_OWNER_METHOD, GET_ASSET_METHOD, GET_ASSET_PROOF_METHOD,
};
use crate::error::IntegrityVerificationError;
use crate::file_keys_fetcher::FileKeysFetcher;
use clap::Parser;
use metrics_utils::utils::start_metrics;
use metrics_utils::{
    BackfillerMetricsConfig, IntegrityVerificationMetrics, IntegrityVerificationMetricsConfig,
    MetricsTrait,
};
use postgre_client::storage_traits::IntegrityVerificationKeysFetcher;
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
mod file_keys_fetcher;
mod params;
mod requests;
mod slots_dumper;

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

    let mut metrics = IntegrityVerificationMetrics::new(
        IntegrityVerificationMetricsConfig::new(),
        BackfillerMetricsConfig::new(),
    );
    metrics.register_metrics();
    start_metrics(metrics.registry, Some(config.metrics_port)).await;

    let mut tasks = JoinSet::new();
    let cancel_token = CancellationToken::new();
    match config.test_source_mode {
        TestSourceMode::File => {
            let diff_checker = DiffChecker::new(
                config.reference_host.clone(),
                config.testing_host.clone(),
                FileKeysFetcher::new(&config.test_file_path_container.clone().unwrap())
                    .await
                    .unwrap(),
                metrics.integrity_verification_metrics.clone(),
                metrics.slot_collector_metrics.clone(),
                config.big_table_creds_path.clone(),
                config.slots_collect_path_container.clone(),
                config.collect_slots,
            )
            .await;
            run_tests(
                &mut tasks,
                config.run_secondary_indexes_tests,
                config.run_proofs_tests,
                config.run_assets_tests,
                diff_checker,
                metrics.integrity_verification_metrics.clone(),
                cancel_token.clone(),
            )
            .await;
        }
        TestSourceMode::Database => {
            let diff_checker = DiffChecker::new(
                config.reference_host.clone(),
                config.testing_host.clone(),
                PgClient::new(&config.database_url.clone().unwrap(), 100, 500).await,
                metrics.integrity_verification_metrics.clone(),
                metrics.slot_collector_metrics.clone(),
                config.big_table_creds_path.clone(),
                config.slots_collect_path_container.clone(),
                config.collect_slots,
            )
            .await;
            run_tests(
                &mut tasks,
                config.run_secondary_indexes_tests,
                config.run_proofs_tests,
                config.run_assets_tests,
                diff_checker,
                metrics.integrity_verification_metrics.clone(),
                cancel_token.clone(),
            )
            .await;
        }
    };

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

async fn run_tests<T>(
    tasks: &mut JoinSet<Result<(), JoinError>>,
    run_secondary_indexes_tests: bool,
    run_proofs_tests: bool,
    run_assets_tests: bool,
    diff_checker: DiffChecker<T>,
    metrics: Arc<IntegrityVerificationMetricsConfig>,
    cancel_token: CancellationToken,
) where
    T: IntegrityVerificationKeysFetcher + Send + Sync + 'static,
{
    let diff_checker = Arc::new(diff_checker);
    if run_assets_tests {
        spawn_test!(
            tasks,
            diff_checker,
            metrics,
            check_get_asset,
            GET_ASSET_METHOD,
            cancel_token
        );
    }
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
