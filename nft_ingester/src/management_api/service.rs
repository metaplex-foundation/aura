use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    routing::{get, post},
    Router,
};
use metrics_utils::IngesterMetricsConfig;
use postgre_client::PgClient;
use rocks_db::Storage;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    asset_fixer::AssetFixer, management_api::handlers::AssetFixHandler,
    processors::processors_wrapper::ProcessorsWrapper,
};

/// Starts the management API server
pub async fn start_management_api(
    cancellation_token: CancellationToken,
    metrics: Arc<IngesterMetricsConfig>,
    rocks_storage: Arc<Storage>,
    postgre_client: Arc<PgClient>,
    rpc_client: Arc<RpcClient>,
    wellknown_fungible_accounts: HashMap<String, String>,
    port: u16,
) {
    let accounts_processor = ProcessorsWrapper::build(
        cancellation_token.child_token(),
        metrics.clone(),
        postgre_client,
        rpc_client.clone(),
        wellknown_fungible_accounts,
    )
    .await
    .unwrap();

    let asset_fixer =
        Arc::new(AssetFixer::new(rocks_storage, rpc_client, accounts_processor, metrics, 10000));
    let asset_fix_handler = Arc::new(AssetFixHandler::new(asset_fixer));

    // build our application with routes
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/management/fix-account-assets", post(AssetFixHandler::fix_account_assets))
        .with_state(asset_fix_handler);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Management API listening on {}", addr);

    // run the server with graceful shutdown
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(cancellation_token.cancelled())
        .await
        .unwrap_or_else(|e| {
            error!("Management API server error: {}", e);
        });
}

/// Simple health check handler
async fn health_check() -> &'static str {
    "Management API is running"
}
