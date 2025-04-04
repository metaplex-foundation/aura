use std::{str::FromStr, sync::Arc};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;
use tracing::{error, info};

use crate::asset_fixer::AssetFixer;

// Without Debug since Storage doesn't implement Debug
#[derive(Clone)]
pub struct AssetFixHandler {
    asset_fixer: Arc<AssetFixer>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FixAccountAssetsRequest {
    pub asset_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct FixAccountAssetsResponse {
    pub success: bool,
    pub fixed_count: usize,
    pub failed_asset_ids: Vec<String>,
}

/// Wrapper for the AccountsProcessor to implement AssetProcessorInterface
pub struct AssetProcessorWrapper {}

impl AssetFixHandler {
    pub fn new(asset_fixer: Arc<AssetFixer>) -> Self {
        Self { asset_fixer }
    }

    pub async fn fix_account_assets(
        state: State<Arc<Self>>,
        Json(request): Json<FixAccountAssetsRequest>,
    ) -> impl IntoResponse {
        info!("Fixing {} account-based assets", request.asset_ids.len());

        // Convert string asset IDs to Pubkeys
        let mut pubkeys = Vec::with_capacity(request.asset_ids.len());
        let mut invalid_asset_ids = Vec::new();

        for asset_id in &request.asset_ids {
            match Pubkey::from_str(asset_id) {
                Ok(pubkey) => pubkeys.push(pubkey),
                Err(_) => {
                    error!("Invalid asset ID format: {}", asset_id);
                    invalid_asset_ids.push(asset_id.clone());
                },
            }
        }

        if !invalid_asset_ids.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(FixAccountAssetsResponse {
                    success: false,
                    fixed_count: 0,
                    failed_asset_ids: invalid_asset_ids,
                }),
            );
        }

        // Fix the accounts
        match state.asset_fixer.fix_accounts(pubkeys).await {
            Ok(result) => {
                let failed_asset_ids: Vec<String> =
                    result.failed_pubkeys.into_iter().map(|pubkey| pubkey.to_string()).collect();

                (
                    StatusCode::OK,
                    Json(FixAccountAssetsResponse {
                        success: result.fixed_count > 0 && failed_asset_ids.is_empty(),
                        fixed_count: result.fixed_count,
                        failed_asset_ids,
                    }),
                )
            },
            Err(e) => {
                error!("Error fixing accounts: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(FixAccountAssetsResponse {
                        success: false,
                        fixed_count: 0,
                        failed_asset_ids: request.asset_ids,
                    }),
                )
            },
        }
    }
}
