use crate::api::dapi::asset;
use crate::api::dapi::response::TransactionSignatureList;
use crate::api::dapi::rpc_asset_convertors::build_transaction_signatures_response;
use entities::api_req_params::AssetSortDirection;
use rocks_db::errors::StorageError;
use rocks_db::Storage;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub async fn get_asset_signatures(
    rocks_db: Arc<Storage>,
    id: Option<Pubkey>,
    tree: Option<Pubkey>,
    leaf_idx: Option<u64>,
    sort_by: Option<AssetSortDirection>,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
) -> Result<TransactionSignatureList, StorageError> {
    let signatures = asset::get_asset_signatures(
        rocks_db, id, tree, leaf_idx, page, &before, &after, limit, sort_by,
    )
    .await?;
    Ok(build_transaction_signatures_response(
        signatures, limit, page,
    ))
}
