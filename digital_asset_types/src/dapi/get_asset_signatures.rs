use crate::dao::scopes;
use crate::dapi::common::build_transaction_signatures_response;
use crate::rpc::response::TransactionSignatureList;
use entities::api_req_params::AssetSortDirection;
use rocks_db::Storage;
use sea_orm::DbErr;
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
) -> Result<TransactionSignatureList, DbErr> {
    let signatures = scopes::asset::get_asset_signatures(
        rocks_db, id, tree, leaf_idx, page, &before, &after, limit, sort_by,
    )
    .await?;

    Ok(build_transaction_signatures_response(
        signatures
            .asset_signatures
            .into_iter()
            .map(|sig| sig.into())
            .collect(),
        limit,
        page,
        Some(signatures.before.to_string()),
        Some(signatures.after.to_string()),
    ))
}
