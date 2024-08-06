use crate::api::dapi::rpc_asset_convertors::build_core_fees_response;
use postgre_client::PgClient;
use rocks_db::errors::StorageError;
use std::sync::Arc;

use crate::api::dapi::response::CoreFeesAccountsList;

pub async fn get_core_fees(
    pg_client: Arc<PgClient>,
    limit: u64,
    page: Option<u64>,
    before: Option<String>,
    after: Option<String>,
    cursor: Option<String>,
) -> Result<CoreFeesAccountsList, StorageError> {
    let cursor_enabled = before.is_none() && after.is_none() && page.is_none();
    // if cursor is passed use it as 'after' parameter
    let after = {
        if cursor_enabled {
            cursor
        } else {
            after
        }
    };

    let accounts = pg_client
        .get_core_fees(limit, page, before, after)
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?;

    let (before, after, cursor, page) = if cursor_enabled {
        (
            None,
            None,
            accounts.last().map(|k| k.sorting_id.clone()),
            None,
        )
    } else if let Some(page) = page {
        (None, None, None, Some(page))
    } else {
        (
            accounts.first().map(|k| k.sorting_id.clone()),
            accounts.last().map(|k| k.sorting_id.clone()),
            None,
            None,
        )
    };
    build_core_fees_response(accounts, limit, page, before, after, cursor)
        .map_err(|e| StorageError::Common(format!("Building response: {:?}", e)))
}
