use crate::dapi::common::build_core_fees_response;
use postgre_client::PgClient;
use rocks_db::errors::StorageError;
use std::sync::Arc;

use crate::rpc::response::CoreFeesAccountsList;

pub async fn get_core_fees(
    pg_client: Arc<PgClient>,
    limit: u64,
    page: Option<u64>,
    // before: Option<String>,
    // after: Option<String>,
    // cursor: Option<String>,
) -> Result<CoreFeesAccountsList, StorageError> {
    // let cursor_enabled = before.is_none() && after.is_none() && page.is_none();

    // if cursor is passed use it as 'after' parameter
    // let after = {
    //     if cursor_enabled {
    //         cursor
    //     } else {
    //         after
    //     }
    // };

    let accounts = pg_client
        .get_core_fees(page.unwrap_or(1), limit)
        .await
        .map_err(|e| StorageError::Common(e.to_string()))?;

    build_core_fees_response(accounts, limit, page)
        .map_err(|e| StorageError::Common(format!("Building response: {:?}", e)))
}
