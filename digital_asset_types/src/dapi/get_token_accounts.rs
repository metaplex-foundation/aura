use std::sync::Arc;

use crate::dapi::common::build_token_accounts_response;
use interface::token_accounts::TokenAccountsGetter;
use sea_orm::DbErr;
use solana_sdk::pubkey::Pubkey;

use crate::rpc::response::TokenAccountsList;

#[allow(clippy::too_many_arguments)]
pub async fn get_token_accounts(
    token_accounts_getter: Arc<impl TokenAccountsGetter>,
    owner: Option<Pubkey>,
    mint: Option<Pubkey>,
    limit: u64,
    page: Option<u64>,
    show_zero_balance: bool,
) -> Result<TokenAccountsList, DbErr> {
    let token_accounts = token_accounts_getter
        .get_token_accounts(owner, mint, page, limit, show_zero_balance)
        .await
        .map_err(|e| DbErr::Custom(e.to_string()))?;
    Ok(build_token_accounts_response(token_accounts, limit, page))
}
