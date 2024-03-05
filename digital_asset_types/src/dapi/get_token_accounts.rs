use std::sync::Arc;

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
    before: Option<String>,
    after: Option<String>,
    show_zero_balance: bool,
) -> Result<TokenAccountsList, DbErr> {
    let _token_accounts = token_accounts_getter.get_token_accounts(
        owner,
        mint,
        page,
        before,
        after,
        limit,
        show_zero_balance,
    );
    Ok(TokenAccountsList::default())
}
