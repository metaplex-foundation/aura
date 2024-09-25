use crate::error::UsecaseError;
use async_trait::async_trait;
use entities::models::{TokenAccResponse, TokenAccountIterableIdx};
use solana_program::pubkey::Pubkey;

#[async_trait]
pub trait TokenAccountsGetter {
    #[allow(clippy::too_many_arguments)]
    fn token_accounts_pubkeys_iter(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        starting_token_account: Option<Pubkey>,
        reverse_iter: bool,
        page: Option<u64>,
        limit: Option<u64>,
        show_zero_balance: bool,
    ) -> Result<impl Iterator<Item = TokenAccountIterableIdx>, UsecaseError>;
    #[allow(clippy::too_many_arguments)]
    async fn get_token_accounts(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        before: Option<String>,
        after: Option<String>,
        page: Option<u64>,
        limit: u64,
        show_zero_balance: bool,
    ) -> Result<Vec<TokenAccResponse>, UsecaseError>;
}
