use crate::error::UsecaseError;
use async_trait::async_trait;
use entities::models::{TokenAccount, TokenAccountIterableIdx};
use solana_program::pubkey::Pubkey;

#[async_trait]
pub trait TokenAccountsGetter {
    fn token_accounts_pubkeys_iter(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        page: Option<u64>,
        limit: u64,
    ) -> Result<impl Iterator<Item = TokenAccountIterableIdx>, UsecaseError>;
    #[allow(clippy::too_many_arguments)]
    async fn get_token_accounts(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        page: Option<u64>,
        limit: u64,
        show_zero_balance: bool,
    ) -> Result<Vec<TokenAccount>, UsecaseError>;
}
