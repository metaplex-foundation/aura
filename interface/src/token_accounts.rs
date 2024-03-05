use entities::models::{TokenAccount, TokenAccountMintOwnerIdxKey, TokenAccountsWithPagination};
use solana_program::pubkey::Pubkey;

pub trait TokenAccountsGetter {
    // fn token_accounts_iter(
    //     &self,
    //     owner: Option<Pubkey>,
    //     mint: Option<Pubkey>,
    //     page: Option<u64>,
    //     after: Option<String>,
    //     limit: u64,
    //     show_zero_balance: bool,
    // ) -> impl Iterator<Item = (TokenAccountMintOwnerIdxKey, TokenAccount)>;
    #[allow(clippy::too_many_arguments)]
    fn get_token_accounts(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
        limit: u64,
        show_zero_balance: bool,
    ) -> TokenAccountsWithPagination;
}
