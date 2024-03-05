use crate::Storage;
use entities::models::{TokenAccount, TokenAccountMintOwnerIdxKey, TokenAccountsWithPagination};
use interface::token_accounts::TokenAccountsGetter;
use solana_sdk::pubkey::Pubkey;

impl TokenAccountsGetter for Storage {
    // fn token_accounts_iter(&self, owner: Option<Pubkey>, mint: Option<Pubkey>, page: Option<u64>, after: Option<String>, limit: u64, show_zero_balance: bool) -> impl Iterator<Item=(TokenAccountMintOwnerIdxKey, TokenAccount)> {
    //     todo!()
    // }

    fn get_token_accounts(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
        limit: u64,
        show_zero_balance: bool,
    ) -> TokenAccountsWithPagination {
        todo!()
    }
}
