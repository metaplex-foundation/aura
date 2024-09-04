use async_trait::async_trait;
use entities::enums::UnprocessedAccount;
use solana_program::pubkey::Pubkey;

#[async_trait]
pub trait UnprocessedAccountsGetter {
    async fn next_account(&self) -> Option<(Pubkey, UnprocessedAccount)>;
}
