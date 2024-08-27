use crate::error::UsecaseError;
use async_trait::async_trait;
use mockall::automock;
use solana_program::pubkey::Pubkey;

#[automock]
#[async_trait]
pub trait AccountBalanceGetter {
    async fn get_account_balance_lamports(&self, address: &Pubkey) -> Result<u64, UsecaseError>;
}