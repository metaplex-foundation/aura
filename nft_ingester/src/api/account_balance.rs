use async_trait::async_trait;
use interface::account_balance::AccountBalanceGetter;
use interface::error::UsecaseError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use std::sync::Arc;

pub struct AccountBalanceGetterImpl {
    client: Arc<RpcClient>,
}

impl AccountBalanceGetterImpl {
    pub fn new(client: Arc<RpcClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AccountBalanceGetter for AccountBalanceGetterImpl {
    async fn get_account_balance_lamports(&self, address: &Pubkey) -> Result<u64, UsecaseError> {
        Ok(self.client.get_account(address).await?.lamports)
    }
}
