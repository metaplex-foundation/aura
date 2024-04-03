use crate::columns::TokenAccountMintOwnerIdx;
use crate::Storage;
use async_trait::async_trait;
use entities::models::{
    TokenAccount, TokenAccountIterableIdx, TokenAccountMintOwnerIdxKey, TokenAccountOwnerIdxKey,
};
use interface::error::UsecaseError;
use interface::token_accounts::TokenAccountsGetter;
use solana_sdk::pubkey::Pubkey;

#[async_trait]
impl TokenAccountsGetter for Storage {
    fn token_accounts_pubkeys_iter(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        page: Option<u64>,
        limit: u64,
        show_zero_balance: bool,
    ) -> Result<impl Iterator<Item = TokenAccountIterableIdx>, UsecaseError> {
        if owner.is_none() && mint.is_none() {
            return Err(UsecaseError::InvalidParameters(
                "Owner or mint must be provided".to_string(),
            ));
        }
        let iter = match mint {
            Some(mint) => self
                .token_account_mint_owner_idx
                .iter(TokenAccountMintOwnerIdxKey {
                    mint,
                    owner: owner.unwrap_or_default(),
                    token_account: Pubkey::default(),
                }),
            None => {
                self.token_account_owner_idx.iter(TokenAccountOwnerIdxKey {
                    // We have check above, so this is safe
                    owner: owner.expect("Expected owner when mint is None"),
                    token_account: Pubkey::default(),
                })
            }
        };

        Ok(iter
            .filter_map(std::result::Result::ok)
            .flat_map(move |(key, value)| {
                let mut res = match mint {
                    Some(_) => {
                        let key = self
                            .token_account_mint_owner_idx
                            .decode_key(key.to_vec())
                            .ok()?;
                        TokenAccountIterableIdx {
                            mint: Some(key.mint),
                            owner: key.owner,
                            token_account: key.token_account,
                            is_zero_balance: false,
                        }
                    }
                    None => {
                        let key = self.token_account_owner_idx.decode_key(key.to_vec()).ok()?;
                        TokenAccountIterableIdx {
                            mint: None,
                            owner: key.owner,
                            token_account: key.token_account,
                            is_zero_balance: false,
                        }
                    }
                };
                // TokenAccountMintOwnerIdx and TokenAccountOwnerIdx have the same data struct
                let value: TokenAccountMintOwnerIdx =
                    bincode::deserialize(value.to_vec().as_slice()).ok()?;
                res.is_zero_balance = value.is_zero_balance;

                Some(res)
            })
            .filter_map(move |iterable_token_account| {
                if !show_zero_balance && iterable_token_account.is_zero_balance {
                    return None;
                }
                Some(iterable_token_account)
            })
            .skip(
                page.and_then(|page| page.saturating_sub(1).checked_mul(limit))
                    .unwrap_or_default() as usize,
            ))
    }

    async fn get_token_accounts(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        page: Option<u64>,
        limit: u64,
        show_zero_balance: bool,
    ) -> Result<Vec<TokenAccount>, UsecaseError> {
        let mut pubkeys = Vec::new();
        for key in self.token_accounts_pubkeys_iter(owner, mint, page, limit, show_zero_balance)? {
            if (pubkeys.len() >= limit as usize)
                || owner.map(|owner| owner != key.owner).unwrap_or_default()
                || mint != key.mint
            {
                break;
            }
            pubkeys.push(key.token_account);
        }

        Ok(self
            .token_accounts
            .batch_get(pubkeys)
            .await
            .map_err(|e| UsecaseError::Storage(e.to_string()))?
            .into_iter()
            .flat_map(|ta| {
                ta.map(|ta| TokenAccount {
                    address: ta.pubkey.to_string(),
                    mint: ta.mint.to_string(),
                    owner: ta.owner.to_string(),
                    amount: ta.amount as u64,
                    delegated_amount: ta.delegated_amount as u64,
                    frozen: ta.frozen,
                })
            })
            .collect::<Vec<_>>())
    }
}
