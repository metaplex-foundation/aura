use crate::columns::TokenAccountMintOwnerIdx;
use crate::Storage;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
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
        token_account: Option<Pubkey>,
        reverse_iter: bool,
        page: Option<u64>,
        limit: u64,
        show_zero_balance: bool,
    ) -> Result<impl Iterator<Item = TokenAccountIterableIdx>, UsecaseError> {
        let iter = {
            if reverse_iter {
                match mint {
                    Some(mint) => self.token_account_mint_owner_idx.iter_reverse(
                        TokenAccountMintOwnerIdxKey {
                            mint,
                            owner: owner.unwrap_or_default(),
                            token_account: token_account.unwrap_or_default(),
                        },
                    ),
                    None => {
                        self.token_account_owner_idx
                            .iter_reverse(TokenAccountOwnerIdxKey {
                                // We have check above, so this is safe
                                owner: owner.expect("Expected owner when mint is None"),
                                token_account: token_account.unwrap_or_default(),
                            })
                    }
                }
            } else {
                match mint {
                    Some(mint) => {
                        self.token_account_mint_owner_idx
                            .iter(TokenAccountMintOwnerIdxKey {
                                mint,
                                owner: owner.unwrap_or_default(),
                                token_account: token_account.unwrap_or_default(),
                            })
                    }
                    None => {
                        self.token_account_owner_idx.iter(TokenAccountOwnerIdxKey {
                            // We have check above, so this is safe
                            owner: owner.expect("Expected owner when mint is None"),
                            token_account: token_account.unwrap_or_default(),
                        })
                    }
                }
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
        before: Option<String>,
        after: Option<String>,
        page: Option<u64>,
        limit: u64,
        show_zero_balance: bool,
    ) -> Result<Vec<TokenAccount>, UsecaseError> {
        let mut reverse = false;

        let start_from = if let Some(after) = &after {
            Some(after)
        } else if let Some(before) = &before {
            reverse = true;
            Some(before)
        } else {
            None
        };

        let (token_account, decoded_owner) = if let Some(key) = start_from {
            let (token_acc, owner) = decode_sorting_key(key)
                .map_err(|e| UsecaseError::InvalidParameters(format!("Pagination: {:?}", e)))?;

            (Some(token_acc), Some(owner))
        } else {
            (None, None)
        };

        let until_token_acc = if let (Some(_after), Some(before)) = (&after, &before) {
            let (token_acc, _owner) = decode_sorting_key(before)
                .map_err(|e| UsecaseError::InvalidParameters(format!("Pagination: {:?}", e)))?;

            Some(token_acc)
        } else {
            None
        };

        let owner = owner.or(decoded_owner);

        if (mint, owner, token_account) == (None, None, None) {
            return Err(UsecaseError::InvalidParameters(
                "Owner or mint must be provided".to_string(),
            ));
        }

        let mut pubkeys = Vec::new();
        for key in self.token_accounts_pubkeys_iter(
            owner,
            mint,
            token_account,
            reverse,
            page,
            limit,
            show_zero_balance,
        )? {
            if (pubkeys.len() >= limit as usize)
                || owner.map(|owner| owner != key.owner).unwrap_or_default()
                || mint != key.mint
                || until_token_acc
                    .map(|k| key.token_account > k)
                    .unwrap_or_default()
            {
                break;
            }
            // if we paginating we should not return the key we started from
            if let Some(t_a) = token_account {
                if t_a == key.token_account {
                    continue;
                }
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

pub fn encode_sorting_key(owner: &Pubkey, token_account: &Pubkey) -> String {
    let owner_b = owner.to_bytes();
    let token_b = token_account.to_bytes();

    let mut v = Vec::from(owner_b);
    v.extend_from_slice(token_b.as_ref());

    general_purpose::STANDARD_NO_PAD.encode(v)
}

pub fn decode_sorting_key(key: &String) -> Result<(Pubkey, Pubkey), String> {
    let decoded = general_purpose::STANDARD_NO_PAD
        .decode(key)
        .map_err(|e| e.to_string())?;

    let mut raw_owner_pubkey = [0; 32];
    raw_owner_pubkey.copy_from_slice(&decoded[0..32]);

    let mut raw_token_pubkey = [0; 32];
    raw_token_pubkey.copy_from_slice(&decoded[32..]);

    Ok((
        Pubkey::new_from_array(raw_token_pubkey),
        Pubkey::new_from_array(raw_owner_pubkey),
    ))
}
