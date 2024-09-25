use crate::column::TypedColumn;
use crate::key_encoders::{decode_pubkeyx2, decode_pubkeyx3, encode_pubkeyx2, encode_pubkeyx3};
use crate::Result;
use crate::Storage;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use bincode::deserialize;
use entities::models::{
    ResponseTokenAccount, TokenAccResponse, TokenAccount, TokenAccountIterableIdx,
    TokenAccountMintOwnerIdxKey, TokenAccountOwnerIdxKey,
};
use interface::error::UsecaseError;
use interface::token_accounts::TokenAccountsGetter;
use rocksdb::MergeOperands;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tracing::log::error;
use usecase::response_prettier::filter_non_null_fields;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenAccountOwnerIdx {
    pub is_zero_balance: bool,
    pub write_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenAccountMintOwnerIdx {
    pub is_zero_balance: bool,
    pub write_version: u64,
}

impl TypedColumn for TokenAccountOwnerIdx {
    type KeyType = TokenAccountOwnerIdxKey;

    type ValueType = Self;
    const NAME: &'static str = "TOKEN_ACCOUNTS_OWNER_IDX";

    fn encode_key(key: TokenAccountOwnerIdxKey) -> Vec<u8> {
        encode_pubkeyx2((key.owner, key.token_account))
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let (owner, token_account) = decode_pubkeyx2(bytes)?;
        Ok(TokenAccountOwnerIdxKey {
            owner,
            token_account,
        })
    }
}

impl TypedColumn for TokenAccountMintOwnerIdx {
    type KeyType = TokenAccountMintOwnerIdxKey;

    type ValueType = Self;
    const NAME: &'static str = "TOKEN_ACCOUNTS_MINT_OWNER_IDX";

    fn encode_key(key: TokenAccountMintOwnerIdxKey) -> Vec<u8> {
        encode_pubkeyx3((key.mint, key.owner, key.token_account))
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let (mint, owner, token_account) = decode_pubkeyx3(bytes)?;
        Ok(TokenAccountMintOwnerIdxKey {
            mint,
            owner,
            token_account,
        })
    }
}

impl TypedColumn for TokenAccount {
    type KeyType = Pubkey;

    type ValueType = Self;
    const NAME: &'static str = "TOKEN_ACCOUNTS";

    fn encode_key(pubkey: Pubkey) -> Vec<u8> {
        pubkey.to_bytes().to_vec()
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let key = Pubkey::try_from(&bytes[0..32])?;
        Ok(key)
    }
}

#[macro_export]
macro_rules! impl_merge_values {
    ($ty:ty) => {
        impl $ty {
            pub fn merge_values(
                _new_key: &[u8],
                existing_val: Option<&[u8]>,
                operands: &MergeOperands,
            ) -> Option<Vec<u8>> {
                let mut result = vec![];
                let mut write_version = 0;
                if let Some(existing_val) = existing_val {
                    match deserialize::<Self>(existing_val) {
                        Ok(value) => {
                            write_version = value.write_version;
                            result = existing_val.to_vec();
                        }
                        Err(e) => {
                            error!(
                                "RocksDB: {} deserialize existing_val: {}",
                                stringify!($ty),
                                e
                            )
                        }
                    }
                }

                for op in operands {
                    match deserialize::<Self>(op) {
                        Ok(new_val) => {
                            if new_val.write_version > write_version {
                                write_version = new_val.write_version;
                                result = op.to_vec();
                            }
                        }
                        Err(e) => {
                            error!("RocksDB: {} deserialize new_val: {}", stringify!($ty), e)
                        }
                    }
                }

                Some(result)
            }
        }
    };
}

pub fn merge_token_accounts(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    let mut write_version = 0;
    if let Some(existing_val) = existing_val {
        match deserialize::<TokenAccount>(existing_val) {
            Ok(value) => {
                write_version = value.write_version;
                result = existing_val.to_vec();
            }
            Err(e) => {
                error!("RocksDB: TokenAccount deserialize existing_val: {}", e)
            }
        }
    }

    for op in operands {
        match deserialize::<TokenAccount>(op) {
            Ok(new_val) => {
                if new_val.write_version > write_version {
                    write_version = new_val.write_version;
                    result = op.to_vec();
                }
            }
            Err(e) => {
                error!("RocksDB: TokenAccount deserialize new_val: {}", e)
            }
        }
    }

    Some(result)
}

impl_merge_values!(TokenAccountOwnerIdx);
impl_merge_values!(TokenAccountMintOwnerIdx);

#[async_trait]
impl TokenAccountsGetter for Storage {
    fn token_accounts_pubkeys_iter(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        starting_token_account: Option<Pubkey>,
        reverse_iter: bool,
        page: Option<u64>,
        limit: Option<u64>,
        show_zero_balance: bool,
    ) -> std::result::Result<impl Iterator<Item = TokenAccountIterableIdx>, UsecaseError> {
        if (mint, owner) == (None, None) {
            return Err(UsecaseError::InvalidParameters(
                "Owner or mint must be provided".to_string(),
            ));
        }

        let (key, handle) = match mint {
            Some(mint) => (
                TokenAccountMintOwnerIdx::encode_key(TokenAccountMintOwnerIdxKey {
                    mint,
                    owner: owner.unwrap_or_default(),
                    token_account: starting_token_account.unwrap_or_default(),
                }),
                self.token_account_mint_owner_idx.handle(),
            ),
            None => {
                (
                    TokenAccountOwnerIdx::encode_key(TokenAccountOwnerIdxKey {
                        // We have check above, so this is safe
                        owner: owner.expect("Expected owner when mint is None"),
                        token_account: starting_token_account.unwrap_or_default(),
                    }),
                    self.token_account_owner_idx.handle(),
                )
            }
        };

        let iter = self.db.iterator_cf(
            &handle,
            rocksdb::IteratorMode::From(
                &key,
                if reverse_iter {
                    rocksdb::Direction::Reverse
                } else {
                    rocksdb::Direction::Forward
                },
            ),
        );

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
                page.zip(limit)
                    .and_then(|(page, limit)| page.saturating_sub(1).checked_mul(limit))
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
    ) -> std::result::Result<Vec<TokenAccResponse>, UsecaseError> {
        let raw_token_accounts = self
            .get_raw_token_accounts(
                owner,
                mint,
                before,
                after,
                page,
                Some(limit),
                show_zero_balance,
            )
            .await?;

        Ok(raw_token_accounts
            .into_iter()
            .flat_map(|ta| {
                ta.map(|ta| TokenAccResponse {
                    token_acc: ResponseTokenAccount {
                        address: ta.pubkey.to_string(),
                        mint: ta.mint.to_string(),
                        owner: ta.owner.to_string(),
                        amount: ta.amount as u64,
                        delegated_amount: ta.delegated_amount as u64,
                        frozen: ta.frozen,
                        token_extensions: filter_non_null_fields(
                            ta.extensions
                                .and_then(|extensions| serde_json::to_value(extensions).ok())
                                .as_ref(),
                        ),
                    },
                    sorting_id: encode_sorting_key(&ta.owner, &ta.pubkey),
                })
            })
            .collect::<Vec<_>>())
    }
}

impl Storage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn get_raw_token_accounts(
        &self,
        owner: Option<Pubkey>,
        mint: Option<Pubkey>,
        before: Option<String>,
        after: Option<String>,
        page: Option<u64>,
        limit: Option<u64>,
        show_zero_balance: bool,
    ) -> std::result::Result<Vec<Option<TokenAccount>>, UsecaseError> {
        let mut reverse = false;

        let start_from = if let Some(after) = &after {
            Some(after)
        } else if let Some(before) = &before {
            reverse = true;
            Some(before)
        } else {
            None
        };

        let (token_account, decoded_owner) = start_from
            .map(|key| {
                decode_sorting_key(key)
                    .map_err(|e| UsecaseError::InvalidParameters(format!("Pagination: {:?}", e)))
            })
            .transpose()?
            .unzip();

        let until_token_acc = if let (Some(_after), Some(before)) = (&after, &before) {
            let (token_acc, _owner) = decode_sorting_key(before)
                .map_err(|e| UsecaseError::InvalidParameters(format!("Pagination: {:?}", e)))?;

            Some(token_acc)
        } else {
            None
        };

        let owner = owner.or(decoded_owner);

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
            if limit
                .map(|limit| pubkeys.len() >= limit as usize)
                .unwrap_or_default()
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

        // if we've got reverse iterator and should reverse assets order back
        if reverse {
            pubkeys.reverse();
        }

        self.token_accounts
            .batch_get(pubkeys)
            .await
            .map_err(|e| UsecaseError::Storage(e.to_string()))
    }
}

pub fn encode_sorting_key(owner: &Pubkey, token_account: &Pubkey) -> String {
    let owner_b = owner.to_bytes();
    let token_b = token_account.to_bytes();

    let mut v = Vec::from(owner_b);
    v.extend_from_slice(token_b.as_ref());

    general_purpose::STANDARD_NO_PAD.encode(v)
}

pub fn decode_sorting_key(key: &String) -> std::result::Result<(Pubkey, Pubkey), String> {
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
