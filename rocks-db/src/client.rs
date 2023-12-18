use crate::cl_items::ClLeaf;
use crate::{Result, Storage};
use bincode::deserialize;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

impl Storage {
    pub async fn get_owners(&self, assets: Vec<Pubkey>) -> Result<HashMap<Pubkey, String>> {
        // let mut result = HashMap::new();
        //
        // for asset in assets {
        //     let accounts_pks = match self
        //         .select_all_pubkey_by_secondary_idx(&self.account_token_mint_idx, asset)
        //         .await
        //     {
        //         Ok(accounts) => accounts,
        //         Err(_) => {
        //             continue;
        //         }
        //     };
        //     let accounts = self.token_accounts.batch_get(accounts_pks).await?;
        //
        //     let owner = match accounts
        //         .into_iter()
        //         .find(|a| a.is_some() && a.as_ref().unwrap().amount > 0)
        //         .ok_or(StorageError::NoAssetOwner(asset.to_string()))
        //     {
        //         Ok(account) => account.unwrap().owner.to_string(),
        //         Err(_) => {
        //             continue;
        //         }
        //     };
        //
        //     result.insert(asset, owner);
        // }

        Ok(HashMap::new())
    }

    pub async fn get_assets_by_owner(&self, owner: Pubkey) -> Result<Vec<String>> {
        // let accounts_pks = self
        //     .select_all_pubkey_by_secondary_idx(&self.account_token_owner_idx, owner)
        //     .await?;
        // let accounts = Vec::new();

        Ok(Vec::new()) // TODO
    }
}
