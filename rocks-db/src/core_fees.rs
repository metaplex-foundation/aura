use crate::{Result, Storage};
use solana_sdk::pubkey::Pubkey;

impl Storage {
    pub fn save_non_paid_asset(&self, pk: Pubkey) -> Result<()> {
        Ok(())
    }
}
