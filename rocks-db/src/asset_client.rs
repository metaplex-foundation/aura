use std::sync::atomic::Ordering;

use solana_sdk::pubkey::Pubkey;

use crate::asset::{AssetsUpdateIdx, SlotAssetIdx};
use crate::key_encoders::encode_u64x2_pubkey;
use crate::{Result, Storage};

impl Storage {
    fn get_next_asset_update_seq(&self) -> Result<u64> {
        if self.assets_update_last_seq.load(Ordering::SeqCst) == 0 {
            // If assets_update_next_seq is zero, fetch the last key from assets_update_idx
            let mut iter = self.assets_update_idx.iter_end(); // Assuming iter_end method fetches the last item

            if let Some(pair) = iter.next() {
                let (last_key, _) = pair?;
                // Assuming the key is structured as (u64, ...)

                let seq = u64::from_be_bytes(last_key[..std::mem::size_of::<u64>()].try_into()?);
                self.assets_update_last_seq.store(seq, Ordering::SeqCst);
            }
        }
        // Increment and return the sequence number
        let seq = self.assets_update_last_seq.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(seq)
    }
    
    // TODO: Add a backfiller to fill the slot_asset_idx based on the assets_update_idx

    pub fn asset_updated(&self, slot: u64, pubkey: Pubkey) -> Result<()> {
        let seq = self.get_next_asset_update_seq()?;
        let value = encode_u64x2_pubkey(seq, slot, pubkey);
        self.assets_update_idx.put(value, &AssetsUpdateIdx {})?;
        self.slot_asset_idx.put((slot, pubkey), &SlotAssetIdx {})
    }
}
