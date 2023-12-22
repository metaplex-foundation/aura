use async_trait::async_trait;
use entities::models::{CompleteAssetDetails, Updated};
use interface::{AssetDetailsStream, AssetDetailsStreamer, AsyncError};
use solana_sdk::pubkey::Pubkey;
use tokio_stream::wrappers::ReceiverStream;

use crate::{asset::SlotAssetIdx, column::TypedColumn, errors::StorageError, Storage};

#[async_trait]
impl AssetDetailsStreamer for Storage {
    async fn get_asset_details_stream_in_range(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> Result<AssetDetailsStream, AsyncError> {
        let (tx, rx) = tokio::sync::mpsc::channel(32); // Adjust the channel size as needed
        let start_key = (start_slot, solana_sdk::pubkey::Pubkey::default());
        // let backend = self.backend.clone();
        // let asset_details_cf = self.asset_details_cf.clone();

        // Spawn a background task to query RocksDB
        tokio::spawn(async move {
            let iterator = self.slot_asset_idx.iter(start_key);
            for pair in iterator {
                // if we got an error, send it over the tx and stop
                match pair {
                    Ok((idx_key, _)) => {
                        match SlotAssetIdx::decode_key(idx_key.to_vec()) {
                            Ok((slot, pubkey)) => {
                                if slot > end_slot {
                                    break;
                                }
                                match self.get_complete_asset_details(pubkey) {
                                    Ok(details) => {
                                        if let Err(_) = tx.send(Ok(details)).await {
                                            // If receiver is dropped, stop sending
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx
                                            .send(Err(Box::new(e) as interface::AsyncError))
                                            .await; // Send error and break
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                // Send error and continue
                                if let Err(_) =
                                    tx.send(Err(Box::new(e) as interface::AsyncError)).await
                                {
                                    // If receiver is dropped, stop sending
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Box::new(e) as interface::AsyncError));
                        break;
                    }
                }
            }
            // todo: do we need to close the channel?
        });

        Ok(Box::pin(ReceiverStream::new(rx)) as AssetDetailsStream)
    }
}

impl Storage {
    fn get_complete_asset_details(&self, pubkey: Pubkey) -> crate::Result<CompleteAssetDetails> {
        let static_data = self.asset_static_data.get(pubkey)?;
        let static_data = match static_data {
            None => {
                return Err(crate::errors::StorageError::Common(
                    "Asset static data not found".to_string(),
                ));
            }
            Some(static_data) => static_data,
        };

        let dynamic_data = self.asset_dynamic_data.get(pubkey)?;
        let dynamic_data = match dynamic_data {
            None => {
                return Err(crate::errors::StorageError::Common(
                    "Asset dynamic data not found".to_string(),
                ));
            }
            Some(dynamic_data) => dynamic_data,
        };
        let authority = self.asset_authority_data.get(pubkey)?;
        let authority = match authority {
            None => {
                return Err(crate::errors::StorageError::Common(
                    "Asset authority not found".to_string(),
                ));
            }
            Some(authority) => authority,
        };
        let owner = self.asset_owner_data.get(pubkey)?;
        let owner = match owner {
            None => {
                return Err(crate::errors::StorageError::Common(
                    "Asset owner not found".to_string(),
                ));
            }
            Some(owner) => owner,
        };

        let leaves = self.asset_leaf_data.get(pubkey)?;
        let collection = self.asset_collection_data.get(pubkey)?;
        
        let onchain_data = match dynamic_data.onchain_data {
            None => None,
            Some(onchain_data) => {
                let v = serde_json::from_str(&onchain_data.value)
                    .map_err(|e| StorageError::Common(e.to_string()))?;
                Some(Updated::new(onchain_data.slot_updated, onchain_data.seq, v))
            }
        };

        Ok(CompleteAssetDetails {
            pubkey: static_data.pubkey,
            specification_asset_class: static_data.specification_asset_class,
            royalty_target_type: static_data.royalty_target_type,
            slot_created: static_data.created_at as u64,
            is_compressible: dynamic_data.is_compressible,
            is_compressed: dynamic_data.is_compressed,
            is_frozen: dynamic_data.is_frozen,
            supply: dynamic_data.supply,
            seq: dynamic_data.seq,
            is_burnt: dynamic_data.is_burnt,
            was_decompressed: dynamic_data.was_decompressed,
            onchain_data: onchain_data,
            creators: dynamic_data.creators,
            royalty_amount: dynamic_data.royalty_amount,
            authority: Updated::new(
                authority.slot_updated,
                None, //todo: where do we get seq?
                authority.authority,
            ),
            owner: owner.owner,
            delegate: owner.delegate,
            owner_type: owner.owner_type,
            owner_delegate_seq: owner.owner_delegate_seq,

        })
    }
}
