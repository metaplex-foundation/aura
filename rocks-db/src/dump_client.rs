use csv::Writer;
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, io::Write};
use sha2::{Digest, Sha256};
use hex;

use crate::{column::TypedColumn, key_encoders::decode_pubkey, storage_traits::AssetIndexReader, AssetStaticDetails, Storage};

impl Storage {
    pub async fn dump_scv<W: Write>(
        &self,
        metadata_writer: &mut Writer<W>,
        creators_writer: &mut Writer<W>,
        assets_writer: &mut Writer<W>,
        batch_size: usize,
        rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), String> {
        let iter = self.asset_static_data.iter_start();
        // collect batch of keys
        let mut batch = Vec::with_capacity(batch_size);
        let mut metadata_key_set = HashSet::new();
        for k in iter
            .filter_map(|k| k.ok())
            .filter_map(|(key, _)| decode_pubkey(key.to_vec()).ok())
        {
            batch.push(k);
            if batch.len() == batch_size {
                self.dump_csv_batch(&batch, metadata_writer, creators_writer, assets_writer, &mut metadata_key_set).await?;
                batch.clear();
            }
            if !rx.is_empty() {
                break;
            }
        }
        if !batch.is_empty() {
            self.dump_csv_batch(&batch, metadata_writer, creators_writer, assets_writer, &mut metadata_key_set).await?;
        }
        Ok(())
    }

    async fn dump_csv_batch<W: Write>(
        &self,
        batch: &[Pubkey],
        metadata_writer: &mut csv::Writer<W>,
        creators_writer: &mut csv::Writer<W>,
        assets_writer: &mut csv::Writer<W>,
        metadata_key_set: &mut HashSet<Vec<u8>>,
    ) -> Result<(), String> {
        let indexes = self.get_asset_indexes(batch).await?;
        for (key, index) in indexes {
            if let Some(url) = index.metadata_url {
                let mut hasher = Sha256::new();
    hasher.update(url.metadata_url);
    let metadata_key = hasher.finalize();
            }
            
            
            // metadata_writer.serialize(asset)?;
            // creators_writer.serialize(asset)?;
            // assets_writer.serialize(asset)?;
        }
        
    }
}
