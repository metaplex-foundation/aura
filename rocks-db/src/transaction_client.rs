use async_trait::async_trait;
use entities::models::SignatureWithSlot;
use interface::error::StorageError;
use solana_sdk::pubkey::Pubkey;

use crate::{
    signature_client::SignatureIdx,
    transaction::{InstructionResult, TransactionResult, TransactionResultPersister},
    Storage,
};

#[async_trait]
impl TransactionResultPersister for Storage {
    async fn store_block(&self, txs: Vec<TransactionResult>) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for tx in txs {
            self.store_transaction_result_with_batch(&mut batch, tx, false)?;
        }
        self.write_batch(batch)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }
}

impl Storage {
    pub async fn store_transaction_result(
        &self,
        tx: TransactionResult,
        with_signatures: bool,
    ) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        self.store_transaction_result_with_batch(&mut batch, tx, with_signatures)?;
        self.write_batch(batch)
            .await
            .map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }

    fn store_transaction_result_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        tx: TransactionResult,
        with_signatures: bool,
    ) -> Result<(), StorageError> {
        let mut skip_signatures = !with_signatures;
        for ix in tx.instruction_results {
            if let Err(e) = self.store_instruction_result_with_batch(batch, ix) {
                skip_signatures = true;
                tracing::error!("Failed to store instruction result: {}", e);
            }
        }
        if !skip_signatures && tx.transaction_signature.is_some() {
            let (pk, signature) = tx.transaction_signature.unwrap();
            self.persist_signature_with_batch(batch, pk, signature)?;
        }
        Ok(())
    }

    fn store_instruction_result_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        ix: InstructionResult,
    ) -> Result<(), StorageError> {
        if let Some(update) = ix.update {
            self.save_changelog_with_batch(batch, &update.event, update.slot);
            if let Some(dyn_data) = update.update {
                if let Err(e) = self.save_tx_data_and_asset_updated_with_batch(
                    batch,
                    dyn_data.pk,
                    dyn_data.slot,
                    dyn_data.leaf,
                    dyn_data.dynamic_data,
                ) {
                    tracing::error!("Failed to save tx data and asset updated: {}", e);
                }
            }
            if let Some(static_update) = update.static_update {
                if let Err(e) = self.asset_static_data.merge_with_batch(
                    batch,
                    static_update.pk,
                    &static_update.details,
                ) {
                    tracing::error!("Failed to merge asset static data: {}", e);
                }
            }
            if let Some(owner_update) = update.owner_update {
                if let Err(e) = self.asset_owner_data.merge_with_batch(
                    batch,
                    owner_update.pk,
                    &owner_update.details,
                ) {
                    tracing::error!("Failed to merge asset owner data: {}", e);
                }
            }
            if let Some(authority_update) = update.authority_update {
                if let Err(e) = self.asset_authority_data.merge_with_batch(
                    batch,
                    authority_update.pk,
                    &authority_update.details,
                ) {
                    tracing::error!("Failed to merge asset authority data: {}", e);
                }
            }
            if let Some(collection_update) = update.collection_update {
                if let Err(e) = self.asset_collection_data.merge_with_batch(
                    batch,
                    collection_update.pk,
                    &collection_update.details,
                ) {
                    tracing::error!("Failed to merge asset collection data: {}", e);
                }
            }
        }
        //todo: this doesn't seem to be a correct way to handle this, as delete will have no effect after any "late" tx ingestion
        if let Some(decompressed) = ix.decompressed {
            self.asset_leaf_data
                .delete_with_batch(batch, decompressed.pk);
            if let Err(e) = self.asset_dynamic_data.merge_with_batch(
                batch,
                decompressed.pk,
                &decompressed.details,
            ) {
                tracing::error!("Failed to save tx data and asset updated: {}", e);
            }
        }
        if let Some(tree_update) = ix.tree_update {
            self.save_tree_with_batch(batch, tree_update);
        }

        Ok(())
    }

    fn persist_signature_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        program_id: Pubkey,
        signature: SignatureWithSlot,
    ) -> Result<(), StorageError> {
        let slot = signature.slot;
        let signature = signature.signature;
        Self::put_with_batch::<SignatureIdx>(
            self.db.clone(),
            batch,
            (program_id, slot, signature),
            &SignatureIdx {},
        )
        .map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }
}
