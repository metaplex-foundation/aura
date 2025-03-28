use async_trait::async_trait;
use entities::models::SignatureWithSlot;
use interface::error::StorageError;
use solana_sdk::pubkey::Pubkey;

use crate::{
    asset::{AssetCompleteDetails, SourcedAssetLeaf},
    column::TypedColumn,
    parameters,
    parameters::Parameter,
    signature_client::SignatureIdx,
    transaction::{InstructionResult, TransactionResult, TransactionResultPersister},
    Storage, ToFlatbuffersConverter,
};

#[async_trait]
impl TransactionResultPersister for Storage {
    async fn store_block(&self, slot: u64, txs: &[TransactionResult]) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatchWithTransaction::<false>::default();
        for tx in txs {
            self.store_transaction_result_with_batch(&mut batch, tx, false, true).await?;
        }
        self.merge_top_parameter_with_batch(&mut batch, Parameter::LastBackfilledSlot, slot)
            .map_err(|e| StorageError::Common(e.to_string()))?;
        self.write_batch(batch).await.map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }
}

impl Storage {
    pub async fn store_transaction_result(
        &self,
        tx: &TransactionResult,
        with_signatures: bool,
        is_from_finalized_source: bool,
    ) -> Result<(), StorageError> {
        let mut batch = rocksdb::WriteBatch::default();
        self.store_transaction_result_with_batch(
            &mut batch,
            tx,
            with_signatures,
            is_from_finalized_source,
        )
        .await?;
        self.write_batch(batch).await.map_err(|e| StorageError::Common(e.to_string()))?;
        Ok(())
    }

    async fn store_transaction_result_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        tx: &TransactionResult,
        with_signatures: bool,
        is_from_finalized_source: bool,
    ) -> Result<(), StorageError> {
        let mut skip_signatures = !with_signatures;
        for ix in tx.instruction_results.iter() {
            if let Err(e) =
                self.store_instruction_result_with_batch(batch, ix, is_from_finalized_source)
            {
                skip_signatures = true;
                tracing::error!("Failed to store instruction result: {}", e);
            }
        }
        if let Some((pk, signature)) = tx.transaction_signature {
            if let Err(e) =
                self.merge_top_parameter(parameters::Parameter::TopSeenSlot, signature.slot).await
            {
                tracing::error!("Failed to store the ingested slot: {}", e);
            }
            if !skip_signatures {
                self.persist_signature_with_batch(batch, pk, signature)?;
            }
        }
        Ok(())
    }

    fn store_instruction_result_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        ix: &InstructionResult,
        is_from_finalized_source: bool,
    ) -> Result<(), StorageError> {
        if let Some(ref update) = ix.update {
            let pk = update
                .static_update
                .as_ref()
                .map(|s| s.pk)
                .or(update.update.as_ref().map(|u| u.pk))
                .or(update.owner_update.as_ref().map(|o| o.pk))
                .or(update.authority_update.as_ref().map(|a| a.pk))
                .or(update.collection_update.as_ref().map(|c| c.pk));
            if let Some(pk) = pk {
                let acd = AssetCompleteDetails {
                    pubkey: pk,
                    static_details: update.static_update.as_ref().map(|s| s.details.clone()),
                    dynamic_details: update.update.as_ref().and_then(|u| u.dynamic_data.clone()),
                    owner: update.owner_update.as_ref().map(|o| o.details.clone()),
                    authority: update.authority_update.as_ref().map(|a| a.details.clone()),
                    collection: update.collection_update.as_ref().map(|c| c.details.clone()),
                };
                let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(2500);
                let acd = acd.convert_to_fb(&mut builder);
                builder.finish_minimal(acd);
                batch.merge_cf(
                    &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                    pk,
                    builder.finished_data(),
                );
                if let Some(leaf) = update.update.as_ref().and_then(|u| u.leaf.as_ref()) {
                    let leaf = SourcedAssetLeaf { leaf: leaf.clone(), is_from_finalized_source };
                    self.asset_leaf_data.merge_with_batch_raw(
                        batch,
                        pk,
                        bincode::serialize(&leaf)
                            .map_err(|e| StorageError::Common(e.to_string()))?,
                    )?;
                };
                if let Some(slot) = update.update.as_ref().map(|u| u.slot) {
                    self.asset_updated_with_batch(batch, slot, pk)?;
                }
            }

            if let Some(ref offchain_data) = update.offchain_data_update {
                if let Err(e) = self.asset_offchain_data.merge_with_batch(
                    batch,
                    offchain_data.url.clone().expect("Url should not be empty"),
                    offchain_data,
                ) {
                    tracing::error!("Failed to merge offchain data: {}", e);
                }
            }
        }
        //todo: this doesn't seem to be a correct way to handle this, as delete will have no effect after any "late" tx ingestion
        if let Some(ref decompressed) = ix.decompressed {
            self.asset_leaf_data.delete_with_batch(batch, decompressed.pk);
            let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(2500);
            let acd = decompressed.details.convert_to_fb(&mut builder);
            builder.finish_minimal(acd);
            batch.merge_cf(
                &self.db.cf_handle(AssetCompleteDetails::NAME).unwrap(),
                decompressed.pk,
                builder.finished_data(),
            );
        }
        if let Some(ref tree_update) = ix.tree_update {
            self.save_changelog_with_batch(
                batch,
                &tree_update.event,
                tree_update.slot,
                is_from_finalized_source,
            );
            self.save_tree_with_batch(batch, tree_update);
            self.save_asset_signature_with_batch(batch, tree_update);
            self.save_leaf_signature_with_batch(batch, tree_update)?;
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
