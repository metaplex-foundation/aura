use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use crate::column::TypedColumn;
use crate::errors::StorageError;
use crate::transaction::TreeUpdate;
use crate::{Result, Storage};
use bincode::{deserialize, serialize};
use rocksdb::MergeOperands;
use solana_sdk::pubkey::Pubkey;
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signature;
use tracing::error;

/// This column family contains sequence updates for each leaf in the tree.
/// Key is a set of `Signature+TreeId+leafId`.
/// Value is a hash map with slots and sequences.
/// It has such a complex value because during forks same
/// transaction can appear in different slots.
/// Even more different slots may contain updates with different sequences.
/// Such structure allows to detect this situations and handle it properly.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LeafSignature {
    pub data: HashMap<u64, HashSet<u64>>,
}

impl TypedColumn for LeafSignature {
    type KeyType = (Signature, Pubkey, u64);
    type ValueType = Self;

    const NAME: &'static str = "LEAF_SIGNATURE"; // Name of the column family

    fn encode_key(index: Self::KeyType) -> Vec<u8> {
        let (sig, pubkey, leaf_idx) = index;
        let signature_size = bincode::serialized_size(&sig).unwrap() as usize;
        let pubkey_size = std::mem::size_of::<Pubkey>();
        let leaf_idx_size = std::mem::size_of::<u64>();
        let mut key = Vec::with_capacity(signature_size + pubkey_size + leaf_idx_size);
        let sig = bincode::serialize(&sig).unwrap();
        key.extend_from_slice(sig.as_slice());
        key.extend_from_slice(&pubkey.to_bytes());
        key.extend_from_slice(&leaf_idx.to_be_bytes());
        key
    }

    fn decode_key(bytes: Vec<u8>) -> Result<Self::KeyType> {
        let signature_size = std::mem::size_of::<Signature>();
        let pubkey_size = std::mem::size_of::<Pubkey>();
        let leaf_idx_size = std::mem::size_of::<u64>();
        if bytes.len() != signature_size + pubkey_size + leaf_idx_size {
            return Err(crate::StorageError::InvalidKeyLength);
        }
        let sig = Signature::try_from(&bytes[..signature_size])?;
        let pubkey = Pubkey::try_from(&bytes[signature_size..signature_size + pubkey_size])?;
        let leaf_idx = u64::from_be_bytes(bytes[signature_size + pubkey_size..].try_into()?);
        Ok((sig, pubkey, leaf_idx))
    }
}

impl LeafSignature {
    pub fn merge_leaf_signatures(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut final_map = HashMap::new();

        if let Some(existing_val) = existing_val {
            match deserialize::<LeafSignature>(existing_val) {
                Ok(value) => {
                    final_map = value.data;
                }
                Err(e) => {
                    error!("RocksDB: LeafSignature deserialize existing_val: {}", e)
                }
            }
        }

        for op in operands {
            match deserialize::<LeafSignature>(op) {
                Ok(new_val) => {
                    for (slot, sequences) in new_val.data {
                        if let Some(seq) = final_map.get_mut(&slot) {
                            seq.extend(sequences);
                        } else {
                            final_map.insert(slot, sequences);
                        }
                    }
                }
                Err(e) => {
                    error!("RocksDB: LeafSignature deserialize new_val: {}", e);
                }
            }
        }

        match serialize(&LeafSignature{data: final_map}) {
            Ok(serialized_data) => Some(serialized_data),
            Err(e) => {
                error!("RocksDB: error serializing final merge result for LeafSignature: {:?}", e);
                Some(vec![])
            }
        }
    }
}

impl Storage {
    pub(crate) fn save_leaf_signature_with_batch(
        &self,
        batch: &mut rocksdb::WriteBatch,
        tree: &TreeUpdate,
    ) -> Result<()> {
        let mut sequence_set = HashSet::new();
        sequence_set.insert(tree.seq);
        let mut slot_sequence_map = HashMap::new();
        slot_sequence_map.insert(tree.slot, sequence_set);

        let signature = Signature::from_str(&tree.tx).map_err(|e| StorageError::Common(format!("RocksDB: could not convert tree.tx into Signature: {}", e)))?;

        if let Err(e) = self.leaf_signature.merge_with_batch(
            batch,
            (signature, tree.tree, tree.event.index as u64),
            &LeafSignature {data: slot_sequence_map},
        ) {
            error!("Error while saving tree update: {}", e);
        };

        Ok(())
    }
}