use crate::{_check_proof, check_proof};
use async_trait::async_trait;
use interface::proofs::ProofChecker;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use spl_account_compression::canopy::fill_in_proof_from_canopy;
use spl_account_compression::state::{
    merkle_tree_get_size, ConcurrentMerkleTreeHeader, CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1,
};
use spl_account_compression::zero_copy::ZeroCopy;
use std::sync::Arc;

use anchor_lang::prelude::*;
use interface::error::IntegrityVerificationError;

/// MaybeProofChecker checks the proofs with the configured probability of the check to occur.
pub struct MaybeProofChecker {
    rpc_client: Arc<RpcClient>,
    check_probability: f64,
    commitment_level: CommitmentLevel,
}

impl MaybeProofChecker {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        check_probability: f64,
        commitment_level: CommitmentLevel,
    ) -> Self {
        Self {
            rpc_client,
            check_probability,
            commitment_level,
        }
    }
}

#[async_trait]
impl ProofChecker for MaybeProofChecker {
    async fn check_proof(
        &self,
        tree_id_pk: Pubkey,
        initial_proofs: Vec<Pubkey>,
        leaf_index: u32,
        leaf: [u8; 32],
    ) -> core::result::Result<bool, IntegrityVerificationError> {
        if rand::random::<f64>() > self.check_probability {
            return Ok(true);
        }
        let account_data = self
            .rpc_client
            .get_account_with_commitment(
                &tree_id_pk,
                CommitmentConfig {
                    commitment: self.commitment_level,
                },
            )
            .await;
        let tree_acc_info = account_data
            .map_err(IntegrityVerificationError::Rpc)?
            .value
            .map(|acc| acc.data);
        if tree_acc_info.is_none() {
            return Err(IntegrityVerificationError::TreeAccountNotFound(
                tree_id_pk.to_string(),
            ));
        }
        validate_proofs(tree_acc_info.unwrap(), initial_proofs, leaf_index, leaf)
    }
}

pub fn validate_proofs(
    mut tree_acc_info: Vec<u8>,
    initial_proofs: Vec<Pubkey>,
    leaf_index: u32,
    leaf: [u8; 32],
) -> core::result::Result<bool, IntegrityVerificationError> {
    let (header_bytes, rest) = tree_acc_info.split_at_mut(CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1);
    let header = ConcurrentMerkleTreeHeader::try_from_slice(header_bytes)?;
    let merkle_tree_size = merkle_tree_get_size(&header)?;
    let (tree_bytes, canopy_bytes) = rest.split_at_mut(merkle_tree_size);

    let mut initial_proofs = initial_proofs
        .iter()
        .map(|p| p.to_bytes())
        .collect::<Vec<_>>();
    fill_in_proof_from_canopy(
        canopy_bytes,
        header.get_max_depth(),
        leaf_index,
        &mut initial_proofs,
    )?;

    check_proof!(&header, &tree_bytes, initial_proofs, leaf, leaf_index)
}
