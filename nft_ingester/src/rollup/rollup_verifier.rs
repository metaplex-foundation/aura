use anchor_lang::AnchorSerialize;
use entities::rollup::{RolledMintInstruction, Rollup};
use mpl_bubblegum::utils::get_asset_id;
use solana_sdk::{
    keccak::{self, Hash},
    pubkey::Pubkey,
};

use crate::{error::RollupValidationError, tree_macros::validate_change_logs};

pub struct RollupVerifier;

impl RollupVerifier {
    pub async fn validate_rollup(&self, rollup: &Rollup) -> Result<(), RollupValidationError> {
        let mut leaf_hashes = Vec::new();
        for asset in rollup.rolled_mints.iter() {
            let leaf_hash = match self.get_leaf_hash(asset, &rollup.tree_id) {
                Ok(leaf_hash) => leaf_hash,
                Err(e) => {
                    return Err(e);
                }
            };
            leaf_hashes.push(leaf_hash);
        }

        validate_change_logs(
            rollup.max_depth,
            rollup.max_buffer_size,
            &leaf_hashes,
            rollup,
        )
    }

    pub fn get_leaf_hash(
        &self,
        asset: &RolledMintInstruction,
        tree_id: &Pubkey,
    ) -> Result<[u8; 32], RollupValidationError> {
        let asset_id = get_asset_id(tree_id, asset.leaf_update.nonce());
        if asset_id != asset.leaf_update.id() {
            return Err(RollupValidationError::PDACheckFail(
                asset_id.to_string(),
                asset.leaf_update.id().to_string(),
            ));
        }

        // @dev: seller_fee_basis points is encoded twice so that it can be passed to marketplace
        // instructions, without passing the entire, un-hashed MetadataArgs struct
        let metadata_args_hash = keccak::hashv(&[asset.mint_args.try_to_vec()?.as_slice()]);
        let data_hash = keccak::hashv(&[
            &metadata_args_hash.to_bytes(),
            &asset.mint_args.seller_fee_basis_points.to_le_bytes(),
        ]);
        if asset.leaf_update.data_hash() != data_hash.to_bytes() {
            return Err(RollupValidationError::InvalidDataHash(
                data_hash.to_string(),
                Hash::new(asset.leaf_update.data_hash().as_slice()).to_string(),
            ));
        }

        // Use the metadata auth to check whether we can allow `verified` to be set to true in the
        // creator Vec.
        let creator_data = asset
            .mint_args
            .creators
            .iter()
            .map(|c| [c.address.as_ref(), &[c.verified as u8], &[c.share]].concat())
            .collect::<Vec<_>>();

        // Calculate creator hash.
        let creator_hash = keccak::hashv(
            creator_data
                .iter()
                .map(|c| c.as_slice())
                .collect::<Vec<&[u8]>>()
                .as_ref(),
        );
        if asset.leaf_update.creator_hash() != creator_hash.to_bytes() {
            return Err(RollupValidationError::InvalidCreatorsHash(
                creator_hash.to_string(),
                Hash::new(asset.leaf_update.creator_hash().as_slice()).to_string(),
            ));
        }

        Ok(asset.leaf_update.hash())
    }
}
