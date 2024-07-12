use std::collections::HashMap;

use anchor_lang::AnchorSerialize;
use entities::rollup::{RolledMintInstruction, Rollup};
use mpl_bubblegum::utils::get_asset_id;
use rollup_sdk::rollup_builder::{verify_signature, MetadataArgsHash};
use solana_sdk::{
    keccak::{self, Hash},
    pubkey::Pubkey,
    signature::Signature,
};

use usecase::error::RollupValidationError;

pub async fn validate_rollup(rollup: &Rollup) -> Result<(), RollupValidationError> {
    let mut leaf_hashes = Vec::new();
    for asset in rollup.rolled_mints.iter() {
        let leaf_hash = match get_leaf_hash(asset, &rollup.tree_id) {
            Ok(leaf_hash) => leaf_hash,
            Err(e) => {
                return Err(e);
            }
        };
        leaf_hashes.push(leaf_hash);

        verify_creators_signatures(
            &rollup.tree_id,
            asset,
            asset.creator_signature.clone().unwrap_or_default(),
        )?;
    }

    usecase::merkle_tree::validate_change_logs(
        rollup.max_depth,
        rollup.max_buffer_size,
        &leaf_hashes,
        rollup,
    )
}

fn verify_creators_signatures(
    tree_key: &Pubkey,
    rolled_mint: &RolledMintInstruction,
    creator_signatures: HashMap<Pubkey, Signature>,
) -> Result<(), RollupValidationError> {
    let metadata_hash =
        MetadataArgsHash::new(&rolled_mint.leaf_update, tree_key, &rolled_mint.mint_args);

    for creator in &rolled_mint.mint_args.creators {
        if creator.verified {
            if let Some(signature) = creator_signatures.get(&creator.address) {
                if !verify_signature(&creator.address, &metadata_hash.get_message(), signature) {
                    return Err(RollupValidationError::FailedCreatorVerification(
                        creator.address.to_string(),
                    ));
                }
            } else {
                return Err(RollupValidationError::MissingCreatorSignature(
                    creator.address.to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn get_leaf_hash(
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
