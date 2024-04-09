use crate::error::IngesterError;
use anchor_lang::AnchorSerialize;
use entities::rollup::RolledMintInstruction;
use mpl_bubblegum::programs::MPL_BUBBLEGUM_ID;
use mpl_bubblegum::types::{LeafSchema, Version};
use mpl_bubblegum::utils::get_asset_id;
use mpl_bubblegum::LeafSchemaEvent;
use solana_program::keccak;
use solana_program::pubkey::Pubkey;

pub fn find_rollup_pda(tree_authority: &Pubkey, tree_nonce: u64) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            b"rollup",
            tree_authority.as_ref(),
            &tree_nonce.to_be_bytes(),
        ],
        &MPL_BUBBLEGUM_ID,
    )
}

pub fn create_leaf_schema(
    asset: &RolledMintInstruction,
    tree_id: &Pubkey,
) -> Result<LeafSchemaEvent, IngesterError> {
    // @dev: seller_fee_basis points is encoded twice so that it can be passed to marketplace
    // instructions, without passing the entire, un-hashed MetadataArgs struct
    let metadata_args_hash = keccak::hashv(&[asset.mint_args.try_to_vec()?.as_slice()]);
    let data_hash = keccak::hashv(&[
        &metadata_args_hash.to_bytes(),
        &asset.mint_args.seller_fee_basis_points.to_le_bytes(),
    ]);

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

    let asset_id = get_asset_id(tree_id, asset.nonce);
    if asset_id != asset.id {
        return Err(IngesterError::PDACheckFail(
            asset_id.to_string(),
            asset.id.to_string(),
        ));
    }
    let leaf = LeafSchema::V1 {
        id: asset.id,
        owner: asset.owner,
        delegate: asset.delegate,
        nonce: asset.nonce,
        data_hash: data_hash.to_bytes(),
        creator_hash: creator_hash.to_bytes(),
    };
    let leaf_hash = leaf.hash();

    Ok(LeafSchemaEvent::new(Version::V1, leaf, leaf_hash))
}
