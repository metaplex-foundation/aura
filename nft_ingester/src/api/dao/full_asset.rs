use entities::models::{EditionData, OffChainData};
use rocks_db::asset::{AssetCollection, AssetLeaf};
use rocks_db::{AssetAuthority, AssetDynamicDetails, AssetOwner, AssetStaticDetails};
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct FullAsset {
    pub asset_static: AssetStaticDetails,
    pub asset_owner: AssetOwner,
    pub asset_dynamic: AssetDynamicDetails,
    pub asset_leaf: AssetLeaf,
    pub offchain_data: OffChainData,
    pub asset_collections: HashMap<Pubkey, AssetCollection>,
    pub assets_authority: HashMap<Pubkey, AssetAuthority>,
    pub edition_data: Option<EditionData>,
}

pub struct FullAssetList {
    pub list: Vec<FullAsset>,
}
