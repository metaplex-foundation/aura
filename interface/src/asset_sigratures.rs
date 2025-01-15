use entities::{
    api_req_params::AssetSortDirection,
    models::{AssetSignature, AssetSignatureKey, AssetSignatureWithPagination},
};
use solana_program::pubkey::Pubkey;

pub trait AssetSignaturesGetter {
    #[allow(clippy::too_many_arguments)]
    fn signatures_iter(
        &self,
        tree: Pubkey,
        leaf_idx: u64,
        page: Option<u64>,
        before_sequence: Option<u64>,
        after_sequence: Option<u64>,
        direction: &AssetSortDirection,
        limit: u64,
    ) -> impl Iterator<Item = (AssetSignatureKey, AssetSignature)>;
    #[allow(clippy::too_many_arguments)]
    fn get_asset_signatures(
        &self,
        tree: Pubkey,
        leaf_idx: u64,
        before_sequence: Option<u64>,
        after_sequence: Option<u64>,
        page: Option<u64>,
        direction: AssetSortDirection,
        limit: u64,
    ) -> AssetSignatureWithPagination;
}
