use entities::api_req_params::{
    AssetSortBy, AssetSortDirection, AssetSorting, GetAsset, GetAssetProof, GetAssetsByAuthority,
    GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner,
};
use rand::Rng;

const GROUP_KEY: &str = "collection";
const MIN_LIMIT: u32 = 1;
const MAX_LIMIT: u32 = 1000;

fn get_random_asset_sorting_arg() -> Option<AssetSorting> {
    let mut rng = rand::thread_rng();
    if rng.gen() {
        return None;
    }

    Some(AssetSorting {
        sort_by: match rng.gen_range(0..4) {
            0 => AssetSortBy::Created,
            1 => AssetSortBy::Updated,
            2 => AssetSortBy::RecentAction,
            _ => AssetSortBy::None,
        },
        sort_direction: if rng.gen() {
            Some(if rng.gen() {
                AssetSortDirection::Asc
            } else {
                AssetSortDirection::Desc
            })
        } else {
            None
        },
    })
}

fn get_random_limit_arg() -> Option<u32> {
    let mut rng = rand::thread_rng();
    if rng.gen() {
        Some(rng.gen_range(MIN_LIMIT..=MAX_LIMIT))
    } else {
        None
    }
}

fn get_random_page_arg() -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen_range(1..=5)
}

pub fn generate_get_assets_by_group_params(
    group_value: String,
    before: Option<String>,
    after: Option<String>,
) -> GetAssetsByGroup {
    let page = if after.is_none() && before.is_none() {
        Some(get_random_page_arg())
    } else {
        None
    };
    GetAssetsByGroup {
        group_key: GROUP_KEY.to_string(),
        group_value,
        sort_by: get_random_asset_sorting_arg(),
        limit: get_random_limit_arg(),
        page,
        before,
        after,
        cursor: None,
        options: Default::default(),
    }
}

pub fn generate_get_assets_by_owner_params(
    owner_address: String,
    before: Option<String>,
    after: Option<String>,
) -> GetAssetsByOwner {
    let page = if after.is_none() && before.is_none() {
        Some(get_random_page_arg())
    } else {
        None
    };
    GetAssetsByOwner {
        owner_address,
        sort_by: get_random_asset_sorting_arg(),
        limit: get_random_limit_arg(),
        page,
        before,
        after,
        cursor: None,
        options: Default::default(),
    }
}

pub fn generate_get_assets_by_creator_params(
    creator_address: String,
    before: Option<String>,
    after: Option<String>,
) -> GetAssetsByCreator {
    let mut rng = rand::thread_rng();
    let page = if after.is_none() && before.is_none() {
        Some(get_random_page_arg())
    } else {
        None
    };
    GetAssetsByCreator {
        creator_address,
        only_verified: if rng.gen() { Some(rng.gen()) } else { None },
        sort_by: get_random_asset_sorting_arg(),
        limit: get_random_limit_arg(),
        page,
        before,
        after,
        cursor: None,
        options: Default::default(),
    }
}

pub fn generate_get_assets_by_authority_params(
    authority_address: String,
    before: Option<String>,
    after: Option<String>,
) -> GetAssetsByAuthority {
    let page = if after.is_none() && before.is_none() {
        Some(get_random_page_arg())
    } else {
        None
    };
    GetAssetsByAuthority {
        authority_address,
        sort_by: get_random_asset_sorting_arg(),
        limit: get_random_limit_arg(),
        page,
        before,
        after,
        cursor: None,
        options: Default::default(),
    }
}

pub fn generate_get_asset_params(id: String) -> GetAsset {
    GetAsset { id, options: Default::default() }
}

pub fn generate_get_asset_proof_params(id: String) -> GetAssetProof {
    GetAssetProof { id }
}
