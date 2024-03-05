use entities::api_req_params::{
    AssetSorting, GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner,
    Pagination, SearchAssets,
};

pub trait RequestWithPagination {
    fn get_all_pagination_parameters(&self) -> Pagination;
    fn get_sort_parameter(&self) -> Option<AssetSorting>;
}

impl RequestWithPagination for GetAssetsByOwner {
    fn get_all_pagination_parameters(&self) -> Pagination {
        Pagination {
            limit: self.limit,
            page: self.page,
            before: self.before.clone(),
            after: self.after.clone(),
            cursor: self.cursor.clone(),
        }
    }

    fn get_sort_parameter(&self) -> Option<AssetSorting> {
        self.sort_by.clone()
    }
}

impl RequestWithPagination for GetAssetsByGroup {
    fn get_all_pagination_parameters(&self) -> Pagination {
        Pagination {
            limit: self.limit,
            page: self.page,
            before: self.before.clone(),
            after: self.after.clone(),
            cursor: self.cursor.clone(),
        }
    }

    fn get_sort_parameter(&self) -> Option<AssetSorting> {
        self.sort_by.clone()
    }
}

impl RequestWithPagination for GetAssetsByCreator {
    fn get_all_pagination_parameters(&self) -> Pagination {
        Pagination {
            limit: self.limit,
            page: self.page,
            before: self.before.clone(),
            after: self.after.clone(),
            cursor: self.cursor.clone(),
        }
    }

    fn get_sort_parameter(&self) -> Option<AssetSorting> {
        self.sort_by.clone()
    }
}

impl RequestWithPagination for GetAssetsByAuthority {
    fn get_all_pagination_parameters(&self) -> Pagination {
        Pagination {
            limit: self.limit,
            page: self.page,
            before: self.before.clone(),
            after: self.after.clone(),
            cursor: self.cursor.clone(),
        }
    }

    fn get_sort_parameter(&self) -> Option<AssetSorting> {
        self.sort_by.clone()
    }
}

impl RequestWithPagination for SearchAssets {
    fn get_all_pagination_parameters(&self) -> Pagination {
        Pagination {
            limit: self.limit,
            page: self.page,
            before: self.before.clone(),
            after: self.after.clone(),
            cursor: self.cursor.clone(),
        }
    }

    fn get_sort_parameter(&self) -> Option<AssetSorting> {
        self.sort_by.clone()
    }
}
