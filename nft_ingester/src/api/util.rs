use entities::api_req_params::{
    AssetSorting, GetAssetsByAuthority, GetAssetsByCreator, GetAssetsByGroup, GetAssetsByOwner,
    GetByMethodsOptions, Pagination, SearchAssets,
};

pub trait ApiRequest {
    fn get_all_pagination_parameters(&self) -> Pagination;
    fn get_sort_parameter(&self) -> Option<AssetSorting>;
    fn get_options(&self) -> GetByMethodsOptions;
}

macro_rules! impl_request_with_pagination {
    ($struct_name:ident) => {
        impl ApiRequest for $struct_name {
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

            fn get_options(&self) -> GetByMethodsOptions {
                self.options.clone().into()
            }
        }
    };
}

impl_request_with_pagination!(GetAssetsByOwner);
impl_request_with_pagination!(GetAssetsByGroup);
impl_request_with_pagination!(GetAssetsByCreator);
impl_request_with_pagination!(GetAssetsByAuthority);
impl_request_with_pagination!(SearchAssets);
