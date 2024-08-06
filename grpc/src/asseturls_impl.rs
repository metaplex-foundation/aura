use crate::asseturls::asset_url_service_server::AssetUrlService;
use crate::asseturls::url_download_details::DlResult;
use crate::asseturls::{
    AssetsToDownload, DownloadError, DownloadResultsRequest, DownloadSuccess, GetAssetUrlsRequest,
    UrlDownloadDetails,
};
use interface::assert_urls::{DownloadOutcome, UrlDownloadNotification, UrlsToDownloadStore};
use std::sync::Arc;
use tonic::{async_trait, Status};

pub struct AssetUrlServiceImpl {
    asset_url_store: Arc<dyn UrlsToDownloadStore>,
}

impl AssetUrlServiceImpl {
    pub fn new(asset_url_store: Arc<dyn UrlsToDownloadStore>) -> Self {
        AssetUrlServiceImpl { asset_url_store }
    }
}

#[async_trait]
impl AssetUrlService for AssetUrlServiceImpl {
    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn get_asset_urls_to_download(
        &self,
        request: tonic::Request<GetAssetUrlsRequest>,
    ) -> std::result::Result<tonic::Response<AssetsToDownload>, tonic::Status> {
        match self
            .asset_url_store
            .get_urls_to_download(request.get_ref().count)
        {
            Ok(urls) => Ok(tonic::Response::new(AssetsToDownload { urls })),
            Err(e) => Err(Status::internal(format!("Internal error: {}", e))),
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    async fn submit_download_result(
        &self,
        request: tonic::Request<DownloadResultsRequest>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let notifications = request
            .into_inner()
            .results
            .into_iter()
            .map(|url_download_details| url_download_details.into())
            .collect::<Vec<_>>();

        match self.asset_url_store.submit_download_results(notifications) {
            Ok(_) => Ok(tonic::Response::new(())),
            Err(e) => Err(Status::internal(format!("Internal error: {}", e))),
        }
    }
}

impl From<UrlDownloadDetails> for UrlDownloadNotification {
    fn from(UrlDownloadDetails { url, dl_result }: UrlDownloadDetails) -> Self {
        let outcome = dl_result
            .map(|dl_res| dl_res.into())
            .unwrap_or(DownloadOutcome::Nothing);

        UrlDownloadNotification { url, outcome }
    }
}

impl From<DlResult> for DownloadOutcome {
    fn from(value: DlResult) -> Self {
        match value {
            DlResult::Success(DownloadSuccess { mime, size }) => {
                DownloadOutcome::Success { mime, size }
            }
            DlResult::Fail(fail) => match DownloadError::try_from(fail) {
                Ok(download_error) => match download_error {
                    DownloadError::NotFound => DownloadOutcome::NotFound,
                    DownloadError::ServerError => DownloadOutcome::ServerError,
                    DownloadError::NotSupportedFormat => DownloadOutcome::NotSupportedFormat,
                    DownloadError::TooLarge => DownloadOutcome::TooLarge,
                    DownloadError::TooManyRequests => DownloadOutcome::TooManyRequests,
                    DownloadError::CorruptedAsset => DownloadOutcome::CorruptedAsset,
                },
                Err(_) => DownloadOutcome::Nothing,
            },
        }
    }
}
