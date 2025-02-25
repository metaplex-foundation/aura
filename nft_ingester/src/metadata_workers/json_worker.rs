use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use entities::{
    enums::{OffchainDataMutability, TaskStatus},
    models::MetadataDownloadTask,
};
use interface::{
    error::{JsonDownloadErrors, JsonDownloaderError},
    json_metadata::{
        CacheControlResponse, JsonDownloadResult, JsonDownloader, JsonPersister,
        MetadataDownloadResult,
    },
};
use metrics_utils::{red::RequestErrorDurationMetrics, JsonDownloaderMetricsConfig, MetricStatus};
use postgre_client::{tasks::UpdatedTask, PgClient};
use reqwest::{
    header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED},
    ClientBuilder,
};
use reqwest_middleware::ClientBuilder as MiddlewareClientBuilder;
use reqwest_retry::{policies::ExponentialBackoffBuilder, RetryTransientMiddleware};
use rocks_db::{
    columns::{
        asset_previews::UrlToDownload,
        offchain_data::{OffChainData, StorageMutability},
    },
    Storage,
};
use serde_json::Value;
use tokio::time::Duration;
use tokio_util::either::Either;
use tracing::{error, info};
use url::Url;

use crate::api::dapi::rpc_asset_convertors::parse_files;

pub struct JsonWorker {
    pub db_client: Arc<PgClient>,
    pub rocks_db: Arc<Storage>,
    pub num_of_parallel_workers: i32,
    pub should_skip_refreshes: bool,
    pub metrics: Arc<JsonDownloaderMetricsConfig>,
    pub red_metrics: Arc<RequestErrorDurationMetrics>,
}

impl JsonWorker {
    pub async fn new(
        db_client: Arc<PgClient>,
        rocks_db: Arc<Storage>,
        metrics: Arc<JsonDownloaderMetricsConfig>,
        red_metrics: Arc<RequestErrorDurationMetrics>,
        parallel_json_downloaders: i32,
        should_skip_refreshes: bool,
    ) -> Self {
        Self {
            db_client,
            num_of_parallel_workers: parallel_json_downloaders,
            should_skip_refreshes,
            metrics,
            red_metrics,
            rocks_db,
        }
    }
}

pub const MAX_RETRIES: u32 = 3;
pub const MIN_RETRY_INTERVAL: Duration = Duration::from_secs(1);
pub const MAX_RETRY_INTERVAL: Duration = Duration::from_secs(3);

#[async_trait]
impl JsonDownloader for JsonWorker {
    async fn download_file(
        &self,
        download_task: &MetadataDownloadTask,
        timeout: Duration,
    ) -> Result<MetadataDownloadResult, JsonDownloaderError> {
        let start_time = chrono::Utc::now();
        let retry_policy = ExponentialBackoffBuilder::default()
            .retry_bounds(MIN_RETRY_INTERVAL, MAX_RETRY_INTERVAL)
            .build_with_max_retries(MAX_RETRIES);

        let client = ClientBuilder::new().timeout(timeout).build().map_err(|e| {
            JsonDownloaderError::ErrorDownloading(JsonDownloadErrors::FailedToCreateClient(e))
        })?;
        let client = MiddlewareClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        // Detect if the URL is an IPFS link
        let parsed_url = if download_task.metadata_url.starts_with("ipfs://") {
            // Extract the IPFS hash or path
            let ipfs_path = download_task.metadata_url.trim_start_matches("ipfs://");
            // Choose an IPFS gateway (you can change this to your preferred gateway)
            let gateway_url = format!("https://ipfs.io/ipfs/{}", ipfs_path);
            // Parse the rewritten URL
            Url::parse(&gateway_url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(JsonDownloadErrors::FailedToParseIpfsUrl(e))
            })?
        } else {
            // Parse the original URL
            Url::parse(&download_task.metadata_url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(JsonDownloadErrors::FailedToParseUrl(e))
            })?
        };

        let host = parsed_url.host_str().unwrap_or("no_host");

        let mut request_builder = client.get(parsed_url.clone());

        if let Some(etag) = &download_task.etag {
            request_builder = request_builder.header(IF_NONE_MATCH, etag);
        };
        if let Some(last_modified_at) = &download_task.last_modified_at {
            request_builder = request_builder.header(IF_MODIFIED_SINCE, last_modified_at);
        };

        let response = request_builder.send().await.map_err(|e| {
            self.red_metrics.observe_error("json_downloader", "download_file", host);
            JsonDownloaderError::ErrorDownloading(JsonDownloadErrors::FailedToMakeRequest(e))
        })?;
        self.red_metrics.observe_request("json_downloader", "download_file", host, start_time);

        let etag = response
            .headers()
            .get(ETAG)
            .and_then(|etag| etag.to_str().ok().map(ToString::to_string));
        let last_modified = response
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|ct| ct.to_str().ok())
            .and_then(|date| date.parse().ok());
        let cache_control = response.headers().get(CACHE_CONTROL).and_then(|cache_control| {
            let cache_control = cache_control.to_str().ok().map(ToString::to_string);
            cache_control.map(|cache_control| parse_cache_control_response(&cache_control))
        });

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(MetadataDownloadResult::new(
                etag,
                last_modified,
                cache_control,
                JsonDownloadResult::NotModified,
            ));
        }

        if response.status() != reqwest::StatusCode::OK {
            return Err(JsonDownloaderError::ErrorStatusCode(response.status()));
        }

        // Get the Content-Type header
        let content_type =
            response.headers().get(CONTENT_TYPE).and_then(|ct| ct.to_str().ok()).unwrap_or("");

        // Excluded content types that are definitely not JSON
        let excluded_types = ["audio/", "application/octet-stream"];
        if excluded_types.iter().any(|&t| content_type.starts_with(t)) {
            return Err(JsonDownloaderError::GotNoJsonFile);
        }

        // Check if the content type is image or video
        if content_type.starts_with("image/") || content_type.starts_with("video/") {
            // Return the URL and MIME type

            return Ok(MetadataDownloadResult::new(
                etag,
                last_modified,
                cache_control,
                JsonDownloadResult::MediaUrlAndMimeType {
                    url: download_task.metadata_url.clone(),
                    mime_type: content_type.to_string(),
                },
            ));
        }

        let metadata_body = response.text().await;
        if let Ok(metadata) = metadata_body {
            // Attempt to parse the response as JSON
            if serde_json::from_str::<Value>(&metadata).is_ok() {
                return Ok(MetadataDownloadResult::new(
                    etag,
                    last_modified,
                    cache_control,
                    JsonDownloadResult::JsonContent(metadata),
                ));
            } else {
                return Err(JsonDownloaderError::CouldNotDeserialize);
            }
        } else {
            Err(JsonDownloaderError::CouldNotDeserialize)
        }
    }

    fn skip_refresh(&self) -> bool {
        self.should_skip_refreshes
    }
}

#[async_trait]
impl JsonPersister for JsonWorker {
    async fn persist_response(
        &self,
        results: Vec<(String, Result<MetadataDownloadResult, JsonDownloaderError>)>,
    ) -> Result<(), JsonDownloaderError> {
        let mut pg_updates = Vec::new();
        let mut pg_update_attempt_time = Vec::new();
        let mut rocks_updates = HashMap::new();
        let curr_time = chrono::Utc::now().timestamp();

        for (metadata_url, result) in results.into_iter() {
            match result {
                Ok(MetadataDownloadResult { result: JsonDownloadResult::NotModified, .. }) => {
                    pg_update_attempt_time.push(metadata_url.clone());
                    self.metrics.inc_tasks("json", MetricStatus::SUCCESS);
                },
                Ok(MetadataDownloadResult {
                    etag,
                    last_modified_at,
                    cache_control: _,
                    result: JsonDownloadResult::JsonContent(json_file),
                }) => {
                    let mutability: StorageMutability = metadata_url.as_str().into();
                    rocks_updates.insert(
                        metadata_url.clone(),
                        OffChainData {
                            storage_mutability: mutability.clone(),
                            url: Some(metadata_url.clone()),
                            metadata: Some(json_file.clone()),
                            last_read_at: curr_time,
                        },
                    );

                    pg_updates.push(UpdatedTask {
                        mutability: mutability.to_string(),
                        status: TaskStatus::Success,
                        metadata_url: metadata_url.clone(),
                        etag,
                        last_modified_at,
                    });

                    self.metrics.inc_tasks("json", MetricStatus::SUCCESS);
                },
                Ok(MetadataDownloadResult {
                    etag,
                    last_modified_at,
                    result: JsonDownloadResult::MediaUrlAndMimeType { url, mime_type },
                    cache_control: _,
                }) => {
                    let mutability = StorageMutability::Immutable;
                    rocks_updates.insert(
                        metadata_url.clone(),
                        OffChainData {
                            url: Some(metadata_url.clone()),
                            metadata: Some(
                                format!("{{\"image\":\"{}\",\"type\":\"{}\"}}", url, mime_type)
                                    .to_string(),
                            ),
                            last_read_at: curr_time,
                            storage_mutability: mutability.clone(),
                        },
                    );
                    pg_updates.push(UpdatedTask {
                        status: TaskStatus::Success,
                        etag,
                        last_modified_at,
                        metadata_url: metadata_url.clone(),
                        mutability: mutability.to_string(),
                    });

                    self.metrics.inc_tasks("media", MetricStatus::SUCCESS);
                },
                Err(json_err) => match json_err {
                    JsonDownloaderError::GotNoJsonFile => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            etag: None,
                            last_modified_at: None,
                            mutability: metadata_url.as_str().into(),
                        });
                        self.metrics.inc_tasks("media", MetricStatus::FAILURE);
                    },
                    JsonDownloaderError::CouldNotDeserialize => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            etag: None,
                            last_modified_at: None,
                            mutability: metadata_url.as_str().into(),
                        });
                        self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                    },
                    JsonDownloaderError::CouldNotReadHeader => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            etag: None,
                            last_modified_at: None,
                            mutability: metadata_url.as_str().into(),
                        });
                        self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                    },
                    //TODO: should we log the error status code?
                    JsonDownloaderError::ErrorStatusCode(err) => {
                        if err == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || err == reqwest::StatusCode::REQUEST_TIMEOUT
                        {
                            info!("Rate limited: {}", metadata_url);
                        } else {
                            pg_updates.push(UpdatedTask {
                                status: TaskStatus::Failed,
                                metadata_url: metadata_url.clone(),
                                etag: None,
                                last_modified_at: None,
                                mutability: metadata_url.as_str().into(),
                            });
                        }

                        self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                    },
                    JsonDownloaderError::ErrorDownloading(err) => {
                        if let JsonDownloadErrors::FailedToMakeRequest(err) = err {
                            info!("Wasn't able to download: {:?}", err);
                        } else {
                            pg_updates.push(UpdatedTask {
                                status: TaskStatus::Failed,
                                metadata_url: metadata_url.clone(),
                                etag: None,
                                last_modified_at: None,
                                mutability: metadata_url.as_str().into(),
                            });
                            self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                        }
                    },
                    _ => {}, // No additional processing needed
                },
            }
        }

        let process_regular_updates_fut = if pg_updates.is_empty() {
            Either::Left(async { Ok(()) })
        } else {
            Either::Right(self.db_client.update_tasks(pg_updates))
        };

        let process_not_modified_assets_fut = if pg_update_attempt_time.is_empty() {
            Either::Left(async { Ok(()) })
        } else {
            Either::Right(self.db_client.update_tasks_attempt_time(pg_update_attempt_time))
        };

        let process_offchain_data_fut = if rocks_updates.is_empty() {
            Either::Left(async { Ok(()) })
        } else {
            Either::Right(self.rocks_db.asset_offchain_data.put_batch(rocks_updates.clone()))
        };

        let process_urls_to_download_fut = if rocks_updates.is_empty() {
            Either::Left(async { Ok(()) })
        } else {
            let urls_to_download = rocks_updates
                .values()
                .filter(|data| {
                    data.metadata.is_some() && !data.metadata.clone().unwrap().is_empty()
                })
                .filter_map(|data| parse_files(data.metadata.clone().unwrap().as_str()))
                .flat_map(|files| files.into_iter())
                .filter_map(|file| file.uri)
                .map(|uri| (uri, UrlToDownload::default()))
                .collect::<HashMap<_, _>>();

            Either::Right(self.rocks_db.urls_to_download.put_batch(urls_to_download))
        };

        let (
            regular_updates_upd_result,
            not_modified_upd_result,
            offchain_data_upd_result,
            urls_to_download_upd_result,
        ) = tokio::join!(
            process_regular_updates_fut,
            process_not_modified_assets_fut,
            process_offchain_data_fut,
            process_urls_to_download_fut
        );

        for res in [regular_updates_upd_result, not_modified_upd_result] {
            res.map_err(|e| JsonDownloaderError::IndexStorageError(e.to_string()))?;
        }

        offchain_data_upd_result
            .map_err(|e| JsonDownloaderError::MainStorageError(e.to_string()))?;
        if let Err(e) = urls_to_download_upd_result {
            error!(error = %e, "Unable to persist URLs to download: {e}");
        };
        Ok(())
    }
}

fn parse_cache_control_response(cache_control: &str) -> CacheControlResponse {
    let mut publicity = None;
    let mut max_age = None;
    let mut mutability = None;

    for directive in cache_control.split(',') {
        let directive = directive.trim();
        if directive.starts_with("publicity=") {
            publicity = directive.trim_start_matches("publicity=").parse().ok();
        }
        if directive.starts_with("max-age=") {
            max_age = directive.trim_start_matches("max-age=").parse().ok();
        }
        if directive.starts_with("mutability=") {
            let mutability_word: Option<String> =
                directive.trim_start_matches("mutability=").parse().ok();
            if let Some(mutability_word) = mutability_word {
                match mutability_word.as_str() {
                    "immutable" => mutability = Some(OffchainDataMutability::Immutable),
                    "mutable" => mutability = Some(OffchainDataMutability::Mutable),
                    _ => {},
                }
            }
        }
    }

    CacheControlResponse { publicity, max_age, mutability }
}
