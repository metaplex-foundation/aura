use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use entities::{enums::TaskStatus, models::MetadataDownloadTask};
use interface::{
    error::JsonDownloaderError,
    json::{JsonDownloadResult, JsonDownloader, JsonPersister, MetadataDownloadResult},
};
use metrics_utils::{red::RequestErrorDurationMetrics, JsonDownloaderMetricsConfig, MetricStatus};
use postgre_client::{tasks::UpdatedTask, PgClient};
use reqwest::ClientBuilder;
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
use tracing::error;
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
            JsonDownloaderError::ErrorDownloading(format!("Failed to create client: {:?}", e))
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
                JsonDownloaderError::ErrorDownloading(format!("Failed to parse IPFS URL: {:?}", e))
            })?
        } else {
            // Parse the original URL
            Url::parse(&download_task.metadata_url).map_err(|e| {
                JsonDownloaderError::ErrorDownloading(format!("Failed to parse URL: {:?}", e))
            })?
        };

        let host = parsed_url.host_str().unwrap_or("no_host");

        let mut request_builder = client.get(parsed_url.clone());
        match (download_task.etag.clone(), download_task.last_modified_at.clone()) {
            (Some(etag), Some(last_modified_at)) => {
                request_builder = request_builder.header("If-None-Match", etag);
                request_builder = request_builder.header("If-Modified-Since", last_modified_at);
            },
            (Some(etag), _) => {
                request_builder = request_builder.header("If-None-Match", etag);
            },
            (_, Some(last_modified_at)) => {
                request_builder = request_builder.header("If-Modified-Since", last_modified_at);
            },
            _ => {},
        }

        self.red_metrics.observe_request("json_downloader", "download_file", host, start_time);
        let response = request_builder.send().await.map_err(|e| {
            let msg = format!("Failed to make request: {:?}", e);
            self.red_metrics.observe_error("json_downloader", "download_file", host);
            JsonDownloaderError::ErrorDownloading(msg)
        })?;

        let etag = response
            .headers()
            .get("etag")
            .and_then(|etag| etag.to_str().ok().map(ToString::to_string));
        let last_modified: Option<DateTime<Utc>> =
            response.headers().get("last-modified").and_then(|ct| {
                ct.to_str().ok().map(|date| date.parse().expect("Wasn't able to parse the date"))
            });

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(MetadataDownloadResult::new(
                etag,
                last_modified,
                JsonDownloadResult::NotModified,
            ));
        }

        if response.status() != reqwest::StatusCode::OK {
            return Err(JsonDownloaderError::ErrorStatusCode(
                response.status().as_str().to_string(),
            ));
        }

        // Get the Content-Type header
        let content_type =
            response.headers().get("Content-Type").and_then(|ct| ct.to_str().ok()).unwrap_or("");

        // Excluded content types that are definitely not JSON
        let excluded_types = ["audio/", "application/octet-stream"];
        if excluded_types.iter().any(|&t| content_type.starts_with(t)) {
            return Err(JsonDownloaderError::GotNotJsonFile);
        }

        // Check if the content type is image or video
        if content_type.starts_with("image/") || content_type.starts_with("video/") {
            // Return the URL and MIME type

            return Ok(MetadataDownloadResult::new(
                etag,
                last_modified,
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
                }) => {
                    let mutability: StorageMutability = metadata_url.as_str().into();
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
                    JsonDownloaderError::GotNotJsonFile => {
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
                    JsonDownloaderError::ErrorStatusCode(_err) => {
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            etag: None,
                            last_modified_at: None,
                            mutability: metadata_url.as_str().into(),
                        });

                        self.metrics.inc_tasks("json", MetricStatus::FAILURE);
                    },
                    JsonDownloaderError::ErrorDownloading(_err) => {
                        // Revert to pending status to retry until max attempts
                        pg_updates.push(UpdatedTask {
                            status: TaskStatus::Failed,
                            metadata_url: metadata_url.clone(),
                            etag: None,
                            last_modified_at: None,
                            mutability: metadata_url.as_str().into(),
                        });
                        self.metrics.inc_tasks("unknown", MetricStatus::FAILURE);
                    },
                    _ => {}, // No additional processing needed
                },
            }
        }

        if !pg_updates.is_empty() {
            self.db_client
                .update_tasks(pg_updates)
                .await
                .map_err(|e| JsonDownloaderError::IndexStorageError(e.to_string()))?;
        }

        if pg_update_attempt_time.is_empty() {
            self.db_client
                .update_tasks_attempt_time(pg_update_attempt_time)
                .await
                .map_err(|e| JsonDownloaderError::IndexStorageError(e.to_string()))?;
        }

        if !rocks_updates.is_empty() {
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

            self.rocks_db
                .asset_offchain_data
                .put_batch(rocks_updates)
                .await
                .map_err(|e| JsonDownloaderError::MainStorageError(e.to_string()))?;

            if let Err(e) = self.rocks_db.urls_to_download.put_batch(urls_to_download).await {
                error!("Unable to persist URLs to download: {e}");
            };
        }

        Ok(())
    }
}
