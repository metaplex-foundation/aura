use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use interface::api::APIJsonDownloaderMiddleware;
use jsonrpc_http_server::jsonrpc_core::futures::TryStreamExt;
use jsonrpc_http_server::{hyper, RequestMiddleware, RequestMiddlewareAction};
use log::info;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::codec::{BytesCodec, FramedRead};
use tonic::async_trait;

use crate::json_downloader::JsonDownloader;

const FULL_BACKUP_REQUEST_PATH: &str = "/snapshot";

pub struct RpcRequestMiddleware {
    pub archives_dir: String,
}

impl RpcRequestMiddleware {
    pub fn new(archives_dir: &str) -> Self {
        Self {
            archives_dir: archives_dir.to_string(),
        }
    }

    fn not_found() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(hyper::Body::empty())
            .unwrap()
    }

    #[allow(dead_code)]
    fn internal_server_error() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
            .body(hyper::Body::empty())
            .unwrap()
    }

    #[cfg(unix)]
    async fn open_no_follow(path: impl AsRef<Path>) -> io::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .await
    }

    fn get_last_backup(&self) -> io::Result<Option<PathBuf>> {
        let entries = std::fs::read_dir(self.archives_dir.clone())?;
        let mut last_created_time = std::time::SystemTime::UNIX_EPOCH;
        let mut last_created_backup: Option<PathBuf> = None;

        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_file() {
                let created_time = metadata.created()?;
                if created_time > last_created_time {
                    last_created_time = created_time;
                    last_created_backup = Some(entry.path());
                }
            }
        }
        Ok(last_created_backup)
    }

    fn process_file_get(&self, path: &str) -> RequestMiddlewareAction {
        let filename;
        match self.get_last_backup() {
            Ok(backup) => {
                if let Some(backup_path) = backup {
                    filename = backup_path
                } else {
                    return RequestMiddlewareAction::Respond {
                        should_validate_hosts: true,
                        response: Box::pin(async move { Ok(Self::not_found()) }),
                    };
                };
            }
            Err(_e) => {
                return RequestMiddlewareAction::Respond {
                    should_validate_hosts: true,
                    response: Box::pin(async move { Ok(Self::internal_server_error()) }),
                };
            }
        };

        let file_length = std::fs::metadata(&filename)
            .map(|m| m.len())
            .unwrap_or_default()
            .to_string();
        info!("get {} -> {:?} ({} bytes)", path, filename, file_length);

        RequestMiddlewareAction::Respond {
            should_validate_hosts: true,
            response: Box::pin(async move {
                match Self::open_no_follow(&filename).await {
                    Err(err) => Ok(if err.kind() == std::io::ErrorKind::NotFound {
                        Self::not_found()
                    } else {
                        Self::internal_server_error()
                    }),
                    Ok(file) => {
                        let stream =
                            FramedRead::new(file, BytesCodec::new()).map_ok(|b| b.freeze());
                        let body = hyper::Body::wrap_stream(stream);

                        Ok(hyper::Response::builder()
                            .header(hyper::header::CONTENT_LENGTH, file_length)
                            .body(body)
                            .unwrap())
                    }
                }
            }),
        }
    }
}

impl RequestMiddleware for RpcRequestMiddleware {
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        if request.uri().path() == FULL_BACKUP_REQUEST_PATH {
            return self.process_file_get(request.uri().path());
        }

        request.into()
    }
}

pub struct JsonDownloaderMiddleware {
    json_downloader: Arc<JsonDownloader>,
    persist_response: bool,
    max_urls_to_process: u16,
}

impl JsonDownloaderMiddleware {
    pub fn new(
        json_downloader: Arc<JsonDownloader>,
        persist_response: bool,
        max_urls_to_process: u16,
    ) -> Self {
        Self {
            json_downloader,
            persist_response,
            max_urls_to_process,
        }
    }
}

#[async_trait]
impl APIJsonDownloaderMiddleware for JsonDownloaderMiddleware {
    async fn get_metadata(
        &self,
        metadata_urls: HashSet<String>,
    ) -> Result<HashMap<String, String>, String> {
        if metadata_urls.len() > self.max_urls_to_process as usize {
            return Err("Too many urls to download".to_string());
        }

        let tasks = self
            .json_downloader
            .db_client
            .get_tasks_by_url(metadata_urls.into_iter().collect())
            .await
            .map_err(|e| e.to_string())?;

        let result = Arc::new(Mutex::new(HashMap::new()));

        let mut tasks_set = JoinSet::new();

        for task in tasks.iter() {
            let json_downloader = self.json_downloader.clone();
            let persist_response = self.persist_response;
            let task = task.clone();
            let result = result.clone();
            tasks_set.spawn(async move {
                let response = JsonDownloader::download_file(task.metadata_url.clone()).await;

                let url = task.metadata_url.clone();

                let metadata = {
                    if persist_response {
                        JsonDownloader::persist_response(
                            response,
                            task,
                            &json_downloader.db_client,
                            &json_downloader.rocks_db,
                            &json_downloader.metrics,
                        )
                        .await
                    } else {
                        response.ok()
                    }
                };

                if let Some(json) = metadata {
                    let mut res = result.lock().await;

                    res.insert(url, json);
                }
            });
        }

        while tasks_set.join_next().await.is_some() {}

        let r = result.lock().await.clone();

        Ok(r)
    }
}
