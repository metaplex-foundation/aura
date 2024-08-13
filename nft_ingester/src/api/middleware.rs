use jsonrpc_core::Output::Failure;
use jsonrpc_core::{ErrorCode, Response};
use std::io;
use std::path::{Path, PathBuf};

use crate::api::error::CANNOT_SERVICE_REQUEST_ERROR_CODE;
use jsonrpc_http_server::hyper::header::HeaderValue;
use jsonrpc_http_server::hyper::StatusCode;
use jsonrpc_http_server::jsonrpc_core::futures::TryStreamExt;
use jsonrpc_http_server::response_middleware::ResponseMiddleware;
use jsonrpc_http_server::{hyper, RequestMiddleware, RequestMiddlewareAction};
use tracing::info;
use serde_json::json;
use tokio_util::codec::{BytesCodec, FramedRead};

const FULL_BACKUP_REQUEST_PATH: &str = "/snapshot";

#[derive(Default, Clone)]
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

#[derive(Default, Clone)]
pub struct RpcResponseMiddleware {}

impl ResponseMiddleware for RpcResponseMiddleware {
    fn on_response(&self, response: Response) -> jsonrpc_http_server::Response {
        fn is_cannot_service_request_error(response: &Response) -> bool {
            match response {
                Response::Single(ref o) => matches!(o, Failure(ref f) if f.error.code == ErrorCode::ServerError(CANNOT_SERVICE_REQUEST_ERROR_CODE)),
                Response::Batch(ref os) => os.iter().any(|o| matches!(o, Failure(ref f) if f.error.code == ErrorCode::ServerError(CANNOT_SERVICE_REQUEST_ERROR_CODE))),
            }
        }
        if is_cannot_service_request_error(&response) {
            return Self::i_am_teapot(response);
        }

        jsonrpc_http_server::Response::ok(format!("{}\n", json!(response)))
    }
}

impl RpcResponseMiddleware {
    fn i_am_teapot(response: Response) -> jsonrpc_http_server::Response {
        jsonrpc_http_server::Response {
            code: StatusCode::IM_A_TEAPOT,
            content_type: HeaderValue::from_static("application/json; charset=utf-8"),
            content: format!("{}", json!(response)),
        }
    }
}
