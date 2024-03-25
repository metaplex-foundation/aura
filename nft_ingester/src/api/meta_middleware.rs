use crate::api::service::MiddlewaresData;
use futures::future::Either;
use jsonrpc_core::futures_util::future;
use jsonrpc_core::middleware::{NoopCallFuture, NoopFuture};
use jsonrpc_core::{Call, ErrorCode, Failure, Metadata, Middleware, Output, Version};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const INDEX_STORAGE_DEPENDS_METHODS: &[&str] = &[
    "getAssetsByOwner",
    "get_assets_by_owner",
    "getAssetsByCreator",
    "get_assets_by_creator",
    "getAssetsByAuthority",
    "get_assets_by_authority",
    "getAssetsByGroup",
    "get_assets_by_group",
    "getGrouping",
    "get_grouping",
    "searchAssets",
    "search_assets",
];

pub const CANNOT_SERVICE_REQUEST_ERROR_CODE: i64 = -32050;

#[derive(Default, Clone)]
struct Sequences {
    last_primary_storage_seq: Arc<AtomicU64>,
    last_index_storage_seq: Arc<AtomicU64>,
    synchronization_api_threshold: u64,
}

#[derive(Default, Clone)]
pub struct RpcMetaMiddleware {
    sequences: Option<Sequences>,
}
impl Metadata for RpcMetaMiddleware {}

impl RpcMetaMiddleware {
    pub(crate) fn new(middlewares_data: &Option<MiddlewaresData>) -> Self {
        Self {
            sequences: middlewares_data.clone().map(|data| Sequences {
                last_primary_storage_seq: data.last_primary_storage_seq,
                last_index_storage_seq: data.last_index_storage_seq,
                synchronization_api_threshold: data.synchronization_api_threshold,
            }),
        }
    }

    fn cannot_service_request() -> Option<Output> {
        Some(Output::Failure(Failure {
            jsonrpc: Some(Version::V2),
            error: jsonrpc_core::types::error::Error {
                code: ErrorCode::ServerError(CANNOT_SERVICE_REQUEST_ERROR_CODE),
                message: "Cannot service request".to_string(),
                data: None,
            },
            id: jsonrpc_core::types::id::Id::Null,
        }))
    }
}

impl<M: Metadata> Middleware<M> for RpcMetaMiddleware {
    type Future = NoopFuture;
    type CallFuture = NoopCallFuture;

    fn on_call<F, X>(&self, call: Call, meta: M, next: F) -> Either<Self::CallFuture, X>
    where
        F: Fn(Call, M) -> X + Send + Sync,
        X: Future<Output = Option<Output>> + Send + 'static,
    {
        if self.sequences.as_ref().map_or(true, |sequences| {
            sequences
                .last_primary_storage_seq
                .load(Ordering::SeqCst)
                .saturating_sub(sequences.last_index_storage_seq.load(Ordering::SeqCst))
                < sequences.synchronization_api_threshold
        }) {
            return Either::Right(next(call, meta));
        }

        let should_cancel_request = match &call {
            Call::MethodCall(method_call) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&method_call.method.as_str())
            }
            Call::Notification(notification) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&notification.method.as_str())
            }
            _ => false,
        };

        if should_cancel_request {
            Either::Left(Box::pin(future::ready(Self::cannot_service_request())))
        } else {
            Either::Right(next(call, meta))
        }
    }
}
