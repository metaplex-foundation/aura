use std::{future::Future, sync::Arc};

use futures::future::Either;
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::{
    futures_util::future,
    middleware::{NoopCallFuture, NoopFuture},
    Call, Failure, Metadata, Middleware, Output, Version,
};

#[derive(Default, Clone)]
pub struct RpcMetaMiddleware {
    consistency_checkers: Vec<Arc<dyn ConsistencyChecker>>,
}
impl Metadata for RpcMetaMiddleware {}

impl RpcMetaMiddleware {
    pub(crate) fn new(consistency_checkers: Vec<Arc<dyn ConsistencyChecker>>) -> Self {
        Self { consistency_checkers }
    }

    fn cannot_service_request() -> Option<Output> {
        Some(Output::Failure(Failure {
            jsonrpc: Some(Version::V2),
            error: crate::api::error::cannot_service_request_error(),
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
        if self.consistency_checkers.iter().any(|checker| checker.should_cancel_request(&call)) {
            return Either::Left(Box::pin(future::ready(Self::cannot_service_request())));
        }
        Either::Right(next(call, meta))
    }
}
