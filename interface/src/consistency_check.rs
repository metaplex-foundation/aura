use jsonrpc_core::Call;

pub trait ConsistencyChecker: Send + Sync + 'static {
    fn should_cancel_request(&self, call: &Call) -> bool;
}
