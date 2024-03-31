use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;

pub struct BackfillingStateConsistencyChecker {}

impl BackfillingStateConsistencyChecker {
    pub fn new() -> Self {
        Self {}
    }
}

impl ConsistencyChecker for BackfillingStateConsistencyChecker {
    fn should_cancel_request(&self, _call: &Call) -> bool {
        false
    }
}
