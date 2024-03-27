use crate::api::service::Sequences;
use interface::consistency_check::ConsistencyChecker;
use jsonrpc_core::Call;
use std::sync::atomic::Ordering;

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

#[derive(Default)]
pub struct SynchronizationStateConsistencyChecker {
    sequences: Option<Sequences>,
}

impl SynchronizationStateConsistencyChecker {
    pub(crate) fn new(sequences: Option<Sequences>) -> Self {
        Self { sequences }
    }
}

impl ConsistencyChecker for SynchronizationStateConsistencyChecker {
    fn should_cancel_request(&self, call: &Call) -> bool {
        if self.sequences.as_ref().map_or(true, |sequences| {
            sequences
                .last_primary_storage_seq
                .load(Ordering::SeqCst)
                .saturating_sub(sequences.last_index_storage_seq.load(Ordering::SeqCst))
                < sequences.synchronization_api_threshold
        }) {
            return false;
        }

        return match call {
            Call::MethodCall(method_call) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&method_call.method.as_str())
            }
            Call::Notification(notification) => {
                INDEX_STORAGE_DEPENDS_METHODS.contains(&notification.method.as_str())
            }
            _ => false,
        };
    }
}
