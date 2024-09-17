use tokio::sync::broadcast::{Receiver, Sender};
use metrics_utils::MetricState;
use nft_ingester::config::IngesterConfig;
use crate::MutexedTasks;

struct NftIngester {
    mutexed_tasks: MutexedTasks,
    metrics_state: MetricState,
    config: IngesterConfig,
    shutdown_channel: (Sender<()>, Receiver<()>)
}