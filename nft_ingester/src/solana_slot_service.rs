use std::{sync::Arc, time::Duration};

use rocks_db::storage_consistency::{
    current_estimated_epoch, epoch_of_slot, slots_to_next_epoch, update_estimated_epoch,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::consistency_calculator::ConsistencyCalcMsg;

const SLOT_TIME: u64 = 400;

/// Background job that monitors current solana slot number,
/// and notifies downstream components about epoch change.
/// This component is an __optional__ part of p2p consistency checking mechanis.
pub struct SolanaSlotService {
    rpc_client: Arc<RpcClient>,
    epoch_changed_notifier: tokio::sync::mpsc::UnboundedSender<ConsistencyCalcMsg>,
}

const MIN_LAG_TO_CHECK_SLOT: u64 = 60;

impl SolanaSlotService {
    async fn run_epoch_changed_notifier(&self) {
        loop {
            let estimated_epoch = current_estimated_epoch();

            let Ok(solana_slot) = self.rpc_client.get_slot().await else {
                tokio::time::sleep(Duration::from_secs(MIN_LAG_TO_CHECK_SLOT)).await;
                continue;
            };
            let current_epoch = epoch_of_slot(solana_slot);

            if current_epoch > estimated_epoch {
                update_estimated_epoch(current_epoch);
                let _ = self
                    .epoch_changed_notifier
                    .send(ConsistencyCalcMsg::EpochChanged {
                        epoch: current_epoch,
                    });
            } else {
                let wait_more_slots = slots_to_next_epoch(solana_slot);
                let mut seconds_to_wait = (wait_more_slots * SLOT_TIME) / 1000;
                if seconds_to_wait < MIN_LAG_TO_CHECK_SLOT {
                    seconds_to_wait += MIN_LAG_TO_CHECK_SLOT;
                }
                tokio::time::sleep(Duration::from_secs(seconds_to_wait)).await;
            }
        }
    }
}
