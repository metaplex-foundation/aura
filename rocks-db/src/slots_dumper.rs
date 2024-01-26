use crate::bubblegum_slots::{form_bubblegum_slots_key, BubblegumSlots};
use crate::Storage;
use async_trait::async_trait;
use interface::slots_dumper::SlotsDumper;
use std::collections::HashMap;
use std::time::Duration;
use tracing::error;

pub const PUT_SLOT_RETRIES: u32 = 5;
pub const SECONDS_TO_RETRY_ROCKSDB_OPERATION: u64 = 5;

#[async_trait]
impl SlotsDumper for Storage {
    async fn dump_slots(&self, slots: &[u64]) {
        tracing::info!("Saving {} slots", slots.len());
        let slots_map: HashMap<String, BubblegumSlots> = slots.iter().fold(
            HashMap::new(),
            |mut acc: HashMap<String, BubblegumSlots>, slot| {
                acc.insert(form_bubblegum_slots_key(*slot), BubblegumSlots {});
                acc
            },
        );

        if !slots_map.is_empty() {
            let mut counter = PUT_SLOT_RETRIES;
            while counter > 0 {
                let put_result = self.bubblegum_slots.put_batch(slots_map.clone()).await;

                match put_result {
                    Ok(_) => {
                        break;
                    }
                    Err(err) => {
                        error!("Error putting slots: {}", err);
                        counter -= 1;
                        tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_ROCKSDB_OPERATION))
                            .await;
                        continue;
                    }
                }
            }
        }
    }
}
