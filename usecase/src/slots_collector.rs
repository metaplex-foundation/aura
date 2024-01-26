use crate::bigtable::SECONDS_TO_RETRY_GET_DATA_FROM_BG;
use interface::slots_dumper::SlotsDumper;
use metrics_utils::{BackfillerMetricsConfig, MetricStatus};
use solana_bigtable_connection::bigtable::BigTableConnection;
use solana_sdk::clock::Slot;
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub const GET_SIGNATURES_LIMIT: i64 = 2000;

pub struct SlotsCollector<T>
where
    T: SlotsDumper,
{
    slots_dumper: Arc<T>,
    big_table_inner_client: Arc<BigTableConnection>,
    slot_start_from: u64,
    slot_parse_until: u64,
    metrics: Arc<BackfillerMetricsConfig>,
}

impl<T> SlotsCollector<T>
where
    T: SlotsDumper,
{
    pub fn new(
        slots_dumper: Arc<T>,
        big_table_inner_client: Arc<BigTableConnection>,
        slot_start_from: u64,
        slot_parse_until: u64,
        metrics: Arc<BackfillerMetricsConfig>,
    ) -> Self {
        SlotsCollector {
            slots_dumper,
            big_table_inner_client,
            slot_start_from,
            slot_parse_until,
            metrics,
        }
    }

    pub async fn collect_slots(&self, collected_pubkey: &str) {
        let mut start_at_slot = self.slot_start_from;
        info!(
            "Collecting slots starting from {} until {}",
            start_at_slot, self.slot_parse_until
        );

        loop {
            let slots = self
                .big_table_inner_client
                .client()
                .get_row_keys(
                    "tx-by-addr",
                    Some(self.slot_to_row(collected_pubkey, start_at_slot)),
                    Some(self.slot_to_row(collected_pubkey, Slot::MIN)),
                    GET_SIGNATURES_LIMIT,
                )
                .await;
            match slots {
                Ok(bg_slots) => {
                    self.metrics
                        .inc_slots_collected("backfiller_slots_collected", MetricStatus::SUCCESS);

                    let mut slots = Vec::new();
                    for slot in bg_slots.iter() {
                        let slot_value = self.row_to_slot(collected_pubkey, slot);
                        match slot_value {
                            Ok(slot) => {
                                slots.push(!slot);
                            }
                            Err(err) => {
                                error!(
                                    "Error while converting key received from BigTable to slot: {}",
                                    err
                                );
                            }
                        }
                    }

                    if !slots.is_empty() {
                        // safe to call unwrap because we checked that slots is not empty
                        let last_slot = *slots.last().unwrap();
                        self.slots_dumper.dump_slots(&slots).await;

                        self.metrics
                            .set_last_processed_slot("collected_slot", last_slot as i64);

                        if (slots.len() == 1 && slots[0] == start_at_slot)
                            || (last_slot < self.slot_parse_until)
                        {
                            info!("All the slots are collected");
                            break;
                        }
                        start_at_slot = last_slot;
                    } else {
                        info!("All the slots are collected");
                        break;
                    }
                }
                Err(err) => {
                    self.metrics
                        .inc_slots_collected("backfiller_slots_collected", MetricStatus::FAILURE);
                    error!("Error getting slots: {}", err);
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG))
                        .await;
                    continue;
                }
            }
        }
    }

    fn slot_to_row(&self, prefix: &str, slot: Slot) -> String {
        let slot = !slot;
        format!("{}{slot:016x}", prefix)
    }

    fn row_to_slot(&self, prefix: &str, key: &str) -> Result<Slot, ParseIntError> {
        Slot::from_str_radix(&key[prefix.len()..], 16)
    }
}
