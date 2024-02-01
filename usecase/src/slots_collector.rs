use crate::bigtable::SECONDS_TO_RETRY_GET_DATA_FROM_BG;
use async_trait::async_trait;
use interface::error::UsecaseError;
use interface::slots_dumper::SlotsDumper;
use metrics_utils::{BackfillerMetricsConfig, MetricStatus};
use mockall::automock;
use solana_bigtable_connection::bigtable::BigTableConnection;
use solana_sdk::clock::Slot;
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tracing::{error, info};

pub const GET_SIGNATURES_LIMIT: i64 = 2000;

#[automock]
#[async_trait]
pub trait RowKeysGetter {
    async fn get_row_keys(
        &self,
        table_name: &str,
        start_at: Option<String>,
        end_at: Option<String>,
        rows_limit: i64,
    ) -> Result<Vec<String>, UsecaseError>;
}

#[async_trait]
impl RowKeysGetter for BigTableConnection {
    async fn get_row_keys(
        &self,
        table_name: &str,
        start_at: Option<String>,
        end_at: Option<String>,
        rows_limit: i64,
    ) -> Result<Vec<String>, UsecaseError> {
        self.client()
            .get_row_keys(table_name, start_at, end_at, rows_limit)
            .await
            .map_err(|e| UsecaseError::Bigtable(e.to_string()))
    }
}

pub struct SlotsCollector<T, R>
where
    T: SlotsDumper + Sync + Send,
    R: RowKeysGetter + Sync + Send,
{
    slots_dumper: Arc<T>,
    row_keys_getter: Arc<R>,
    metrics: Arc<BackfillerMetricsConfig>,
}

impl<T, R> SlotsCollector<T, R>
where
    T: SlotsDumper + Sync + Send,
    R: RowKeysGetter + Sync + Send,
{
    pub fn new(
        slots_dumper: Arc<T>,
        row_keys_getter: Arc<R>,
        metrics: Arc<BackfillerMetricsConfig>,
    ) -> Self {
        SlotsCollector {
            slots_dumper,
            row_keys_getter,
            metrics,
        }
    }

    pub async fn collect_slots(
        &self,
        collected_pubkey: &str,
        slot_start_from: u64,
        slot_parse_until: u64,
        rx: &Receiver<()>,
    ) -> Option<u64> {
        let mut start_at_slot = slot_start_from;
        info!(
            "Collecting slots for {} starting from {} until {}",
            collected_pubkey, start_at_slot, slot_parse_until
        );
        let mut top_slot_collected = None;
        loop {
            if !rx.is_empty() {
                info!("Received stop signal, returning");
                return None;
            }
            let slots = self
                .row_keys_getter
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
                                slots.push(slot);

                                if top_slot_collected.is_none() {
                                    top_slot_collected = Some(slot);
                                }
                                if slot <= slot_parse_until {
                                    break;
                                }
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
                            || (last_slot <= slot_parse_until)
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
        top_slot_collected
    }

    fn slot_to_row(&self, prefix: &str, slot: Slot) -> String {
        let slot = !slot;
        format!("{}{slot:016x}", prefix)
    }

    fn row_to_slot(&self, prefix: &str, key: &str) -> Result<Slot, ParseIntError> {
        Slot::from_str_radix(&key[prefix.len()..], 16).map(|s| !s)
    }
}
