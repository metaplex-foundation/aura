use std::{num::ParseIntError, sync::Arc, time::Duration};

use async_trait::async_trait;
use interface::{error::UsecaseError, slots_dumper::SlotsDumper};
use metrics_utils::{BackfillerMetricsConfig, MetricStatus};
use mockall::automock;
use solana_bigtable_connection::bigtable::BigTableConnection;
use solana_sdk::clock::Slot;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::bigtable::SECONDS_TO_RETRY_GET_DATA_FROM_BG;

pub const GET_SIGNATURES_LIMIT: i64 = 2000;

#[automock]
#[async_trait]
pub trait SlotsGetter {
    async fn get_slots_sorted_desc(
        &self,
        collected_key: &solana_program::pubkey::Pubkey,
        start_at: u64,
        rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError>;
}

#[async_trait]
impl SlotsGetter for BigTableConnection {
    async fn get_slots_sorted_desc(
        &self,
        collected_key: &solana_program::pubkey::Pubkey,
        start_at: u64,
        rows_limit: i64,
    ) -> Result<Vec<u64>, UsecaseError> {
        let key = format!("{}/", collected_key);
        self.client()
            .get_row_keys(
                "tx-by-addr",
                Some(slot_to_row(&key, start_at)),
                Some(slot_to_row(&key, Slot::MIN)),
                rows_limit,
            )
            .await
            .map_err(|e| UsecaseError::Bigtable(e.to_string()))
            .and_then(|bg_slots| {
                bg_slots
                    .iter()
                    .map(|slot| {
                        row_to_slot(&key, slot).map_err(|e| UsecaseError::Bigtable(e.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
    }
}

fn slot_to_row(prefix: &str, slot: Slot) -> String {
    let slot = !slot;
    format!("{}{slot:016x}", prefix)
}

fn row_to_slot(prefix: &str, key: &str) -> Result<Slot, ParseIntError> {
    Slot::from_str_radix(&key[prefix.len()..], 16).map(|s| !s)
}

pub struct SlotsCollector<T, R>
where
    T: SlotsDumper + Sync + Send,
    R: SlotsGetter + Sync + Send,
{
    slots_dumper: Arc<T>,
    row_keys_getter: Arc<R>,
    metrics: Arc<BackfillerMetricsConfig>,
}

impl<T, R> SlotsCollector<T, R>
where
    T: SlotsDumper + Sync + Send,
    R: SlotsGetter + Sync + Send,
{
    pub fn new(
        slots_dumper: Arc<T>,
        row_keys_getter: Arc<R>,
        metrics: Arc<BackfillerMetricsConfig>,
    ) -> Self {
        SlotsCollector { slots_dumper, row_keys_getter, metrics }
    }

    pub async fn collect_slots(
        &self,
        collected_pubkey: &solana_program::pubkey::Pubkey,
        slot_start_from: u64,
        slot_parse_until: u64,
        cancellation_token: CancellationToken,
    ) -> Option<u64> {
        let mut start_at_slot = slot_start_from;
        info!(
            "Collecting slots for {} starting from {} until {}",
            collected_pubkey, start_at_slot, slot_parse_until
        );
        let mut top_slot_collected = None;
        while !cancellation_token.is_cancelled() {
            let slots = self
                .row_keys_getter
                .get_slots_sorted_desc(collected_pubkey, start_at_slot, GET_SIGNATURES_LIMIT)
                .await;
            match slots {
                Ok(s) => {
                    self.metrics
                        .inc_slots_collected("backfiller_slots_collected", MetricStatus::SUCCESS);

                    let mut slots = Vec::new();
                    for slot in s.into_iter() {
                        slots.push(slot);

                        if top_slot_collected.is_none() {
                            top_slot_collected = Some(slot);
                        }
                        if slot <= slot_parse_until {
                            break;
                        }
                    }

                    if !slots.is_empty() {
                        // safe to call unwrap because we checked that slots is not empty
                        let last_slot = *slots.last().unwrap();
                        self.slots_dumper.dump_slots(&slots).await;

                        self.metrics.set_last_processed_slot("collected_slot", last_slot as i64);

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
                },
                Err(err) => {
                    self.metrics
                        .inc_slots_collected("backfiller_slots_collected", MetricStatus::FAILURE);
                    error!("Error getting slots: {}", err);
                    tokio::time::sleep(Duration::from_secs(SECONDS_TO_RETRY_GET_DATA_FROM_BG))
                        .await;
                    continue;
                },
            }
        }
        top_slot_collected
    }
}
