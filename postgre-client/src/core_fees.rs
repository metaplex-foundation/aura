use crate::{PgClient, BATCH_SELECT_ACTION, INSERT_ACTION, SQL_COMPONENT};
use chrono::Utc;
use entities::models::{CoreFee, CoreFeesAccount, CoreFeesAccountWithSortingID};
use solana_sdk::pubkey::Pubkey;
use sqlx::{Postgres, QueryBuilder, Row};

impl PgClient {
    pub async fn save_core_fees(&self, fees: Vec<CoreFee>) -> Result<(), String> {
        if fees.is_empty() {
            return Ok(());
        }
        let start_time = Utc::now();
        let mut query_builder = QueryBuilder::new(
            "INSERT INTO core_fees (
                fee_pubkey,
                fee_paid,
                fee_current_balance,
                fee_minimum_rent,
                fee_slot_updated
            ) ",
        );
        query_builder.push_values(fees, |mut builder, core_fee| {
            builder
                .push_bind(core_fee.pubkey.to_bytes().to_vec())
                .push_bind(core_fee.is_paid)
                .push_bind(core_fee.current_balance as i64)
                .push_bind(core_fee.minimum_rent as i64)
                .push_bind(core_fee.slot_updated as i64);
        });
        query_builder.push(" ON CONFLICT (fee_pubkey) DO UPDATE SET fee_paid = EXCLUDED.fee_paid, fee_slot_updated = EXCLUDED.fee_slot_updated WHERE core_fees.fee_slot_updated < EXCLUDED.fee_slot_updated;");
        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|err| {
            self.metrics
                .observe_error(SQL_COMPONENT, INSERT_ACTION, "core_fees");
            format!("Insert core fees: {}", err)
        })?;

        self.metrics
            .observe_request(SQL_COMPONENT, INSERT_ACTION, "core_fees", start_time);

        Ok(())
    }

    pub async fn get_core_fees(
        &self,
        limit: u64,
        page: Option<u64>,
        before: Option<String>,
        after: Option<String>,
    ) -> Result<Vec<CoreFeesAccountWithSortingID>, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT fee_pubkey, fee_current_balance, fee_minimum_rent, fee_slot_updated FROM core_fees WHERE not fee_paid  ",
        );
        let order_reversed = before.is_some() && after.is_none();
        if let Some(before) = before {
            let comparison = if order_reversed { " > " } else { " < " };
            add_slot_and_key_comparison(before.as_ref(), comparison, &mut query_builder)?;
        }
        if let Some(after) = after {
            let comparison = if order_reversed { " < " } else { " > " };
            add_slot_and_key_comparison(after.as_ref(), comparison, &mut query_builder)?;
        }
        query_builder.push(" ORDER BY fee_slot_updated ");
        if order_reversed {
            query_builder.push(" DESC ")
        } else {
            query_builder.push(" ASC ")
        };

        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit as i64);
        if let Some(page_num) = page {
            if page_num > 0 {
                let offset = (page_num.saturating_sub(1)) * limit; // Prevent underflow
                query_builder.push(" OFFSET ");
                query_builder.push_bind(offset as i64);
            }
        }
        query_builder.push(";");
        let query = query_builder.build();
        let start_time = chrono::Utc::now();
        let result = query.fetch_all(&self.pool).await.map_err(|err| {
            self.metrics
                .observe_error(SQL_COMPONENT, BATCH_SELECT_ACTION, "core_fees");
            err.to_string()
        })?;
        self.metrics
            .observe_request(SQL_COMPONENT, BATCH_SELECT_ACTION, "core_fees", start_time);
        Ok(result
            .iter()
            .map(|row| {
                let slot = row.get::<i64, _>("fee_slot_updated");
                let pubkey = Pubkey::try_from(row.get::<Vec<u8>, _>("fee_pubkey")).unwrap();
                CoreFeesAccountWithSortingID::from((
                    pubkey.to_bytes().to_vec().as_slice(),
                    slot,
                    CoreFeesAccount {
                        address: pubkey.to_string(),
                        current_balance: row.get::<i64, _>("fee_current_balance"),
                        minimum_rent: row.get::<i64, _>("fee_minimum_rent"),
                    },
                ))
            })
            .collect::<Vec<_>>())
    }
}

fn add_slot_and_key_comparison(
    key: &str,
    comparison: &str,
    query_builder: &mut QueryBuilder<'_, Postgres>,
) -> Result<(), String> {
    let res = crate::asset_filter_client::decode_sorting_key(key);

    if let Ok((slot, pubkey)) = res {
        query_builder.push(format!(" AND (fee_slot_updated {}", comparison));
        query_builder.push_bind(slot);
        query_builder.push(" OR (fee_slot_updated = ");
        query_builder.push_bind(slot);
        query_builder.push(format!(" AND fee_pubkey {}", comparison));
        query_builder.push_bind(pubkey);
        query_builder.push("))");
    }

    Ok(())
}
