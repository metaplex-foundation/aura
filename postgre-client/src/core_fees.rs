use crate::{PgClient, BATCH_SELECT_ACTION, INSERT_ACTION, SQL_COMPONENT};
use chrono::Utc;
use entities::models::{CoreFee, CoreFeesAccount};
use solana_sdk::pubkey::Pubkey;
use sqlx::{QueryBuilder, Row};

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
        page: u64,
        limit: u64,
    ) -> Result<Vec<CoreFeesAccount>, String> {
        let mut query_builder = QueryBuilder::new(
            "SELECT fee_pubkey, fee_current_balance, fee_minimum_rent FROM core_fees WHERE not fee_paid ORDER BY fee_slot_updated",
        );
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit as i64);
        if page > 1 {
            query_builder.push(" OFFSET ");
            query_builder.push_bind((page as i64).saturating_sub(1) * limit as i64);
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
            .map(|row| CoreFeesAccount {
                address: Pubkey::try_from(row.get::<Vec<u8>, _>("fee_pubkey"))
                    .unwrap()
                    .to_string(),
                current_balance: row.get::<i64, _>("fee_current_balance"),
                minimum_rent: row.get::<i64, _>("fee_minimum_rent"),
            })
            .collect::<Vec<_>>())
    }
}
