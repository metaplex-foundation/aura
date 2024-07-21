use crate::{PgClient, INSERT_ACTION, SQL_COMPONENT};
use chrono::Utc;
use entities::models::CoreFee;
use sqlx::QueryBuilder;

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
}
