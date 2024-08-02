CREATE TABLE core_fees (
   fee_pubkey bytea NOT NULL PRIMARY KEY,
   fee_paid bool NOT NULL DEFAULT false,
   fee_current_balance bigint NOT NULL DEFAULT 0,
   fee_minimum_rent bigint NOT NULL DEFAULT 0,
   fee_slot_updated bigint NOT NULL DEFAULT 0
);
CREATE INDEX fee_paid_slot_index ON core_fees(fee_pubkey, fee_slot_updated) where not fee_paid;
