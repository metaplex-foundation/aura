ALTER TABLE fungible_tokens ADD COLUMN fbt_balance bigint NOT NULL DEFAULT 0;
ALTER TABLE fungible_tokens ADD COLUMN fbt_slot_updated bigint NOT NULL DEFAULT 0;
CREATE INDEX fungible_tokens_fbt_balance_idx ON fungible_tokens(fbt_balance) WHERE fbt_balance > 0;
CREATE INDEX fungible_tokens_fbt_slot_updated_idx ON fungible_tokens(fbt_slot_updated);
