ALTER TABLE fungible_tokens ADD COLUMN fbt_pubkey bytea NOT NULL;

ALTER TABLE fungible_tokens DROP CONSTRAINT fungible_tokens_pkey;

ALTER TABLE fungible_tokens ADD PRIMARY KEY (fbt_pubkey);

DROP INDEX IF EXISTS fungible_tokens_fbt_balance_idx;

DROP INDEX IF EXISTS fungible_tokens_fbt_slot_updated_idx;

CREATE INDEX fungible_tokens_fbt_owner_balance_idx ON fungible_tokens(fbt_owner, fbt_balance);

CREATE INDEX fungible_tokens_fbt_asset_idx ON fungible_tokens(fbt_asset);
