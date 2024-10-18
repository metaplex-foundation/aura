DROP TABLE fungible_tokens;

-- recreate table and add fbt_pubkey

CREATE TABLE fungible_tokens (
    fbt_pubkey bytea NOT NULL,
    fbt_owner bytea NOT NULL,
    -- mint key
    fbt_asset bytea NOT NULL,
    fbt_balance bigint NOT NULL DEFAULT 0,
    fbt_slot_updated bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (fbt_pubkey)
);

CREATE INDEX fungible_tokens_fbt_owner_balance_idx ON fungible_tokens(fbt_owner, fbt_balance);

CREATE INDEX fungible_tokens_fbt_asset_idx ON fungible_tokens(fbt_asset);
