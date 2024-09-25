CREATE TABLE fungible_tokens (
     fbt_owner bytea NOT NULL,
     fbt_asset bytea NOT NULL,
     PRIMARY KEY (fbt_owner, fbt_asset)
);
CREATE INDEX fungible_tokens_fbt_asset_idx ON fungible_tokens(fbt_asset);