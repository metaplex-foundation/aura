CREATE INDEX fungible_tokens_fbt_owner_fbt_asset_fbt_balance_idx ON fungible_tokens (fbt_owner, fbt_asset, fbt_balance);

CREATE INDEX assets_ast_owner_type_ast_pubkey_idx ON assets_v3 (ast_owner_type, ast_pubkey);