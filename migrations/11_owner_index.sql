DROP INDEX IF EXISTS assets_v3_owner;
CREATE INDEX IF NOT EXISTS idx_assets_v3_owner_pubkey ON assets_v3 (ast_owner, ast_pubkey DESC);
