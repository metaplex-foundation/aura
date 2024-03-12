
CREATE INDEX asset_creators_v3_creator ON asset_creators_v3(asc_creator, asc_verified);

CREATE UNIQUE INDEX tasks_metadata_url ON tasks (tsk_metadata_url);
CREATE INDEX tasks_status ON tasks (tsk_status);
CREATE INDEX tasks_locked_until ON tasks (tsk_locked_until);

-- Note: SQLite doesn't directly support partial indexes, so we'll create general indexes instead
CREATE INDEX assets_v3_specification_version ON assets_v3 (ast_specification_version);
CREATE INDEX assets_v3_specification_asset_class ON assets_v3 (ast_specification_asset_class);
CREATE INDEX assets_v3_royalty_target_type ON assets_v3 (ast_royalty_target_type);
CREATE INDEX assets_v3_royalty_amount ON assets_v3 (ast_royalty_amount);
CREATE INDEX assets_v3_slot_created ON assets_v3 (ast_slot_created);
CREATE INDEX assets_v3_owner_type ON assets_v3 (ast_owner_type);
CREATE INDEX assets_v3_metadata_url ON assets_v3 (ast_metadata_url_id);
CREATE INDEX assets_v3_owner ON assets_v3(ast_owner);
CREATE INDEX assets_v3_delegate ON assets_v3(ast_delegate);
CREATE INDEX assets_v3_authority ON assets_v3(ast_authority);
CREATE INDEX assets_v3_collection_is_collection_verified ON assets_v3(ast_collection, ast_is_collection_verified);
CREATE INDEX assets_v3_is_burnt ON assets_v3(ast_is_burnt);
CREATE INDEX assets_v3_is_compressible ON assets_v3(ast_is_compressible);
CREATE INDEX assets_v3_is_compressed ON assets_v3(ast_is_compressed);
CREATE INDEX assets_v3_is_frozen ON assets_v3(ast_is_frozen);
CREATE INDEX assets_v3_supply ON assets_v3(ast_supply);
CREATE INDEX assets_v3_slot_updated ON assets_v3(ast_slot_updated);
