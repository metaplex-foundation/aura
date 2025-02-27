DROP INDEX IF EXISTS assets_v3_collection_is_collection_verified;

CREATE INDEX assets_v3_collection_is_collection_verified_supply ON assets_v3(ast_collection, ast_is_collection_verified, ast_supply) WHERE ast_collection IS NOT NULL AND ast_supply IS NOT NULL;