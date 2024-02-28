ALTER TYPE specification_asset_class ADD VALUE 'programmable_nft';
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE assets_v3;

DELETE FROM tasks WHERE tsk_metadata_url = '';

ALTER TABLE tasks
ADD COLUMN tsk_id_hash bytea;

UPDATE tasks
SET tsk_id_hash = digest(tsk_metadata_url, 'sha256');

-- Ensure there are no null hashes
ALTER TABLE tasks ALTER COLUMN tsk_id_hash SET NOT NULL;

ALTER TABLE tasks DROP CONSTRAINT tasks_pk;
-- Add the primary key
ALTER TABLE tasks ADD CONSTRAINT tsk_id_hash_pkey PRIMARY KEY (tsk_id_hash);


ALTER TABLE tasks DROP COLUMN tsk_id;

ALTER TABLE tasks RENAME COLUMN tsk_id_hash TO tsk_id;


CREATE TABLE assets_v3 (
	ast_pubkey bytea NOT NULL,
	ast_specification_version specification_versions DEFAULT 'v1'::specification_versions not null,
	ast_specification_asset_class specification_asset_class,
	ast_royalty_target_type royalty_target_type NOT NULL DEFAULT 'creators'::royalty_target_type,
	ast_royalty_amount bigint NOT NULL DEFAULT 0,
	ast_slot_created bigint NOT NULL,
	ast_owner bytea,
	ast_owner_type owner_type,
	ast_delegate bytea,
	ast_authority bytea,
	ast_collection bytea,
	ast_is_collection_verified bool,
	ast_is_burnt bool NOT NULL DEFAULT false,
	ast_is_compressible bool NOT NULL DEFAULT false,
	ast_is_compressed bool NOT NULL DEFAULT false,
	ast_is_frozen bool NOT NULL DEFAULT false,
	ast_supply bigint,
	ast_metadata_url_id bytea NULL,
	ast_slot_updated bigint NOT NULL,
	CONSTRAINT assets_pkey PRIMARY KEY (ast_pubkey),
    FOREIGN KEY (ast_metadata_url_id) REFERENCES tasks(tsk_id) ON DELETE RESTRICT ON UPDATE CASCADE
);
-- indexes on the fields that will not get updated:
-- so far we only know of V1 specification versions, so we only index on others
CREATE INDEX assets_v3_specification_version ON assets_v3 (ast_specification_version) WHERE ast_specification_version <> 'v1'::specification_versions;
CREATE INDEX assets_v3_specification_asset_class ON assets_v3 (ast_specification_asset_class) WHERE ast_specification_asset_class IS NOT NULL AND ast_specification_asset_class <> 'unknown'::specification_asset_class;
-- so far we only know of creators as royalty target type, so we only index on others
CREATE INDEX assets_v3_royalty_target_type ON assets_v3 (ast_royalty_target_type) WHERE ast_royalty_target_type <> 'creators'::royalty_target_type;
-- so far I've seen only null royalty targets, this should be verified
CREATE INDEX assets_v3_royalty_amount ON assets_v3 (ast_royalty_amount);
CREATE INDEX assets_v3_slot_created ON assets_v3 (ast_slot_created);
CREATE INDEX assets_v3_owner_type ON assets_v3 (ast_owner_type) WHERE ast_owner_type IS NOT NULL AND ast_owner_type <> 'unknown'::owner_type;

-- indexes on the fields that will get updated:
-- a conditional index on ast_metadata_url_id where ast_metadata_url_id is not null
CREATE INDEX assets_v3_metadata_url ON assets_v3 (ast_metadata_url_id) WHERE ast_metadata_url_id IS NOT NULL;
CREATE INDEX assets_v3_owner ON assets_v3(ast_owner) WHERE ast_owner IS NOT NULL;
CREATE INDEX assets_v3_delegate ON assets_v3(ast_delegate) WHERE ast_delegate IS NOT NULL;
CREATE INDEX assets_v3_authority ON assets_v3(ast_authority) WHERE ast_authority IS NOT NULL;
CREATE INDEX assets_v3_collection_is_collection_verified ON assets_v3(ast_collection, ast_is_collection_verified) WHERE ast_collection IS NOT NULL;
-- as 95% of assets are not burnt, we only index on burnt assets
CREATE INDEX assets_v3_is_burnt ON assets_v3(ast_is_burnt) WHERE ast_is_burnt IS TRUE;
-- all observed assets are not compressible, so we only index on compressible assets
CREATE INDEX assets_v3_is_compressible ON assets_v3(ast_is_compressible) WHERE ast_is_compressible IS TRUE;
CREATE INDEX assets_v3_is_compressed ON assets_v3(ast_is_compressed);
-- all observed assets are not frozen, so we only index on frozen assets
CREATE INDEX assets_v3_is_frozen ON assets_v3(ast_is_frozen) WHERE ast_is_frozen IS TRUE;

CREATE INDEX assets_v3_supply ON assets_v3(ast_supply) WHERE ast_supply IS NOT NULL;
CREATE INDEX assets_v3_slot_updated ON assets_v3(ast_slot_updated);

UPDATE last_synced_key SET last_synced_asset_update_key = null WHERE id = 1;
