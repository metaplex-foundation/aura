-- this file contains the SQL statements to create the tables and indexes for the v3 schema, where postgres acts as a secondary index for the Solana blockchain data.
-- it uses the same types as v2, so it's not duplicated here.
-- TODO: add data/indexes needed for other API endpoints and not only search assets

CREATE TYPE royalty_target_type AS ENUM (
	'unknown',
	'creators',
	'fanout',
	'single'
);
CREATE TYPE specification_versions AS ENUM (
    'unknown',
    'v0',
    'v1',
    'v2'
);
CREATE TYPE specification_asset_class AS ENUM (
    'unknown',
    'fungible_token',
    'fungible_asset',
    'nft',
    'printable_nft',
    'print',
    'transfer_restricted_nft',
    'non_transferable_nft',
    'identity_nft',
    'programmable_nft',
    'mpl_core_asset',
    'mpl_core_collection'
);
CREATE TYPE owner_type AS ENUM (
    'unknown',
    'token',
    'single'
);
CREATE TYPE task_status AS ENUM (
	'pending',
	'running',
	'success',
	'failed'
);

CREATE TABLE asset_creators_v3 (
    asc_pubkey bytea NOT NULL,
    asc_creator bytea NOT NULL,
    asc_verified bool NOT NULL,
	asc_slot_updated bigint NOT NULL, -- not really the updated slot of the creator, but of an asset, is used to upsert the row properly
    CONSTRAINT asset_creators_v3_pkey PRIMARY KEY (asc_pubkey, asc_creator)
);
CREATE INDEX asset_creators_v3_creator ON asset_creators_v3(asc_creator, asc_verified);

CREATE TABLE tasks (
    tsk_id bytea
        CONSTRAINT tasks_pk
            PRIMARY KEY,
    tsk_metadata_url text NOT NULL,
    tsk_status task_status NOT NULL,
    tsk_locked_until timestamptz NULL DEFAULT (now() AT TIME ZONE 'utc'::text),
    tsk_attempts int2 NOT NULL DEFAULT 0,
    tsk_max_attempts int2 NOT NULL DEFAULT 10,
    tsk_error text
);
CREATE UNIQUE INDEX tasks_metadata_url ON tasks (tsk_metadata_url);
CREATE INDEX tasks_status ON tasks (tsk_status);
CREATE INDEX tasks_locked_until ON tasks (tsk_locked_until);

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

CREATE TABLE last_synced_key (
    id integer NOT NULL PRIMARY KEY DEFAULT 1,
    last_synced_asset_update_key bytea,
	constraint only_one_row check (id = 1)
);

-- Insert an initial row (assuming there's no last_synced_key initially)
INSERT INTO last_synced_key (last_synced_asset_update_key) VALUES (null);