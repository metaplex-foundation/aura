CREATE TYPE royalty_target_type AS ENUM (
	'unknown',
	'creators',
	'fanout',
	'single'
);
CREATE TYPE mutability AS ENUM (
    'unknown',
    'mutable',
    'immutable'
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
    'identity_nft'
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

CREATE TABLE pubkeys (
	pbk_id bigserial NOT NULL,
	pbk_key bytea NOT NULL,
	CONSTRAINT pubkeys_pkey PRIMARY KEY (pbk_id),
	CONSTRAINT pubkeys_pubkey_key UNIQUE (pbk_key)
);

CREATE TABLE assets (
	ast_pubkey bigint NOT NULL,
    ast_owner bigint,
	ast_delegate bigint,
	ast_authority bigint,
	ast_collection bigint,
	ast_is_collection_verified bool NOT NULL DEFAULT false,
	ast_collection_seq bigint,
    ast_is_compressible bool NOT NULL DEFAULT false,
	ast_is_compressed bool NOT NULL DEFAULT false,
	ast_is_frozen bool NOT NULL DEFAULT false,
    ast_supply bigint,
	ast_seq bigint,
	ast_tree_id bigint,
	ast_leaf bytea,
	ast_nonce bigint,
	ast_royalty_target_type royalty_target_type NOT NULL DEFAULT 'creators'::royalty_target_type,
	ast_royalty_target bigint,
	ast_royalty_amount bigint NOT NULL DEFAULT 0,
	ast_created_at timestamptz NULL DEFAULT (now() AT TIME ZONE 'utc'::text),
	ast_is_burnt bool NOT NULL DEFAULT false,
	ast_slot_updated bigint,
	ast_data_hash bpchar(200),
	ast_creator_hash bpchar(200),
	ast_owner_delegate_seq bigint,
	ast_was_decompressed bool NOT NULL DEFAULT false,
	ast_leaf_seq bigint,
    ast_specification_version specification_versions DEFAULT 'v1'::specification_versions not null,
    ast_specification_asset_class specification_asset_class,
    ast_owner_type owner_type not null default 'single',
	ast_onchain_data text,
    ast_supply_slot_updated bigint,
	CONSTRAINT assets_pkey PRIMARY KEY (ast_pubkey),
    FOREIGN KEY (ast_pubkey) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (ast_owner) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (ast_delegate) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (ast_authority) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (ast_collection) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (ast_royalty_target) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE
);
CREATE INDEX asset_owner ON assets(ast_owner);
CREATE INDEX asset_delegate ON assets(ast_delegate);
CREATE INDEX asset_authority ON assets(ast_authority);
CREATE INDEX asset_collection ON assets(ast_collection);
CREATE INDEX asset_supply ON assets USING btree (ast_supply);

CREATE TABLE offchain_data (
	ofd_pubkey bigint NOT NULL,
	ofd_metadata_url varchar(200) NOT NULL,
	ofd_metadata text NOT NULL DEFAULT 'processing'::text,
    ofd_chain_data_mutability mutability not null default 'mutable'::mutability,
	ofd_status task_status NOT NULL,
    ofd_locked_until timestamptz NULL DEFAULT (now() AT TIME ZONE 'utc'::text),
    ofd_attempts int2 NOT NULL DEFAULT 0,
    ofd_max_attempts int2 NOT NULL DEFAULT 10,
    ofd_error text,
	CONSTRAINT offchain_data_pkey PRIMARY KEY (ofd_pubkey),
	FOREIGN KEY(ofd_pubkey) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE
);
CREATE INDEX offchain_data_task_status ON offchain_data (ofd_status);
CREATE INDEX offchain_data_locked_until ON offchain_data (ofd_locked_until);

CREATE TABLE asset_creators (
	asc_id bigserial NOT NULL,
    asc_asset bigint NOT NULL,
    asc_creator bigint NOT NULL,
    asc_share int4 NOT NULL,
    asc_verified bool NOT NULL,
    asc_seq bigint NOT NULL,
    asc_slot_updated bigint NOT NULL,
    asc_position bigint NOT NULL,
    CONSTRAINT asset_creators_pkey PRIMARY KEY (asc_id),
    FOREIGN KEY(asc_asset) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY(asc_creator) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE
);
CREATE UNIQUE INDEX asset_creators_asset_creator ON asset_creators (asc_asset, asc_creator);
CREATE INDEX asset_creators_asset ON asset_creators(asc_asset);
CREATE INDEX asset_creators_creator ON asset_creators(asc_creator);

CREATE TABLE cl_items (
	cli_id bigserial NOT NULL,
	cli_tree bigint NOT NULL,
	cli_node_idx int8 NOT NULL,
	cli_leaf_idx int8 NULL,
	cli_seq int8 NOT NULL,
	cli_level int8 NOT NULL,
	cli_hash bytea NOT NULL,
	CONSTRAINT cl_items_pkey PRIMARY KEY (cli_id),
	FOREIGN KEY(cli_tree) REFERENCES pubkeys(pbk_id) ON DELETE RESTRICT ON UPDATE CASCADE
);
CREATE UNIQUE INDEX cl_items__tree_node ON cl_items (cli_tree, cli_node_idx);
CREATE INDEX cl_items_hash_idx ON cl_items (cli_hash);
CREATE INDEX cl_items_leaf_idx ON cl_items (cli_leaf_idx);
CREATE INDEX cl_items_level ON cl_items (cli_level);
CREATE INDEX cl_items_node_idx ON cl_items (cli_node_idx);

CREATE TABLE bubblegum_slots (
	bbs_slot int8 NOT NULL,
	bbs_status int4 NULL DEFAULT 0,
	CONSTRAINT bubblegum_slots_pkey PRIMARY KEY (bbs_slot)
);
CREATE INDEX bubblegum_slots_status ON bubblegum_slots (bbs_status);