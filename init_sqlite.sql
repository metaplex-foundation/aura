-- Enum-like behavior using CHECK constraints or separate reference tables

-- Creating tables without ENUM but with CHECK constraints for text fields
CREATE TABLE asset_creators_v3 (
    asc_pubkey BLOB NOT NULL,
    asc_creator BLOB NOT NULL,
    asc_verified BOOLEAN NOT NULL,
    asc_slot_updated INTEGER NOT NULL,
    PRIMARY KEY (asc_pubkey, asc_creator)
);

CREATE TABLE tasks (
    tsk_id BLOB PRIMARY KEY,
    tsk_metadata_url TEXT NOT NULL,
    tsk_status TEXT NOT NULL CHECK (tsk_status IN ('pending', 'running', 'success', 'failed')),
    tsk_locked_until TEXT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    tsk_attempts INTEGER NOT NULL DEFAULT 0,
    tsk_max_attempts INTEGER NOT NULL DEFAULT 10,
    tsk_error TEXT
);

CREATE TABLE assets_v3 (
    ast_pubkey BLOB NOT NULL,
    ast_specification_version TEXT DEFAULT 'v1' NOT NULL CHECK (ast_specification_version IN ('unknown', 'v0', 'v1', 'v2')),
    ast_specification_asset_class TEXT CHECK (ast_specification_asset_class IN ('unknown', 'fungible_token', 'fungible_asset', 'nft', 'printable_nft', 'print', 'transfer_restricted_nft', 'non_transferable_nft', 'identity_nft', 'programmable_nft')),
    ast_royalty_target_type TEXT NOT NULL DEFAULT 'creators' CHECK (ast_royalty_target_type IN ('unknown', 'creators', 'fanout', 'single')),
    ast_royalty_amount INTEGER NOT NULL DEFAULT 0,
    ast_slot_created INTEGER NOT NULL,
    ast_owner BLOB,
    ast_owner_type TEXT CHECK (ast_owner_type IN ('unknown', 'token', 'single')),
    ast_delegate BLOB,
    ast_authority BLOB,
    ast_collection BLOB,
    ast_is_collection_verified BOOLEAN,
    ast_is_burnt BOOLEAN NOT NULL DEFAULT 0,
    ast_is_compressible BOOLEAN NOT NULL DEFAULT 0,
    ast_is_compressed BOOLEAN NOT NULL DEFAULT 0,
    ast_is_frozen BOOLEAN NOT NULL DEFAULT 0,
    ast_supply INTEGER,
    ast_metadata_url_id BLOB NULL,
    ast_slot_updated INTEGER NOT NULL,
    PRIMARY KEY (ast_pubkey),
    FOREIGN KEY (ast_metadata_url_id) REFERENCES tasks(tsk_id) ON DELETE RESTRICT ON UPDATE CASCADE
);


CREATE TABLE last_synced_key (
    id INTEGER NOT NULL PRIMARY KEY DEFAULT 1,
    last_synced_asset_update_key BLOB,
    CONSTRAINT only_one_row CHECK (id = 1)
);

INSERT INTO last_synced_key(id) VALUES (1);
