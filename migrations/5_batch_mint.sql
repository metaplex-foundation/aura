DROP TABLE IF EXISTS rollups;
DROP TYPE IF EXISTS rollup_state CASCADE;

CREATE TYPE batch_mint_state AS ENUM (
	'uploaded',
	'validation_fail',
    'validation_complete',
    'fail_upload_to_arweave',
    'uploaded_to_arweave',
    'fail_sending_transaction',
	'complete'
);

CREATE TABLE IF NOT EXISTS batch_mints (
    btm_file_name varchar(41) NOT NULL
    CONSTRAINT file_name_pk
    PRIMARY KEY,
    btm_state batch_mint_state NOT NULL,
    btm_error varchar(200),
    btm_url varchar(200),
    btm_tx_reward int8,
    btm_created_at timestamptz NOT NULL DEFAULT (now() AT TIME ZONE 'utc'::text)
);

CREATE INDEX IF NOT EXISTS batch_mints_state_created ON batch_mints (btm_state, btm_created_at);
CREATE INDEX IF NOT EXISTS batch_mints_url ON batch_mints (btm_url);