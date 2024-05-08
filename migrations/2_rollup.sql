CREATE TYPE rollup_state AS ENUM (
	'uploaded',
	'processing',
	'validation_fail',
    'uploaded_to_arweave',
    'transaction_sent',
    'fail_sending_transaction',
    'moving_to_storage',
	'complete'
);

CREATE TABLE IF NOT EXISTS rollups (
    rlp_file_name varchar(41) NOT NULL
        CONSTRAINT file_name_pk
            PRIMARY KEY,
    rlp_state rollup_state NOT NULL,
    rlp_error varchar(200),
    rlp_url varchar(200),
    rlp_tx_reward int8,
    rlp_created_at timestamptz NOT NULL DEFAULT (now() AT TIME ZONE 'utc'::text)
);

CREATE INDEX IF NOT EXISTS rollups_state_created ON rollups (rlp_state, rlp_created_at);
CREATE INDEX IF NOT EXISTS rollups_url ON rollups (rlp_url);
CREATE INDEX IF NOT EXISTS rollups_created_at ON rollups (rlp_created_at);