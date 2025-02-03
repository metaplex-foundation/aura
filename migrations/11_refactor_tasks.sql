CREATE TYPE status AS ENUM (
    'success',
    'pending',
    'failed'
);

CREATE TYPE mutability AS ENUM (
    'immutable',
    'mutable'
);

ALTER TABLE tasks
    DROP COLUMN tsk_attempts,
    DROP COLUMN tsk_max_attempts,
    DROP COLUMN tsk_error;
    RENAME COLUMN tsk_id TO metadata_hash,
    RENAME COLUMN tsk_metadata_url TO metadata_url,
    RENAME COLUMN tsk_locked_until to next_try_at,
    ADD COLUMN etag text DEFAULT NULL,
    ADD COLUMN last_modified_at timestamptz DEFAULT NULL,
    ADD COLUMN mutability mutability NOT NULL DEFAULT 'mutable',
    ADD COLUMN task_type task_type NOT NULL DEFAULT 'failed';

UPDATE tasks
SET mutability = 'immutable'
WHERE metadata_url = ''
   OR metadata_url LIKE 'ipfs://%'
   OR metadata_url LIKE 'https://ipfs%'
   OR metadata_url LIKE 'https://arweave%'
   OR metadata_url LIKE 'https://www.arweave%';