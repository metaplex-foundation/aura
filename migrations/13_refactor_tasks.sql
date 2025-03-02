ALTER TABLE tasks ALTER COLUMN tsk_status TYPE TEXT;
DROP TYPE task_status;

CREATE TYPE task_status AS ENUM (
    'success',
    'pending',
    'failed'
);

CREATE TYPE mutability AS ENUM (
    'immutable',
    'mutable'
);

ALTER TABLE tasks RENAME COLUMN tsk_id TO metadata_hash;
ALTER TABLE tasks RENAME COLUMN tsk_metadata_url TO metadata_url;

ALTER TABLE tasks
    ADD COLUMN tasks_task_status task_status NOT NULL DEFAULT 'pending',
    ADD COLUMN tasks_next_try_at timestamptz DEFAULT NULL,
    ADD COLUMN tasks_etag text DEFAULT NULL,
    ADD COLUMN tasks_mutability mutability NOT NULL DEFAULT 'mutable'
    ADD COLUMN tasks_last_modified_at timestamptz DEFAULT NULL;

UPDATE tasks
SET mutability = 'immutable'
WHERE metadata_url = ''
   OR metadata_url LIKE 'ipfs://%'
   OR metadata_url LIKE 'https://ipfs%'
   OR metadata_url LIKE 'https://arweave%'
   OR metadata_url LIKE 'https://www.arweave%';

UPDATE tasks SET tasks_task_status = 'success' WHERE tsk_status = 'success';
UPDATE tasks SET tasks_next_try_at = NOW() + INTERVAL '1 day', tasks_last_modified_at = 0 WHERE tsk_status = 'success';

ALTER TABLE tasks
    DROP COLUMN tsk_attempts,
    DROP COLUMN tsk_max_attempts,
    DROP COLUMN tsk_error,
    DROP COLUMN tsk_status,
    DROP COLUMN tsk_locked_until;

ALTER TABLE assets_v3 DROP CONSTRAINT assets_v3_ast_metadata_url_id_fkey;
ALTER TABLE assets_v3 ADD FOREIGN KEY (ast_metadata_url_id) REFERENCES tasks(metadata_hash) ON DELETE CASCADE;
   