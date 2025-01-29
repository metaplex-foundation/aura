-- 1) Create a new ENUM for task types
CREATE TYPE task_type AS ENUM (
    'initial_download',
    'refresh'
);

CREATE TYPE task_mutability AS ENUM (
    'immutable',
    'mutable'
);

-- 2) Extend the existing 'tasks' table
ALTER TABLE tasks
    ADD COLUMN tsk_type task_type NOT NULL DEFAULT 'initial_download',
    ADD COLUMN tsk_next_refresh_at timestamptz NULL,
    ADD COLUMN tsk_etag text NULL,
    ADD COLUMN tsk_last_modified text NULL,
    ADD COLUMN tsk_mutability task_mutability NOT NULL DEFAULT 'mutable';

UPDATE tasks
SET tsk_mutability = 'immutable'
WHERE tsk_metadata_url = ''
   OR tsk_metadata_url LIKE 'ipfs://%'
   OR tsk_metadata_url LIKE 'https://ipfs%'
   OR tsk_metadata_url LIKE 'https://arweave%'
   OR tsk_metadata_url LIKE 'https://www.arweave%';
