CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE tasks
ADD COLUMN tsk_id_hash bytea;

UPDATE tasks
SET tsk_id_hash = digest(tsk_metadata_url, 'sha256');

-- Ensure there are no null hashes
ALTER TABLE tasks ALTER COLUMN tsk_id_hash SET NOT NULL;

-- Add the primary key
ALTER TABLE tasks ADD CONSTRAINT tsk_id_hash_pkey PRIMARY KEY (tsk_id_hash);


ALTER TABLE tasks DROP COLUMN tsk_id;

ALTER TABLE tasks RENAME COLUMN tsk_id_hash TO tsk_id;
