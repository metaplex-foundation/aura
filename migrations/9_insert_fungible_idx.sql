ALTER TABLE last_synced_key DROP CONSTRAINT only_one_row;
INSERT INTO last_synced_key (null);
ADD CONSTRAINT only_two_rows CHECK (id IN (1, 2));
