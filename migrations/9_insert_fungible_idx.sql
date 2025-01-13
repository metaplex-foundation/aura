ALTER TABLE last_synced_key DROP CONSTRAINT only_one_row;
INSERT INTO last_synced_key (id, last_synced_asset_update_key) VALUES (2, NULL);
ALTER TABLE last_synced_key ADD CONSTRAINT only_two_rows CHECK (id IN (1, 2));
