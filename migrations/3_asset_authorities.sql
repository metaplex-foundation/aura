CREATE TABLE assets_authorities (
    auth_pubkey bytea NOT NULL PRIMARY KEY,
    auth_authority bytea,
    auth_slot_updated bigint NOT NULL
);
CREATE INDEX assets_authority ON assets_authorities(auth_authority) WHERE auth_authority IS NOT NULL;

INSERT INTO assets_authorities (auth_pubkey, auth_authority, auth_slot_updated)
SELECT
    CASE
        WHEN ast_specification_asset_class = 'mpl_core_asset' AND ast_collection IS NOT NULL THEN ast_collection
        ELSE ast_pubkey
        END,
    ast_authority,
    ast_slot_updated
FROM
    assets_v3
WHERE
    ast_authority IS NOT NULL
ON CONFLICT DO NOTHING;

ALTER TABLE assets_v3 ADD COLUMN ast_authority_fk bytea;
UPDATE assets_v3
SET ast_authority_fk = CASE
      WHEN ast_specification_asset_class = 'mpl_core_asset' AND ast_collection IS NOT NULL THEN ast_collection
      ELSE ast_pubkey
    END
WHERE ast_authority IS NOT NULL;
ALTER TABLE assets_v3 ADD CONSTRAINT assets_v3_authority_fk_constraint FOREIGN KEY (ast_authority_fk) REFERENCES assets_authorities(auth_pubkey) ON DELETE SET NULL ON UPDATE CASCADE;
CREATE INDEX assets_v3_authority_fk ON assets_v3(ast_authority_fk) WHERE ast_authority_fk IS NOT NULL;

DROP INDEX IF EXISTS assets_v3_authority;
ALTER TABLE assets_v3 DROP COLUMN IF EXISTS ast_authority;
