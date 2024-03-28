ALTER TABLE assets_v3 ADD COLUMN temp_ast_specification_asset_class varchar;
UPDATE assets_v3 SET temp_ast_specification_asset_class = ast_specification_asset_class::text;
ALTER TABLE assets_v3 DROP COLUMN ast_specification_asset_class;
ALTER TABLE assets_v3 RENAME COLUMN temp_ast_specification_asset_class TO ast_specification_asset_class;
DROP TYPE IF EXISTS specification_asset_class;
CREATE TYPE specification_asset_class AS ENUM (
    'unknown',
    'fungible_token',
    'fungible_asset',
    'nft',
    'printable_nft',
    'print',
    'transfer_restricted_nft',
    'non_transferable_nft',
    'identity_nft',
    'programmable_nft',
    'mpl_core_asset',
    'mpl_core_collection'
    );
ALTER TABLE assets_v3
ALTER COLUMN ast_specification_asset_class TYPE specification_asset_class
        USING (ast_specification_asset_class::specification_asset_class);
CREATE INDEX assets_v3_specification_asset_class ON assets_v3 (ast_specification_asset_class) WHERE ast_specification_asset_class IS NOT NULL AND ast_specification_asset_class <> 'unknown'::specification_asset_class;
