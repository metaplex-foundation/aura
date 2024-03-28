COMMIT;
ALTER TYPE specification_asset_class ADD VALUE IF NOT EXISTS 'mpl_core_asset';
ALTER TYPE specification_asset_class ADD VALUE IF NOT EXISTS 'mpl_core_collection';
BEGIN;