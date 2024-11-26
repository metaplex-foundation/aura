# Data model

## Assets model
The main data storage is the RocksDB. It persists all the live data and serves as a primary source of data for all the requests. 

The asset data is split between several columns based on the udpate patterns:

- ASSET_STATIC - holds static data that is persisted once on asset creation and will not change during the asset lifecycle.
- ASSET_DYNAMIC - holds most freequently updated fields; may be updated on multiple occurencies, should be merged carefully - consider selecting before the update;
- ASSET_AUTHORITY - holds the authority of the asset; is updated on `update_authority` method usage;
- ASSET_OWNER - holds the ownership info; is updated on `update_asset_owner_and_delegate_info` method, that's called for any transfer;
- ASSET_LEAF - updated on `update_asset_leaf_info`;
- ASSET_COLLECTION - collection information; updated on `update_asset_collection_info`;

The assets table in Postgres holds only the data needed for indexes. The intended schema of working with the datastorage may be expressed with the following pseudocode:

```
let pubkeys = `SELECT ast_pubkey FROM assets_v3 WHERE ast_owner='abc' AND ... ORDER BY ast_slot_updated DESC LIMIT 100;`;
let assets = rocks.get_assets_by_ids(pubkeys[..]);
return assets
```

## Secondary DB syncronization
The index database is updated in batches. To support this a secondary index is introduced in rocks:
- ASSETS_UPDATED_IN_SLOT_IDX - keys of this table are constructed of slot and pubkey of the asset
- FUNGIBLE_ASSETS_UPDATED_IN_SLOT_IDX - the same as above, the only difference is that the column only stores funglibe assets

The batch update may be expressed as (pseudocode):

```
let last_processed_slot = `SELECT max(ast_slot_updated) FROM assets_v3;`; // just an example, some other method of retrieving the last processed slot may be used depending on the batching strategy.
let updated_key_slots = rocks.fetch_asset_update_index_range_starting_from_slot(last_processed_slot);
// some additional logic on batching may be incorporated here, like filtering too many items, selecting only a certain range of slots, starting from (inclusive) last_processed_slot
let unique_keys = extract_deduplicate_keys(updated_key_slots);
let assets = rocks.get_assets_by_ids(unique_keys);
let assetsIndexed = AssetIndex::from_each(assets);
pg.upsert(assetsIndexed);
```

The batching strategy should account for always reprocessing the last processed slot to ensure the data stays consistent. 