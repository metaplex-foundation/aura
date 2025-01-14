# Consistency check tools

This crate has two binaries to check data consistency in the DB.

## Compressed assets

The binary `compressed_assets` takes a `csv` file with tree keys and checks the proof for each minted asset in a tree.

Here is an example of the `csv` file it expects to receive:

```csv
5wmXasetQTJJ54L3MJta8a4TNPX9piBDneRsC2m2x3Lw
5c5GTTkDVHerDyvXM3gb8bF63AT1ejQ1KzRkJYF7YUnL
DyxaLr1TwhQxD39jdgCYcScZouT815tuHpcLbjEz7ejo
5mEWS3Nzi4seDVLTm8eozdYzMb9vmADzgaT5mG9hfbHm
6B1xTnmCY7naTCJaxKsT6GCRUpZ6NTvJezSowEespk8a
ErmSicq5YrwGdhsvKbzvcotb11ygvHJbVQ1XAJTpmBYc
EDR6ywjZy9pQqz7UCCx3jzCeMQcoks231URFDizJAUNq
Ude9FcHfavnXhPWAUjvPZQ2sbVwJ6bJowQs7FA7nVJg
BrDVQPfTAUCFo5YtJSALM8jt71cW3fGCKYGPDsCSp1rS
```

Launch command:

```
cargo r --bin compressed_assets -- --rpc-endpoint https://solana.rpc --db-path /path/to/rocksdb --trees-file-path ./trees.csv --workers 50 --inner-workers 300 --asset-batch 1000
```

The `workers` parameter points to how many trees will be processed in parallel.

The `inner-workers` parameter points to how many threads each worker will use to process the tree.

The `asset-batch` parameter points to how many assets will be selected within one batch request to the RocksDB.

After launch, it will show a progress bar with information about how many assets and trees have already been processed.

Once it finishes its job it will create two `csv` files: `failed_checks.csv` and `failed_proofs.csv`.

The Failed checks file contains tree addresses that were not processed because of some errors, which could be an RPC error or tree config account deserialization error.

The Failed proofs file contains data in format `treeID, assetID`, and it shows assets that have invalid proofs.

## Regular assets

The binary `regular_assets` takes a Solana accounts snapshot and verifies that the DB is not missing any key from the snapshot.

Launch command:

```
cargo r --bin regular_assets -- --db-path /path/to/rocksdb --snapshot-path /path/to/snapshot.tar.zst --inner-workers 100
```

There are two threads spawned. One to check NFTs and one to check fungible tokens.

The parameter `inner-workers` points to how many threads each of those workers is going to use to check if the account is in the DB.

After launch, it will show a progress bar which shows how many assets have been iterated over. So that counter shows the number of keys in a snapshot but not the number of NFTs.

Once it finishes its job it will create three files: `missed_asset_data.csv`, `missed_mint_info.csv`, `missed_token_acc.csv`.

The Missed asset data file contains NFTs for which we missed asset data. Asset data is complete details about the NFT.

The Missed mint info file contains mint addresses that we missed.

The Missed token acc file contains token account addresses that we missed.