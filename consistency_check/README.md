# Consistency check tools

This crate has two binaries to check data consistency in the DB.

## Compressed assets

Binary `compressed_assets` is taking `csv` file with tree keys and check proof for each minted asset in a tree.

Here is example of `csv` file it expects to receive:

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
cargo r --bin compressed_assets -- --rpc-endpoint https://solana.rpc --db-path /path/to/rocksdb --trees-file-path ./trees.csv --workers 50 --inner-workers 300
```

Workers parameter points how many trees will be processed in parallel.

Inner workers parameter points how many threads each worker will use to process the tree.

After launch it will show progress bar with information about how much assets and trees already processed.

Once it finishes its job it will create two `csv` files: `failed_checks.csv` and `failed_proofs.csv`.

Failed checks file contains tree addresses which were not processed because of some errors, it could RPC error or tree config account deserialisation error.

Failed proofs file contains data like `treeID,assetID`, it shows assets which has invalid proofs.

## Regular assets

Binary `regular_assets` is taking Solana accounts snapshot and verifies that DB is not missing any key from snapshot.

Launch command:

```
cargo r --bin regular_assets -- --db-path /path/to/rocksdb --snapshot-path /path/to/snapshot.tar.zst --inner-workers 100
```

There are two threads spawned. One to check NFTs and one to check fungible tokens.

Parameter inner workers points how many threads each of that worker going to use to check if account is in DB.

After launch it will show progress bar which shows how many assets iterated over. So that counter shows amount of keys in a snapshot but not number of NFTs.

Once it finishes its job it will create three files: `missed_asset_data.csv`, `missed_mint_info.csv`, `missed_token_acc.csv`.

Missed asset data file contains NFTs for which we missed asset data. Asset data is complete details about NFT.

Missed mint info file contains mint addresses which we missed.

Missed token acc file contains token accounts addresses which we missed.