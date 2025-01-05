## Building the Project

Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/metaplex-foundation/aura.git
cd nft_ingester
```

Build the project using Cargo:

```bash
cargo build --release --bin explorer
```

## Running the Service

Run to see the full list of available arguments:

```bash
./target/debug/ingester -h
```

Run indexer with minimum functionality. (without API/Back Filler/Bubblegum BackFiller/Gap Filler/Run Profiling/Sequence Consistent Checker)

```bash
./target/debug/ingester \
  --pg-database-url postgres://solana:solana@localhost:5432/aura_db \
  --rpc-host https://mainnet-aura.metaplex.com/{personal_rpc_key} \
  --redis-connection-config '{"redis_connection_str":"redis://127.0.0.1:6379/0"}'
```

### Main components that can be run with ingester

* --start_api: API service.
* --run_gapfiller: Gapfiller service.
* --run_profiling: Enable profiling.
* --restore_rocks_db: Try to restore rocksDb.
* --run-sequence-consistent-checker: Run sequence consistent checker.

To increase log verbosity, set the log level to debug:
`  --log-level  debug`


## Tips for local debugging/testing

To fill the local Redis with messages you can use any other Redis that available. 
There is a script that will copy existing messages from one Redis and forward copies of these messages to another one. 
`nft_ingester/scripts/transfer_redis_messages.py`