## Building the Project

Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/metaplex-foundation/aura.git
cd nft_ingester
```

Build the project using Cargo:

```bash
cargo build --bin synchronizer
```

## Running the Service

Run to see the full list of available arguments:

```bash
./target/debug/synchronizer -h
```

Run Synchronizer with minimum functionality.

```bash
./target/debug/synchronizer \
  --pg-database-url postgres://solana:solana@localhost:5432/aura_db
```


## Tips for local debugging/testing

To increase log verbosity, set the log level to debug:
`  --log-level  debug`

To fill the local Redis with messages you can use any other Redis that is available. 
There is a script that will copy existing messages from one Redis and forward copies of these messages to another one. 
`nft_ingester/scripts/transfer_redis_messages.py`