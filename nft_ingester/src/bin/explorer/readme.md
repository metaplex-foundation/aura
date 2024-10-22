# Explorer

Explorer is a small RESTful service built in Rust that allows you to interact with a RocksDB database. It provides endpoints to:

- **Iterate over keys** in a specified column family.
- **Retrieve the value** for a specific key.

Both keys and values are Base58-encoded for safe transmission over HTTP.

## Table of Contents

- [Explorer](#explorer)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
  - [Building the Project](#building-the-project)
  - [Running the Service](#running-the-service)
    - [Optional Arguments](#optional-arguments)
    - [Example with all arguments:](#example-with-all-arguments)
  - [Configuration](#configuration)
  - [API Endpoints](#api-endpoints)
    - [Iterate Keys](#iterate-keys)
      - [Example Request:](#example-request)
    - [Get Value](#get-value)
      - [Example Request:](#example-request-1)
  - [Acknowledgements](#acknowledgements)

## Features

- **Iterate Keys**: Retrieve a list of Base58-encoded keys from a specified column family in RocksDB.
- **Get Value**: Retrieve the Base58-encoded value for a given key.
- **Command-line Configuration**: Specify database paths and server port via command-line arguments.
- **RESTful API**: Accessible via HTTP requests using `curl` or any HTTP client.

## Prerequisites

- **Rust**: You need to have Rust installed. You can install Rust using [rustup](https://www.rust-lang.org/tools/install).
- **Cargo**: Cargo is the Rust package manager, which comes with Rust installation.
- **RocksDB Database**: A RocksDB database that you want to interact with.

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

Run the compiled binary with the required arguments:

```bash
./target/release/explorer --primary-db-path /path/to/your/primary/db 
```
### Optional Arguments

•	--secondary-db-path: Path to the secondary RocksDB database. If not provided, a temporary directory is used.
•	--port: Port number for the REST service (defaults to 8086).


### Example with all arguments:

```bash
./target/release/explorer \
  --primary-db-path /path/to/your/primary/db \
  --secondary-db-path /path/to/your/secondary/db \
  --port 8086
```

## Configuration

The service accepts the following command-line arguments:

•	--primary-db-path (**required**): Path to the primary RocksDB database.
•	--secondary-db-path (optional): Path to the secondary RocksDB database.
•	--port (optional): Port number for the REST service (default is 8086).

## API Endpoints

### Iterate Keys

•	URL: /iterate_keys
•	Method: GET
•	Query Parameters:
•	cf_name (required): Name of the column family.
•	limit (optional): Maximum number of keys to return (default is 10).
•	start_key (optional): Base58-encoded key to start iteration from.

#### Example Request:
```bash
curl "http://localhost:8086/iterate_keys?cf_name=default&limit=5"
```

### Get Value

•	URL: /get_value
•	Method: GET
•	Query Parameters:
•	cf_name (required): Name of the column family.
•	key (required): Base58-encoded key whose value is to be retrieved.

#### Example Request:
```bash
curl "http://localhost:8086/get_value?cf_name=default&key=3vQB7B6MrGQZaxCuFg4oh"
```

## Acknowledgements

•	[RocksDB](https://rocksdb.org) for the high-performance key-value database.
•	[Actix Web](https://actix.rs) for the powerful and pragmatic web framework.
•	[bs58](https://docs.rs/bs58/) crate for Base58 encoding and decoding.
•	[Clap](https://clap.rs/) for command-line argument parsing.