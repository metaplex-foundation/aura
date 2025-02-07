# Initial stage: install cargo-chef
FROM rust:1.84.0-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /rust

# Planning stage: determine dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef
COPY --from=planner /rust/recipe.json recipe.json
RUN apt update && apt install -y libclang-dev protobuf-compiler
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin ingester --bin slot_persister --bin backfill --bin api --bin synchronizer  --bin rocksdb_backup
