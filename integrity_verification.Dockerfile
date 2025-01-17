# Initial stage: install cargo-chef
FROM rust:1.84-bullseye AS chef
RUN cargo install cargo-chef

# Planning stage: determine dependencies
FROM chef AS planner
WORKDIR /rust
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY backfill_rpc ./backfill_rpc
COPY entities ./entities
COPY grpc ./grpc
COPY interface ./interface
COPY metrics_utils ./metrics_utils
COPY nft_ingester ./nft_ingester
COPY postgre-client ./postgre-client
COPY rocks-db ./rocks-db
COPY tests/setup ./tests/setup
COPY usecase ./usecase
COPY integrity_verification ./integrity_verification
COPY blockbuster ./blockbuster

RUN cargo chef prepare --recipe-path recipe.json

# Caching dependencies
FROM chef AS cacher
WORKDIR /rust
RUN apt update && apt install -y libclang-dev protobuf-compiler
COPY --from=planner /rust/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Building the services
FROM cacher AS builder
COPY . .
RUN cargo build --release --bin integrity-verification

# Final image
FROM rust:1.84-slim-bullseye
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Etc/UTC APP_USER=appuser
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}

COPY --from=builder /rust/target/release/integrity-verification ${APP}/integrity-verification
WORKDIR ${APP}
STOPSIGNAL SIGINT
