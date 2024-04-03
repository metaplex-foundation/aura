# Initial stage: install cargo-chef
FROM rust:1.75-bullseye AS chef
RUN cargo install cargo-chef

# Planning stage: determine dependencies
FROM chef AS planner
WORKDIR /rust
COPY Cargo.toml Cargo.toml
COPY backfill_rpc ./backfill_rpc
COPY digital_asset_types ./digital_asset_types
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
RUN cargo build --release --bin ingester --bin api --bin raw_backfiller --bin synchronizer

# Final image
FROM rust:1.75-slim-bullseye
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Etc/UTC APP_USER=appuser
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}

COPY --from=builder /rust/target/release/ingester ${APP}/ingester
COPY --from=builder /rust/target/release/raw_backfiller ${APP}/raw_backfiller
COPY --from=builder /rust/target/release/api ${APP}/api
COPY --from=builder /rust/target/release/synchronizer ${APP}/synchronizer
WORKDIR ${APP}
STOPSIGNAL SIGINT