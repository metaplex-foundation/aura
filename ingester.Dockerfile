# Initial stage: install cargo-chef
FROM rust:1.74-bullseye AS chef
RUN cargo install cargo-chef

# Planning stage: determine dependencies
FROM chef AS planner
WORKDIR /rust
COPY Cargo.toml Cargo.toml
COPY nft_ingester ./nft_ingester
COPY digital_asset_types ./digital_asset_types
COPY entities ./entities
COPY grpc ./grpc
COPY interface ./interface
COPY usecase ./usecase
COPY metrics_utils ./metrics_utils
COPY rocks-db ./rocks-db
COPY postgre-client ./postgre-client
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
RUN cargo build --release --bin ingester

# Final image
FROM rust:1.74-slim-bullseye
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Etc/UTC APP_USER=appuser
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}

COPY --from=builder /rust/target/release/ingester ${APP}/ingester
WORKDIR ${APP}