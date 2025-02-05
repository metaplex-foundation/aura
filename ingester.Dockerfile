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
COPY integration_tests ./integration_tests
COPY consistency_check ./consistency_check

RUN cargo chef prepare --recipe-path recipe.json

# Caching dependencies
FROM chef AS cacher
WORKDIR /rust
RUN apt update && apt install -y libclang-dev protobuf-compiler
RUN wget https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2 && \
    tar -xjf jemalloc-5.3.0.tar.bz2 && \
    cd jemalloc-5.3.0 && \
    ./configure --enable-prof --enable-stats --enable-debug --enable-fill && \
    make && make install
COPY --from=planner /rust/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Building the services
FROM cacher AS builder
COPY . .
RUN cargo build --release --bin ingester --bin api --bin backfill --bin synchronizer --bin slot_persister --bin rocksdb_backup

# Building the profiling feature services
FROM cacher AS builder-with-profiling
COPY . .
RUN cargo build --release --features profiling --bin ingester --bin api --bin backfill --bin synchronizer --bin slot_persister

# Final image
FROM debian:bullseye-slim AS runtime
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata libjemalloc2 google-perftools graphviz libjemalloc-dev && rm -rf /var/lib/apt/lists/*
COPY --from=cacher /usr/local/lib/libjemalloc.so.2 /usr/local/lib/libjemalloc.so.2
ENV TZ=Etc/UTC APP_USER=appuser LD_PRELOAD="/usr/local/lib/libjemalloc.so.2"
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}

COPY --from=builder /rust/target/release/ingester ${APP}/ingester
COPY --from=builder /rust/target/release/backfill ${APP}/backfill
COPY --from=builder /rust/target/release/api ${APP}/api
COPY --from=builder /rust/target/release/synchronizer ${APP}/synchronizer
COPY --from=builder /rust/target/release/slot_persister ${APP}/slot_persister
COPY --from=builder /rust/target/release/rocksdb_backup ${APP}/rocksdb_backup
COPY --from=builder-with-profiling /rust/target/release/ingester ${APP}/profiling_ingester
COPY --from=builder-with-profiling /rust/target/release/backfill ${APP}/profiling_backfill
COPY --from=builder-with-profiling /rust/target/release/api ${APP}/profiling_api
COPY --from=builder-with-profiling /rust/target/release/synchronizer ${APP}/profiling_synchronizer
COPY --from=builder-with-profiling /rust/target/release/slot_persister ${APP}/profiling_slot_persister

WORKDIR ${APP}
STOPSIGNAL SIGINT
