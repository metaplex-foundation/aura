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
RUN apt update && apt install -y libclang-dev protobuf-compiler google-perftools graphviz
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
RUN cargo build --release --bin ingester --bin api --bin raw_backfiller --bin synchronizer

# Building the profiling feature services
FROM cacher AS builder-with-profiling
COPY . .
RUN cargo build --release --features profiling --bin ingester --bin api --bin raw_backfiller --bin synchronizer

# Final image
FROM rust:1.75-slim-bullseye AS runtime
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata libjemalloc2 graphviz && rm -rf /var/lib/apt/lists/*
ENV TZ=Etc/UTC APP_USER=appuser LD_PRELOAD="/usr/lib/x86_64-linux-gnu/libjemalloc.so"
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}

COPY --from=builder /rust/target/release/ingester ${APP}/ingester
COPY --from=builder /rust/target/release/raw_backfiller ${APP}/raw_backfiller
COPY --from=builder /rust/target/release/api ${APP}/api
COPY --from=builder /rust/target/release/synchronizer ${APP}/synchronizer
COPY --from=builder-with-profiling /rust/target/release/ingester ${APP}/profiling_ingester
COPY --from=builder-with-profiling /rust/target/release/raw_backfiller ${APP}/profiling_raw_backfiller
COPY --from=builder-with-profiling /rust/target/release/api ${APP}/profiling_api
COPY --from=builder-with-profiling /rust/target/release/synchronizer ${APP}/profiling_synchronizer

WORKDIR ${APP}
STOPSIGNAL SIGINT