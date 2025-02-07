FROM mplx-aura/base AS builder
ARG BINARY
RUN wget https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2 && \
    tar -xjf jemalloc-5.3.0.tar.bz2 && \
    cd jemalloc-5.3.0 && \
    ./configure --enable-prof --enable-stats --enable-debug --enable-fill && \
    make && make install
COPY . .
RUN cargo build --release --features profiling --bin ${BINARY}

FROM debian:bookworm-slim
ARG BINARY
ENV BINARY=${BINARY}
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata libjemalloc2 google-perftools graphviz libjemalloc-dev && rm -rf /var/lib/apt/lists/*
ENV TZ=Etc/UTC APP_USER=appuser LD_PRELOAD="/usr/local/lib/libjemalloc.so.2"
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}
COPY --from=builder /usr/local/lib/libjemalloc.so.2 /usr/local/lib/libjemalloc.so.2
COPY --from=mplx-aura/base:latest /rust/VERSION.txt ${APP}/VERSION.txt
COPY --from=builder /rust/target/release/${BINARY} ${APP}/${BINARY}
WORKDIR ${APP}
ENTRYPOINT ["/bin/bash", "-c", "./$BINARY"]
STOPSIGNAL SIGINT
