FROM debian:bookworm-slim
ARG BINARY
ENV BINARY=${BINARY}
ARG APP=/usr/src/app
RUN apt update && apt install -y curl ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
ENV TZ=Etc/UTC APP_USER=appuser
RUN groupadd $APP_USER && useradd -g $APP_USER $APP_USER && mkdir -p ${APP}
COPY --from=ghcr.io/metaplex-foundation/aura-base:latest /rust/VERSION.txt ${APP}/
COPY --from=ghcr.io/metaplex-foundation/aura-base:latest /rust/target/release/${BINARY} ${APP}/
WORKDIR ${APP}
USER ${APP_USER}
ENTRYPOINT ["/bin/bash", "-c", "./$BINARY"]
STOPSIGNAL SIGINT
