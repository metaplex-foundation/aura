#!/usr/bin/env bash

set -eo pipefail

BINARIES=("ingester" "slot_persister" "backfill" "api" "synchronizer" "rocksdb_backup")

docker build . -f docker/base.Dockerfile -t mplx-aura/base:latest

for binary in "${BINARIES[@]}"; do
    echo "Building binary $binary" && \
        docker build . -f docker/app.Dockerfile -t mplx-aura/$binary:latest --build-arg BINARY=$binary
done

