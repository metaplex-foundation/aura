#!/usr/bin/env bash

set -eo pipefail

# Default to metaplex-foundation if no organization is provided
ORG=${1:-metaplex-foundation}
BINARIES=("ingester" "slot_persister" "backfill" "api" "synchronizer" "rocksdb_backup")

docker build . -f docker/base.Dockerfile -t ghcr.io/${ORG}/aura-base:latest --build-arg VERSION_INFO="$(exec "$(dirname "$(realpath "$0")")/version.sh")"

for binary in "${BINARIES[@]}"; do
    echo "Building binary $binary" && \
        docker build . -f docker/app.Dockerfile -t ghcr.io/${ORG}/aura-$binary:latest --build-arg BINARY=$binary
done

