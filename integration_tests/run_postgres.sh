#!/bin/bash

CONTAINER_NAME="test_db"
IMAGE_NAME="postgres:14"
DB_USER="solana"
DB_PASSWORD="solana"
DB_NAME="solana"
DB_PATH="./db-data"
ROCKS_DUMP_PATH="./rocks_dump"
HOST_PORT="5432"
CONTAINER_PORT="5432"

mkdir -p "$DB_PATH"
mkdir -p "$ROCKS_DUMP_PATH"

docker run -d \
    --name $CONTAINER_NAME \
    -e POSTGRES_USER=$DB_USER \
    -e POSTGRES_PASSWORD=$DB_PASSWORD \
    -e POSTGRES_DB=$DB_NAME \
    -v "$DB_PATH:/var/lib/postgresql/data:rw" \
    -v "$ROCKS_DUMP_PATH:/aura/integration_tests/rocks_dump:ro" \
    -p $HOST_PORT:$CONTAINER_PORT \
    --shm-size=1g \
    $IMAGE_NAME \
    postgres -c log_statement=none \
            -c log_destination=stderr \

if [ $? -eq 0 ]; then
    echo "PostgreSQL container '$CONTAINER_NAME' is running."
else
    echo "Failed to start the PostgreSQL container."
    exit 1
fi
