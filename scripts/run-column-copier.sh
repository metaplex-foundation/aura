#!/usr/bin/env bash

ask_for_confirmation() {
    echo "Make sure you pointed correct columns in the array in rocks-db/src/bin/column_copier/main.rs."
    echo "Do you really want to launch this script? (y/n)"

    read -r answer
    if [ "$answer" != "y" ]; then
        echo "Script execution aborted."
        exit 1
    fi
}

ask_for_confirmation

cargo b --release --package rocks-db --bin column_copier

./target/release/column_copier <source_db_path> <destination_db_path>