# Slot storage migrator

## Quick overview

This exists to migrate from the old, deprecated format of slots, containing UiConfirmedBlock, to a new one, containing a smaller custom data structure that fits our needs.

The function of the migration is pretty simple:

1. Open the slots database in primary mode with the deprecated CF descriptor
2. Start iterating on the deprecated blocks
3. In parallel, decode the bytes into the deprecated structure, convert it to the new format, and send over a channel to the writing worker.
4. The writing worker receives the new data in (k, v) format, and writes it to a batch, which is optionally flushed to the new column family if the batch reaches the desired size.
5. Periodically free the memory of the spawned tasks once they are finished.
6. When the iterator has ended, do one final flush of the batch, and delete the old column family.

**NOTE**: the deprecated column family is not dropped until the iterator has ended, meaning this process can be restarted from the last known slot key.

## Known issues

This process consumes a lot of memory due to the need of allocating additional heap memory to convert iterator items to owned values (vectors). Therefore, if the memory is not sufficient, the task might be killed by the OS.
Because of this, we must be able to start the migration task from the last known point. The process of doing this is looking at the logs, determining the last slot that was successfully inserted into rocksdb, and specifying this slot in the `start_slot` parameter.

## Running

To start the migration, run:
```shell
cargo r -r --bin migrate_slots_db -- --slots-db-path /path/to/slot/storage
```

To migrate from the specified slot, the command is largely the same, with the exception of specifying the starting slot:
```shell
cargo r -r --bin migrate_slots_db -- --slots-db-path /path/to/slot/storage --start-slot 180000000 # Replace the start slot with the desired one
```
