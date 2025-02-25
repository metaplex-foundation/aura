# Backfiller Modes Explanation

It has four modes of operation. Below, you can find a brief description for each of them.

## Ingest Directly

This is a one-time job.

The consumer is the **DirectBlockParser**, which is a struct with a Bubblegum transactions parser.

The produced item is the **BackfillSource**. The inner object can either be a BigTable client or an RPC client.

It launches the **SlotsCollector** with parameters to start from and parse until to collect slots (u64 numbers) for a pubkey.

The **SlotsCollector** saves slots to the BubblegumSlots Rocks CF.

Then the **TransactionsParser** is launched. It uses the **BubblegumSlotGetter** to get slots to process from the BubblegumSlots CF.

The block producer here is the **BackfillSource**.

It processes blocks, saves transaction results, and then drops the numbers of processed slots from the Rocks BubblegumSlots and adds them to the IngestableSlots CFs.

It doesn’t save any parameters and doesn’t save raw blocks.

## Persist

This is a one-time job.

The consumer is RocksDB.

The producer is the **BackfillSource** (either BigTable or RPC).

Slots to start from and parse until will be taken from the config.

The **SlotsCollector** collects slots and saves them to the BubblegumSlots Rocks CF.

The **TransactionsParser** is launched to get the block by slot number and persists it to the Rocks RawBlock CF.

Once a block is persisted, its number is dropped from BubblegumSlots and added to the IngestableSlots Rocks CF.

It doesn’t save any parameters.

## Ingest Persisted

This is a one-time job.

The consumer is the **DirectBlockParser**, which is a struct with a Bubblegum transactions parser.

The producer is RocksDB.

At the beginning, the **TransactionsParser** takes the slot to start from. It can take this value either from the config or it will start the iteration from the beginning of the raw_blocks_cbor Rocks CF.

For the **DirectBlockParser**, the already_processed_slot function always returns false, so it will parse everything.

The block is extracted from the producer - RocksDB.

The consumer receives the block and processes it. More specifically, it parses transactions, calls get_ingest_transaction_results() to get TransactionResult, and saves it to the Rocks.

Once it has parsed all the blocks, it saves the maximum slot number to the LastFetchedSlot RocksDB parameter. This allows us to restart the backfiller in **PersistAndIngest** mode and it will start collecting new slots and blocks we don't have yet in the DB.

Once it finishes its job, it will not do any post backfill jobs.

## Persist And Ingest

This is a continuous job.

Three workers are running in this mode: slot collector, block fetcher and saver, and block parsing.

### Perpetual Slot Collection

The consumer is Rocks.

The producer for slots is the **BackfillSource** (BigTable or RPC).

It takes the parse_until slot from RockDB LastFetchedSlot parameter. If there is no value, it takes it from the config.

Slot numbers are saved to the BubblegumSlots Rocks CF.

### Perpetual Slot Processing

The consumer is RocksDB.

The producer is the **BackfillSource** (BigTable or RPC).

From the BubblegumSlots Rocks CF, slots are extracted, and then the block is downloaded with the help of the **BackfillSource**.

Once the block is downloaded and saved, the slot is dropped from the BubblegumSlots CF and also this slot is added to the IngestableSlots CF so the next worker can parse it.

### Perpetual Block Ingestion

The consumer is the **DirectBlockParser**, which is a struct with a Bubblegum transactions parser.

The producer is RocksDB.

The **IngestableSlotGetter** returns slots from the IngestableSlots CF, and then blocks are extracted from the Rocks.

Once a block is received, it’s parsed, and the slot is dropped from the IngestableSlots CF.