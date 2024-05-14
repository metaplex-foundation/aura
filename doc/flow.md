# General flow

The main flow of the application can be illustrated as following:

```mermaid
flowchart TB
  BigTable -->|1.Receive\ntransaction\ndata| Ingester
  SolanaRPC -->|1.Receive\ntransaction\ndata| Ingester
  Ingester -->|2.Schedule metadata\ndownload task| Postgre[("PostgreSQL\n(download queue\n&\nindex)")]
  JSONDownloader -->|3.Take metadata\ndownload task| Postgre
  JSONDownloader["JSON\nDownloader"] -->|4.Download\nmedia\nmetadata| DigitalAssetResource["Digital\nAsset\nResource"]
  
  subgraph system[" "]
    Backfiller -->|0.Trigger historical\ndata backfill\nif required| Ingester
    Ingester--> RocksDB[("RocksDB\n(asset metadata)")]
    JSONDownloader --> |5.Save\nmetadata|RocksDB
    JsonRPCServer["JSON RPC\nserver"]-->|9.Retrieve\ndesired\nmetadata |RocksDB
  end

  RocksDB --> Synchronizer -->|6.Update index| Postgre

  Clients <-->|7.Request digital assets| JsonRPCServer
  JsonRPCServer-->|8.Search using index| Postgre
```

Data preparation part:

1. The **Ingester** mechanism continuously fetches fresh transactions from Solana RPC nodes (similar info can be also taken from Google BigTable).
2. The Ingester filters the newly fetched transactions (we are interested only in media related records), and saves them as task for downloading into the PostgreSQL db (we also use PostgreSQL as a queue).
3. After that, the JsonDownloader (interface::json::JsonDownloader) picks up a next tasks from the PostgreSQL "queue".
4. **JsonDownloader** fetches the media metadata from the source the actual media asset is persisted at.
5. The fetched metadata is saved to the RocksDB.
6. The **Synchronizer** (separate process) updates the index in PosgreSQL to make available searching by different matadata fields.

There is also the backfill mechanism that is used to load a historical data (metadata of already existing transactions).

Search data part:

7. A client (end user or another service) makes a call to our JSON RPC endpoint specifying field he wants to search by.
8. The server first goes to the PostgreSQL inxed to find an ID of the required record.
9. Using the ID, the server fetches required metadata from the RocksDB and returns to the client.

