# AURA 

The Metaplex Aura Network is a decentralized indexing and data availability network that extends Solana and the Solana Virtual Machine (SVM). 

The Metaplex Aura Network has three main components:
- Data Availability for Digital Assets: decentralized data storage and retrieval, ensuring that digital assets and media content are always accessible without relying on a single point of failure
- Multi-Program Consistent Indexing across any SVM: enhanced Digital Asset Standard (DAS) API for performant indexing across any protocols on Solana and the broader SVM. 
- Elastic State Management: ability to transition assets between SVM account space and different levels of state compression on demand, increasing flexibility and optimizing asset performance and scalability.

For more background information, please see [our blog](http://www.metaplex.com/posts/aura-network).

## Aura Infrastructure
This repo houses the Multi-Program Consistent Indexer, API and Types of the Metaplex Aura. Together these 
components are responsible for the aggregation of Solana Validator Data into an extremely fast and well typed DAS API. This 
api provides a nice interface on top of the Metaplex programs. It abstracts the byte layout on chain, allows for 
super-fast querying and searching, as well as serves the merkle proofs needed to operate over compressed nfts. 

See the [application main flow](doc/flow.md).

## Aura index
Aura index consists of 2 major components:
- primary Digital Asset data stored in a RocksDB - a performant key-value storage holding complete info
- lookup index stored in a relational DB - Postgres. It's used to fetch the keys of the assets that match a complex search criteria.

In order to keep the lookup index in sync with the primary storage a [Synchronizer](./nft_ingester/src/index_syncronizer.rs) is being run in the background.

Aura primary storage holds the following components:
- raw data used to recreate a full index of compressed assets
- digital assets data
- change logs for the compressed assets
- downloaded and indexed metadata for assets
- indexes used for consistency checks

## Aura architecture

The project is based on the [Clean Architecture Principles](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)

The project's structure is a reflection of the following clean architecture principles:

- Framework Independence: The architecture is built to be independent of any specific framework or library.
- Testability: Business rules can be independently tested without UI, Database, or external interfaces.
- UI Independence: Changes in the UI do not impact the core application logic.
- Database Independence: Business logic is not tightly coupled with the database technology.
- External Agency Independence: Business rules are unaware of anything outside their scope.

### Project structure

- entities. Contains business models that represent the domain and are used across various layers of the application.
- interface. Stores traits that define public-facing interfaces, segregating the layers and ensuring loose coupling.
- usecase. The domain-centric heart of the application, containing use cases that encapsulate business rules.
- grpc. Houses gRPC service definitions and their implementation.
- nft_injester. meant for API implementation; it currently includes indexing, synchronization and additional logic that should be refactored in line with clean architecture.
- data layer spread across
  - rocks-db. Primary data source for the application. It contains the implementation of the database client and should be used for all persistent data storage, retrieval, and data manipulations.
  - postgre-client. Secondary data source used exclusively for search indexes. The implementation here should focus on indexing and search-related operations to optimize query performance. Potentially this may be replaced with any relational DB.

### Components
1. Ingester -> A background processing system that gets messages from a [Messenger](https://github.com/metaplex-foundation/digital-asset-validator-plugin), and uses [BlockBuster](https://github.com/metaplex-foundation/blockbuster) Parsers to store the canonical representation of Metaplex types in a storage system for all the supported programs.
2. Ingester -> Api -> A JSON Rpc api that serves Metaplex objects. This api allows filtering, pagination and searching over Metaplex data. This data includes serving the merkle proofs for the compressed NFTs system. It is intended to be run right alongside the Solana RPC and works in much the same way. Just like the solana RPC takes data from the validator and serves it in a new format, so this api takes data off the validator and serves it.

The API specification is compatible with the standard DAS specification here https://github.com/metaplex-foundation/api-specifications


### Developing and running

#### PR/Code requirements 
1) CI/CD has code formating checker so use FTM before code commit: `cargo fmt`
2) 

#### Run Integration tests
Integration tests require Postgres db url, devnet and mainnet rpc  

How to run with CLI:
```cml
DATABASE_TEST_URL='postgres://solana:solana@localhost:5432/aura_db' DEVNET_RPC_URL='https://devnet-aura.metaplex.com/{YOUR_TOKEN_ACCESS}' MAINNET_RPC_URL='https://mainnet-aura.metaplex.com/{YOUR_TOKEN_ACCESS}' cargo test --features integration_tests
```



Full documentation and contribution guidelines coming soon…


