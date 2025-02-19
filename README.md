# AURA

The Metaplex Aura Network is a decentralized indexing and data availability network that extends Solana and the Solana Virtual Machine (SVM).

The Metaplex Aura Network has three main components:

*   **Data Availability for Digital Assets:** Decentralized data storage and retrieval, ensuring that digital assets and media content are always accessible without relying on a single point of failure.
*   **Multi-Program Consistent Indexing across any SVM:** Enhanced Digital Asset Standard (DAS) API for performant indexing across any protocols on Solana and the broader SVM.
*   **Elastic State Management:** Ability to transition assets between SVM account space and different levels of state compression on demand, increasing flexibility and optimizing asset performance and scalability.

For more background information, please see [our blog](http://www.metaplex.com/posts/aura-network).

## Aura Infrastructure

This repo houses the Multi-Program Consistent Indexer, API, and Types of the Metaplex Aura.  These components aggregate Solana Validator Data into an extremely fast and well-typed DAS API. This API provides a user-friendly interface on top of Metaplex programs, abstracting on-chain byte layouts, enabling super-fast querying and searching, and serving the Merkle proofs necessary for compressed NFTs.

**For a detailed overview of the system architecture, components, and databases, please refer to the [Architecture Document](doc/architecture.md).**

## Key Features

*   **Fast and Efficient Indexing:**  Optimized for both write-heavy ingestion and read-heavy API access.
*   **Data Consistency:**  Robust mechanisms to ensure data integrity, including validation and gap filling.
*   **Scalability:** Designed to handle large data volumes and high transaction throughput.
*   **Extensibility:** Modular design allows for future additions and modifications.
*   **Compressed NFT Support:**  Provides Merkle proofs required for operating with compressed NFTs.
*   **Clean Architecture:** Based on Clean Architecture principles for maintainability and testability.

## Getting Started

### Contributing

Please read our [contribution guidelines](CONTRIBUTING.md) for details on our GitFlow process and how to submit pull requests to us.

### Building

To build all Docker images locally, run:

```sh
make ci
```

This will produce several images corresponding to the binary names (e.g., `ingester`, `api`, etc.).  The images will be tagged as `ghcr.io/metaplex-foundation/aura-<binary name>:latest`, allowing you to run the containers independently.

### Running Integration Tests

```bash
cargo t -F integration_tests
```

### Profiling (Advanced)

To profile any of the binaries, replace the `dockerfile` parameter in the relevant service within `docker-compose.yaml` with `docker/profiling/app.Dockerfile`. This builds the binary with the `profiling` feature enabled. Make sure to rebuild the containers after changing the required Dockerfile.
