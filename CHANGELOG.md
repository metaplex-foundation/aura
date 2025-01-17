

## [Unreleased]

### Added
- Unique consumer ID for each worker. [MTG-1155]
- Unique worker name to simplify debugging. [MTG-1155]
- Ability to configure workers batch size via env variables. (account_processor_buffer_size tx_processor_buffer_size) [MTG-1155]
- Configurable timeout for the PG database queries [MTG-1110](https://github.com/metaplex-foundation/aura/pull/371)
- 






### Changed
- Default number of Redis message reads retries to the (number of workers + 1) [MTG-1155]
- 

### Removed


### Fixed
- Issue where messages from Redis were being processed more than once, causing synchronization issues between workers. [MTG-1155]
- 



### Recommendations

- [MTG-1155] The following settings will need to be selected in a real environment with a real load, since it is impossible to do this locally. Before this changes buffer_size used for all workers as a default value 10.
  * account_processor_buffer_size
  * tx_processor_buffer_size
  * redis_accounts_parsing_workers
  * redis_transactions_parsing_workers
- [MTG-1110] Configur PG max query statement timeout or test default configuration.
  * INGESTER_PG_MAX_QUERY_TIMEOUT_SECS (default: 120sec)
  * SYNCHRONIZER_PG_MAX_QUERY_TIMEOUT_SECS (default: 24h)
  * MIGRATOR_PG_MAX_QUERY_TIMEOUT_SECS (default: 24h)
  * API_PG_MAX_QUERY_TIMEOUT_SECS (default: 120sec)

