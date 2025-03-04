# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0] - 2025-03-04
[0.5.0]: https://github.com/metaplex-foundation/aura/compare/v0.1.0...v0.5.0

### Bug Fixes

- Drop core indexing from make file ([ddd1467](https://github.com/metaplex-foundation/aura/commit/ddd146776fcc63b93cf1ec4f0310f93a87653d69))

- Add required permissions for GitHub Container Registry (#410) ([d3e4623](https://github.com/metaplex-foundation/aura/commit/d3e4623567b653846c5db818f961c02f88bace37))

- [MTG-1301] Fix Docker workflow to use GitHub Container Registry (#412) ([f22ab9f](https://github.com/metaplex-foundation/aura/commit/f22ab9f9c74e93a5f0478c37ae87e14fc5310345))

- Fix the build arg in docker (#417) ([faef18d](https://github.com/metaplex-foundation/aura/commit/faef18d5abc07663e5e722265c4a75978a2b07c9))

- Encase version info build arg in quotes (#418) ([c9924d9](https://github.com/metaplex-foundation/aura/commit/c9924d938fb77c37d68cf3c399bcf94abf3cc297))

- Reorder `cursor` and `options` params in api requests (#411) ([cf54be2](https://github.com/metaplex-foundation/aura/commit/cf54be2f21f80e234e71ce49adc42d69add015d2))

- MTG-1225 stop setting asset owner for Fungible tokens in assets_v3 table ([ecbce76](https://github.com/metaplex-foundation/aura/commit/ecbce7622bd4cbe5cf5fb0936513a72b38b62301))


### Build Process & Auxiliary Tools

- Make build happy ([2a18411](https://github.com/metaplex-foundation/aura/commit/2a184114c53c7610f2a3ac79b801b122d69d6055))

- Bump rust toolchain version to 1.84 ([55aa90a](https://github.com/metaplex-foundation/aura/commit/55aa90a6b76e4bfcd7df72499687b18ebc24873e))

- Return back old env example because we have example_new ([a96eec8](https://github.com/metaplex-foundation/aura/commit/a96eec8734b5192326299302e1e6da18b9e9039e))

- Configure a single sensible rustfmt, format ([9b58797](https://github.com/metaplex-foundation/aura/commit/9b587979e591af9119cca0722cce12d244527215))

- Remove unused enum definitions ([a72dfb1](https://github.com/metaplex-foundation/aura/commit/a72dfb16cf42d8e566980218f3beb99ac7f6769c))

- Remove `PrintableNft` definition & usages ([4fb61d7](https://github.com/metaplex-foundation/aura/commit/4fb61d7dd6d8a0b737eda3072e1a6c4ea53d94ae))

- Fmt ([8868956](https://github.com/metaplex-foundation/aura/commit/886895634919cdbc6257ce018c8dfff03690cbbc))

- Regenerate rust files ([59db74e](https://github.com/metaplex-foundation/aura/commit/59db74e8b2156132abdbcc0e329437ac66253d9b))

- Add consistency check to ingester.Dockerfile ([70e7466](https://github.com/metaplex-foundation/aura/commit/70e74669b14b30c2e9a05cf4864f7030a7d08f4c))

- Remove panic from account processor job start ([841529b](https://github.com/metaplex-foundation/aura/commit/841529b173726035e2d461ea9c3e5f0b2020b160))

- Add `start-rocksdb-backup` command to Makefile ([74e4c48](https://github.com/metaplex-foundation/aura/commit/74e4c48a04ec2305ff337fa79c16635e433f10ed))

- Mount primary rocksdb path as readonly to backup container ([084b2a9](https://github.com/metaplex-foundation/aura/commit/084b2a9d20110ae4f08c55bbb03582b90f545786))

- Optimize retrieval of assets ([f56abf4](https://github.com/metaplex-foundation/aura/commit/f56abf4b6302905d50334c4d17786a4cb7546106))

- Move raydium api url to `consts.rs` ([d8beac4](https://github.com/metaplex-foundation/aura/commit/d8beac46473360574d30a3b95367be456707e7b5))

- Move out `pprof` as optional, gate behind feature ([75a058a](https://github.com/metaplex-foundation/aura/commit/75a058ab3421db4fc446a425678a0ba5c53b777e))

- Fmt rust workflow ([464967c](https://github.com/metaplex-foundation/aura/commit/464967cd687372626392642970da67718d7ae474))

- Reorder contributing readme section ([0200e69](https://github.com/metaplex-foundation/aura/commit/0200e694adb2dbd7c2b15694b374266ed5ed2530))

- Fix ignored tests (#414) ([c730dac](https://github.com/metaplex-foundation/aura/commit/c730dac99be06417f5ec9bcd09f86563ba879d43))

- Added missing cliff config for 0.5 release ([6a67ac1](https://github.com/metaplex-foundation/aura/commit/6a67ac1db830121d2173e95a9dcfa287bb6e071b))

- Dropped the version of nft_ingester to the one before 0.5 ([d266f50](https://github.com/metaplex-foundation/aura/commit/d266f50c2aef17175d431f34a1b7cd7895f3e9f6))

- Bump rust toolchain to 1.85 (#438) ([9d705e2](https://github.com/metaplex-foundation/aura/commit/9d705e26a9a2edc104d8af4903e8d28819324d1f))

- Cherry pick scripts for release ([90c7f51](https://github.com/metaplex-foundation/aura/commit/90c7f51505b922d6c0d3ce7acfda8e3669fe90f2))

- Clean cliff.toml ([733aaa7](https://github.com/metaplex-foundation/aura/commit/733aaa7fae8396cced7c2cfeb2c8e00bfa6c5981))

- Bump version to 0.5.0 ([09c737f](https://github.com/metaplex-foundation/aura/commit/09c737f15ca1c3cac9434b3c0d70af4e99d227d9))


### CI/CD

- Enable CI for develop branch PRs (#405) ([85f2336](https://github.com/metaplex-foundation/aura/commit/85f2336974dfc473b9d15908e145942a12591a7e))


### Documentation

- Add GitFlow contribution guidelines (#402) ([7d044bc](https://github.com/metaplex-foundation/aura/commit/7d044bcc7d0f27f6892ac50c11c8ce696fb6b6e4))

- [MTG-1221] Add comprehensive architecture documentation (#420) ([d68d0ac](https://github.com/metaplex-foundation/aura/commit/d68d0acf4bec52b42ad51cb51a2d3c28251797e6))


### Features

- Update rust versions in dockerfiles & rust-toolchain.toml ([38f2d98](https://github.com/metaplex-foundation/aura/commit/38f2d985a5851ffee835ed7823b19ca13bd0a05f))

- Add env feature for clap ([f52806f](https://github.com/metaplex-foundation/aura/commit/f52806f39bf296efdab6ab73ca0d2ab78f2acd33))

- Drop profiling for slot persister ([5092470](https://github.com/metaplex-foundation/aura/commit/509247098d8c672f593374cb069ef4e694a46218))

- Change the way workers take envs ([0725a9a](https://github.com/metaplex-foundation/aura/commit/0725a9a3da362d41806ada2fb8ec4342723b4b16))

- Change env slightly for secondary DB ([73debce](https://github.com/metaplex-foundation/aura/commit/73debce2016c8a8e108c245c19226377657244b9))

- Drop profiling from docker compose for slot persister ([728af36](https://github.com/metaplex-foundation/aura/commit/728af3645166199884d34ebecb13938ac55c632b))

- Revert changes ([2d8f4d0](https://github.com/metaplex-foundation/aura/commit/2d8f4d077bd948aa6bef8e7e5a97c6179512f163))

- Update env.example ([5fb21b8](https://github.com/metaplex-foundation/aura/commit/5fb21b8f822fde53691d3fc51ebd225ad1c2d5c9))

- Add a separate secondary rocks backup service ([1fbfc10](https://github.com/metaplex-foundation/aura/commit/1fbfc101c7dc961397b5de73ca4c3ef54e094c22))

- Make the backup service a one-time job ([4d01d0c](https://github.com/metaplex-foundation/aura/commit/4d01d0c4d882b44ef136b2671cc930e61a720938))

- Use `PathBuf`s in backup-related functions ([221938b](https://github.com/metaplex-foundation/aura/commit/221938ba5dfb0b6f09da4b56ca33b8ec138151d7))

- Add metrics for `RedisReceiver` ([72a9ad0](https://github.com/metaplex-foundation/aura/commit/72a9ad0d8d1f41173e10f64e8a3357c923fdd467))

- Extend range of interfaces for supply object display (#382) ([f2a45d7](https://github.com/metaplex-foundation/aura/commit/f2a45d799e8b5458153fcd7308d60d965fe93935))

- Add Raydium price fetcher cache warmup for symbols ([39c0457](https://github.com/metaplex-foundation/aura/commit/39c0457624de16f13c709de4822ae060d4951400))

- Add exclusive backoff for signature fetch ([68f14ba](https://github.com/metaplex-foundation/aura/commit/68f14ba9a6298c3d958de7e0491adc94574b616f))

- Cover json downloader with tests ([e91de01](https://github.com/metaplex-foundation/aura/commit/e91de01b0c463ff585b1fd303d6338c2388a549f))

- Reverse backfill from the newest slots to the oldest (#407) ([9bda337](https://github.com/metaplex-foundation/aura/commit/9bda337e01ebb44a61bad7dee9df7b4b12b2e2dd))

- Rework Dockerfiles and improve build ([2324b6c](https://github.com/metaplex-foundation/aura/commit/2324b6ca06811f79161d2ceb5ea9ca87750689f7))

- Add docker build workflow ([4a66616](https://github.com/metaplex-foundation/aura/commit/4a66616ad5e9d5f0ae79e8237d44dda93ea1277b))

- Add version info to images ([9fd5ace](https://github.com/metaplex-foundation/aura/commit/9fd5ace276bda32a673f4140d2d8a97feb580533))

- Add repository dispatch to docker job ([5353d58](https://github.com/metaplex-foundation/aura/commit/5353d58a3de6f584d1e44d7e74ae08303370232d))

- Update blockbuster, bubblegum-batch-sdk to fix acct decode panic (#419) ([b01a695](https://github.com/metaplex-foundation/aura/commit/b01a69548433703e695bdc6ef526800c01227fca))

- Rework payload parsing (#423) ([e18e809](https://github.com/metaplex-foundation/aura/commit/e18e809fed02eaf49b35364e67612510938c8b8d))

- Improve env variables ([fcfadc3](https://github.com/metaplex-foundation/aura/commit/fcfadc3df60e3efae777d34fe84add45e340967b))


### Testing

- Add new getAsset test for regular nft collection (#377) ([daac082](https://github.com/metaplex-foundation/aura/commit/daac082cd0b16269baba2442e534cc12a3fdff00))

- MTG-1225 test searchAssets by owner with  ShowZeroBalance: false ([c69a647](https://github.com/metaplex-foundation/aura/commit/c69a6474af1aa1c696a54bb1ca305cb71ab30659))

- MTG-1225 All different search query scenarios for the showZeroBalance option are covered for all token types ([ba768bd](https://github.com/metaplex-foundation/aura/commit/ba768bdb9410e2a01394ce657d82cf4b90a6ae19))



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

