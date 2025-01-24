use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use entities::models::UnprocessedAccountMessage;
use metrics_utils::MetricState;
use mpl_token_metadata::accounts::Metadata;
use nft_ingester::{
    api::{account_balance::AccountBalanceGetterImpl, DasApi},
    buffer::Buffer,
    config::JsonMiddlewareConfig,
    index_syncronizer::Synchronizer,
    init::init_index_storage_with_migration,
    json_worker::JsonWorker,
    message_parser::MessageParser,
    processors::{
        accounts_processor::AccountsProcessor,
        transaction_based::bubblegum_updates_processor::BubblegumTxProcessor,
    },
    raydium_price_fetcher::{self, RaydiumTokenPriceFetcher},
};
use plerkle_serialization::{
    serializer::{seralize_encoded_transaction_with_status, serialize_account},
    solana_geyser_plugin_interface_shims::ReplicaAccountInfoV2,
};
use postgre_client::PgClient;
use rocks_db::{batch_savers::BatchSaveStorage, migrator::MigrationState, Storage};
use serde::de::DeserializeOwned;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    client_error::{ClientError, Result as RpcClientResult},
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcTransactionConfig},
    rpc_request::RpcRequest,
    rpc_response::{Response as RpcResponse, RpcTokenAccountBalance},
};
use solana_sdk::{
    account::Account,
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::Signature,
};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use tokio::{
    sync::{broadcast, Mutex},
    task::JoinSet,
    time::{sleep, Instant},
};
use tracing::{error, info};
use usecase::proofs::MaybeProofChecker;

pub const DEFAULT_SLOT: u64 = 1;

const ACC_PROCESSOR_FETCH_BATCH_SIZE: usize = 1;

const MAX_PG_CONNECTIONS: u32 = 5;
const MIN_PG_CONNECTIONS: u32 = 5;

const API_MAX_PAGE_LIMIT: usize = 100;

const DUMP_SYNCHRONIZER_BATCH_SIZE: usize = 1000;
const SYNCHRONIZER_PARALLEL_TASKS: usize = 1;
const SYNCHRONIZER_DUMP_PATH: &str = "rocks_dump";

const POSTGRE_MIGRATIONS_PATH: &str = "../migrations";
const POSTGRE_BASE_DUMP_PATH: &str = "/aura/integration_tests/";

pub struct TestSetup {
    pub name: String,
    pub client: Arc<RpcClient>,
    pub db: Arc<PgClient>,
    pub rocks_db: Arc<Storage>,
    pub metrics: MetricState,
    pub message_parser: MessageParser,
    pub acc_processor: Arc<AccountsProcessor<Buffer>>,
    pub tx_processor: BubblegumTxProcessor,
    pub synchronizer: Synchronizer<Storage, PgClient>,
    pub das_api: DasApi<
        MaybeProofChecker,
        JsonWorker,
        JsonWorker,
        AccountBalanceGetterImpl,
        RaydiumTokenPriceFetcher,
        Storage,
    >,
}

impl TestSetup {
    pub async fn new(name: String) -> Self {
        Self::new_with_options(name, TestSetupOptions::default()).await
    }

    pub async fn new_with_options(name: String, opts: TestSetupOptions) -> Self {
        let red_metrics = Arc::new(metrics_utils::red::RequestErrorDurationMetrics::new());

        let db_url = std::env::var("DATABASE_TEST_URL").unwrap();

        let index_storage = Arc::new(
            init_index_storage_with_migration(
                db_url.as_ref(),
                MAX_PG_CONNECTIONS,
                red_metrics.clone(),
                MIN_PG_CONNECTIONS,
                POSTGRE_MIGRATIONS_PATH,
                Some(PathBuf::from_str(POSTGRE_BASE_DUMP_PATH).unwrap()),
                None,
            )
            .await
            .unwrap(),
        );

        let rpc_url = match opts.network.unwrap_or_default() {
            Network::Mainnet => std::env::var("MAINNET_RPC_URL").unwrap(),
            Network::Devnet => std::env::var("DEVNET_RPC_URL").unwrap(),
        };
        let client = Arc::new(RpcClient::new(rpc_url.to_string()));

        let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let buffer = Arc::new(Buffer::new());

        let metrics_state = MetricState::new();

        let mutexed_tasks = Arc::new(Mutex::new(JoinSet::new()));

        let acc_processor = AccountsProcessor::build(
            shutdown_rx.resubscribe(),
            ACC_PROCESSOR_FETCH_BATCH_SIZE,
            buffer.clone(),
            metrics_state.ingester_metrics.clone(),
            None,
            index_storage.clone(),
            client.clone(),
            mutexed_tasks.clone(),
            None,
        )
        .await
        .unwrap();

        let rocks_db_dir = tempfile::TempDir::new().unwrap();

        if opts.clear_db {
            index_storage.clean_db().await.unwrap();
        }

        let storage = Arc::new(
            Storage::open(
                rocks_db_dir.path(),
                mutexed_tasks.clone(),
                red_metrics.clone(),
                MigrationState::Last,
            )
            .unwrap(),
        );

        let tx_processor =
            BubblegumTxProcessor::new(storage.clone(), metrics_state.ingester_metrics.clone());

        let das_api = DasApi::new(
            index_storage.clone(),
            storage.clone(),
            metrics_state.api_metrics.clone(),
            None,
            None,
            API_MAX_PAGE_LIMIT,
            None,
            None,
            JsonMiddlewareConfig::default(),
            Arc::new(AccountBalanceGetterImpl::new(client.clone())),
            None,
            Arc::new(RaydiumTokenPriceFetcher::new(
                "".to_string(), // API url, is not used in tests
                raydium_price_fetcher::CACHE_TTL,
                None,
            )),
            "11111111111111111111111111111111".to_string(),
        );

        let message_parser = MessageParser::new();

        let synchronizer = Synchronizer::new(
            storage.clone(),
            index_storage.clone(),
            DUMP_SYNCHRONIZER_BATCH_SIZE,
            SYNCHRONIZER_DUMP_PATH.to_string(),
            metrics_state.synchronizer_metrics.clone(),
            SYNCHRONIZER_PARALLEL_TASKS,
        );

        TestSetup {
            name,
            client,
            db: index_storage,
            rocks_db: storage,
            metrics: metrics_state,
            acc_processor: Arc::new(acc_processor),
            tx_processor,
            synchronizer,
            message_parser,
            das_api,
        }
    }

    pub async fn clean_up_data_bases(&self) {
        self.db.clean_db().await.unwrap();
        self.rocks_db.clean_db().await;
    }
}

#[derive(Clone, Copy, Default)]
pub struct TestSetupOptions {
    pub network: Option<Network>,
    pub clear_db: bool,
}

pub async fn get_transaction(
    client: &RpcClient,
    signature: Signature,
    max_retries: u8,
) -> Result<EncodedConfirmedTransactionWithStatusMeta, ClientError> {
    let mut retries = 0;
    let mut delay = Duration::from_millis(500);

    const CONFIG: RpcTransactionConfig = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(CommitmentConfig { commitment: CommitmentLevel::Confirmed }),
        max_supported_transaction_version: Some(0),
    };

    loop {
        let response = client
            .send(RpcRequest::GetTransaction, serde_json::json!([signature.to_string(), CONFIG,]))
            .await;

        if let Err(error) = response {
            if retries < max_retries {
                error!("failed to get transaction {:?}: {:?}", signature, error);
                sleep(delay).await;
                delay *= 2;
                retries += 1;
                continue;
            } else {
                return Err(error);
            }
        }
        return response;
    }
}

pub async fn fetch_and_serialize_transaction(
    client: &RpcClient,
    sig: Signature,
) -> anyhow::Result<Option<Vec<u8>>> {
    let max_retries = 5;
    let tx: EncodedConfirmedTransactionWithStatusMeta =
        get_transaction(client, sig, max_retries).await?;

    // Ignore if tx failed or meta is missed
    let meta = tx.transaction.meta.as_ref();
    if meta.map(|meta| meta.status.is_err()).unwrap_or(true) {
        info!("Ignoring failed transaction: {}", sig);
        return Ok(None);
    }
    let fbb = flatbuffers::FlatBufferBuilder::new();
    let fbb = seralize_encoded_transaction_with_status(fbb, tx)?;
    let serialized = fbb.finished_data();

    Ok(Some(serialized.to_vec()))
}

// Util functions for accounts
pub async fn rpc_tx_with_retries<T, E>(
    client: &RpcClient,
    request: RpcRequest,
    value: serde_json::Value,
    max_retries: u8,
    error_key: E,
) -> RpcClientResult<T>
where
    T: DeserializeOwned,
    E: fmt::Debug,
{
    let mut retries = 0;
    let mut delay = Duration::from_millis(500);
    loop {
        match client.send(request, value.clone()).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if retries < max_retries {
                    error!("retrying {request} {error_key:?}: {error}");
                    sleep(delay).await;
                    delay *= 2;
                    retries += 1;
                } else {
                    return Err(error);
                }
            },
        }
    }
}

pub async fn fetch_account(
    pubkey: Pubkey,
    client: &RpcClient,
    max_retries: u8,
) -> anyhow::Result<(Account, u64)> {
    const CONFIG: RpcAccountInfoConfig = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64Zstd),
        commitment: Some(CommitmentConfig { commitment: CommitmentLevel::Finalized }),
        data_slice: None,
        min_context_slot: None,
    };

    let response: RpcResponse<Option<UiAccount>> = rpc_tx_with_retries(
        client,
        RpcRequest::GetAccountInfo,
        serde_json::json!([pubkey.to_string(), CONFIG]),
        max_retries,
        pubkey,
    )
    .await?;

    let account: Account = response
        .value
        .ok_or_else(|| anyhow::anyhow!("failed to get account {pubkey}"))?
        .decode()
        .ok_or_else(|| anyhow::anyhow!("failed to parse account {pubkey}"))?;

    Ok((account, response.context.slot))
}

pub async fn fetch_and_serialize_account(
    client: &RpcClient,
    pubkey: Pubkey,
    slot: Option<u64>,
) -> anyhow::Result<Vec<u8>> {
    let max_retries = 5;

    let fetch_result = fetch_account(pubkey, client, max_retries).await;

    let (account, actual_slot) = match fetch_result {
        Ok((account, actual_slot)) => (account, actual_slot),
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to fetch account: {:?}", e));
        },
    };

    let fbb = flatbuffers::FlatBufferBuilder::new();
    let account_info = ReplicaAccountInfoV2 {
        pubkey: &pubkey.to_bytes(),
        lamports: account.lamports,
        owner: &account.owner.to_bytes(),
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: &account.data,
        write_version: 0,
        txn_signature: None,
    };
    let is_startup = false;

    let fbb = serialize_account(
        fbb,
        &account_info,
        match slot {
            Some(slot) => slot,
            None => actual_slot,
        },
        is_startup,
    );
    Ok(fbb.finished_data().to_vec())
}

pub async fn get_token_largest_account(client: &RpcClient, mint: Pubkey) -> anyhow::Result<Pubkey> {
    let response: RpcResponse<Vec<RpcTokenAccountBalance>> = rpc_tx_with_retries(
        client,
        RpcRequest::Custom { method: "getTokenLargestAccounts" },
        serde_json::json!([mint.to_string(),]),
        5,
        mint,
    )
    .await?;

    match response.value.first() {
        Some(account) => {
            let pubkey = Pubkey::from_str(&account.address);
            match pubkey {
                Ok(pubkey) => Ok(pubkey),
                Err(e) => anyhow::bail!("failed to parse pubkey: {:?}", e),
            }
        },
        None => anyhow::bail!("no accounts for mint {mint}: burned nft?"),
    }
}

pub async fn index_and_sync_account_bytes(setup: &TestSetup, account_bytes: Vec<u8>) {
    process_and_save_accounts_to_rocks(setup, account_bytes).await;

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    // copy data to Postgre
    setup.synchronizer.synchronize_nft_asset_indexes(&shutdown_rx, 1000).await.unwrap();
    setup.synchronizer.synchronize_fungible_asset_indexes(&shutdown_rx, 1000).await.unwrap();
}

async fn process_and_save_accounts_to_rocks(setup: &TestSetup, account_bytes: Vec<u8>) {
    let parsed_acc = setup.message_parser.parse_account(account_bytes, false).unwrap();
    let ready_to_process = parsed_acc
        .into_iter()
        .map(|acc| UnprocessedAccountMessage {
            account: acc.unprocessed_account,
            key: acc.pubkey,
            id: String::new(), // Redis message id
        })
        .collect();

    let mut batch_storage =
        BatchSaveStorage::new(setup.rocks_db.clone(), 1, setup.metrics.ingester_metrics.clone());

    let mut interval = tokio::time::interval(Duration::from_millis(1));
    let mut batch_fill_instant = Instant::now();
    let mut core_fees = HashMap::new();
    setup
        .acc_processor
        .process_account(
            &mut batch_storage,
            ready_to_process,
            &mut core_fees,
            &mut vec![],
            &mut interval,
            &mut batch_fill_instant,
        )
        .await;

    let _ = batch_storage.flush();
}

pub async fn cached_fetch_account(
    setup: &TestSetup,
    account: Pubkey,
    slot: Option<u64>,
) -> Vec<u8> {
    cached_fetch_account_with_error_handling(setup, account, slot).await.unwrap()
}

fn get_relative_project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

async fn cached_fetch_account_with_error_handling(
    setup: &TestSetup,
    account: Pubkey,
    slot: Option<u64>,
) -> anyhow::Result<Vec<u8>> {
    let dir = get_relative_project_path(&format!("src/data/accounts/{}", setup.name));

    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(format!("{}", account));

    if file_path.exists() {
        Ok(std::fs::read(file_path).unwrap())
    } else {
        let account_bytes = fetch_and_serialize_account(&setup.client, account, slot).await?;
        std::fs::write(file_path, &account_bytes).unwrap();
        Ok(account_bytes)
    }
}

async fn cached_fetch_transaction(setup: &TestSetup, sig: Signature) -> Vec<u8> {
    let dir = get_relative_project_path(&format!("src/data/transactions/{}", setup.name));

    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(format!("{}", sig));

    if file_path.exists() {
        std::fs::read(file_path).unwrap()
    } else {
        let txn_bytes = fetch_and_serialize_transaction(&setup.client, sig).await.unwrap().unwrap();
        std::fs::write(file_path, &txn_bytes).unwrap();
        txn_bytes
    }
}

pub async fn index_transaction(setup: &TestSetup, sig: Signature) {
    let txn_bytes: Vec<u8> = cached_fetch_transaction(setup, sig).await;

    let ready_to_process_tx = setup.message_parser.parse_transaction(txn_bytes, false).unwrap();

    setup
        .tx_processor
        .process_transaction(ready_to_process_tx, false) // TODO:
        // is from finalized source or not?
        .await
        .unwrap();

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    setup.synchronizer.synchronize_nft_asset_indexes(&shutdown_rx, 1000).await.unwrap();

    setup.synchronizer.synchronize_fungible_asset_indexes(&shutdown_rx, 1000).await.unwrap();
}

async fn cached_fetch_largest_token_account_id(client: &RpcClient, mint: Pubkey) -> Pubkey {
    let dir = get_relative_project_path(&format!("src/data/largest_token_account_ids/{}", mint));

    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(format!("{}", mint));

    if file_path.exists() {
        Pubkey::try_from(std::fs::read(file_path).unwrap()).unwrap()
    } else {
        let token_account = get_token_largest_account(client, mint).await.unwrap();
        std::fs::write(file_path, token_account.to_bytes()).unwrap();
        token_account
    }
}

#[allow(unused)]
#[derive(Clone, Copy, Debug)]
pub enum SeedEvent {
    Account(Pubkey),
    Nft(Pubkey),
    TokenMint(Pubkey),
    Signature(Signature),
}

#[derive(Clone, Copy, Debug, Default)]
pub enum Network {
    #[default]
    Mainnet,
    Devnet,
}

#[derive(Clone, Copy, Debug)]
pub enum Order {
    Forward,
    AllPermutations,
}

/// Data will be indexed, saved to RocskDB and copied to Postgre.
pub async fn index_seed_events(setup: &TestSetup, events: Vec<&SeedEvent>) {
    for event in events {
        match event {
            SeedEvent::Account(account) => {
                index_and_sync_account_with_ordered_slot(setup, *account).await;
            },
            SeedEvent::Nft(mint) => {
                index_nft(setup, *mint).await;
            },
            SeedEvent::Signature(sig) => {
                index_transaction(setup, *sig).await;
            },
            SeedEvent::TokenMint(mint) => {
                index_token_mint(setup, *mint).await;
            },
        }
    }
}

/// Data will be indexed and saved to one DB - RocksDB.
///
/// For sync with Postgre additional method should be called.
pub async fn single_db_index_seed_events(setup: &TestSetup, events: Vec<&SeedEvent>) {
    for event in events {
        match event {
            SeedEvent::Account(account) => {
                index_account_with_ordered_slot(setup, *account).await;
            },
            _ => {
                // TODO: add more seed events processing if it needs
                panic!("Current SeedEvent is not supported for single DB processing")
            },
        }
    }
}

#[allow(unused)]
pub fn seed_account(str: &str) -> SeedEvent {
    SeedEvent::Account(Pubkey::from_str(str).unwrap())
}

pub fn seed_nft(str: &str) -> SeedEvent {
    SeedEvent::Nft(Pubkey::from_str(str).unwrap())
}

#[allow(unused)]
pub fn seed_token_mint(str: &str) -> SeedEvent {
    SeedEvent::TokenMint(Pubkey::from_str(str).unwrap())
}

pub fn seed_txn(str: &str) -> SeedEvent {
    SeedEvent::Signature(Signature::from_str(str).unwrap())
}

pub fn seed_txns<I>(strs: I) -> Vec<SeedEvent>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    strs.into_iter().map(|s| seed_txn(s.as_ref())).collect()
}

#[allow(unused)]
pub fn seed_accounts<I>(strs: I) -> Vec<SeedEvent>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    strs.into_iter().map(|s| seed_account(s.as_ref())).collect()
}

pub fn seed_nfts<I>(strs: I) -> Vec<SeedEvent>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    strs.into_iter().map(|s| seed_nft(s.as_ref())).collect()
}

#[allow(unused)]
pub fn seed_token_mints<I>(strs: I) -> Vec<SeedEvent>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    strs.into_iter().map(|s| seed_token_mint(s.as_ref())).collect()
}

pub async fn index_account(setup: &TestSetup, account: Pubkey) {
    // If we used different slots for accounts, then it becomes harder to test updates of related
    // accounts because we need to factor the fact that some updates can be disregarded because
    // they are "stale".
    let slot = Some(DEFAULT_SLOT);
    let account_bytes = cached_fetch_account(setup, account, slot).await;
    index_and_sync_account_bytes(setup, account_bytes).await;
}

#[derive(Clone, Copy)]
pub struct NftAccounts {
    pub mint: Pubkey,
    pub metadata: Pubkey,
    pub token: Pubkey,
}

pub async fn get_nft_accounts(setup: &TestSetup, mint: Pubkey) -> NftAccounts {
    let metadata_account = Metadata::find_pda(&mint).0;
    let token_account = cached_fetch_largest_token_account_id(&setup.client, mint).await;
    NftAccounts { mint, metadata: metadata_account, token: token_account }
}

async fn index_and_sync_account_with_ordered_slot(setup: &TestSetup, account: Pubkey) {
    let slot = None;
    let account_bytes = cached_fetch_account(setup, account, slot).await;
    index_and_sync_account_bytes(setup, account_bytes).await;
}

async fn index_account_with_ordered_slot(setup: &TestSetup, account: Pubkey) {
    let slot = None;
    let account_bytes = cached_fetch_account(setup, account, slot).await;
    process_and_save_accounts_to_rocks(setup, account_bytes).await;
}

async fn index_token_mint(setup: &TestSetup, mint: Pubkey) {
    let token_account = cached_fetch_largest_token_account_id(&setup.client, mint).await;
    index_account(setup, mint).await;
    index_account(setup, token_account).await;

    // If we used different slots for accounts, then it becomes harder to test updates of related
    // accounts because we need to factor the fact that some updates can be disregarded because
    // they are "stale".
    let slot = Some(1);
    let metadata_account = Metadata::find_pda(&mint).0;
    match cached_fetch_account_with_error_handling(setup, metadata_account, slot).await {
        Ok(account_bytes) => {
            index_and_sync_account_bytes(setup, account_bytes).await;
        },
        Err(_) => {
            // If we can't find the metadata account, then we assume that the mint is not an NFT.
        },
    }
}

pub async fn index_nft(setup: &TestSetup, mint: Pubkey) {
    index_nft_accounts(setup, get_nft_accounts(setup, mint).await).await;
}

pub async fn index_nft_accounts(setup: &TestSetup, nft_accounts: NftAccounts) {
    for account in [nft_accounts.mint, nft_accounts.metadata, nft_accounts.token] {
        index_account(setup, account).await;
    }
}

pub fn trim_test_name(name: &str) -> String {
    name.replace("test_", "")
}
