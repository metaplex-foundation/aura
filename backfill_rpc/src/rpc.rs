use std::{
    future::Future,
    ops::Deref,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use entities::models::{BufferedTransaction, SignatureWithSlot};
use flatbuffers::FlatBufferBuilder;
use futures::{stream, StreamExt, TryStreamExt};
use interface::{
    error::UsecaseError, slot_getter::FinalizedSlotGetter, solana_rpc::TransactionsGetter,
};
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::RpcTransactionConfig,
    rpc_request::RpcError,
};
use solana_program::pubkey::Pubkey;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use solana_transaction_status::UiTransactionEncoding;
use tokio::sync::Notify;
use tokio_retry::{strategy::ExponentialBackoff, RetryIf};

const MAX_SIGNATURES_LIMIT: usize = 50_000_000;
const INITIAL_BACKOFF_DELAY_SECS: u64 = 2;
pub(crate) const MAX_RPC_RETRIES: usize = 7;
pub struct BackfillRPC {
    pub(crate) client: BackoffRpcClient,
}

impl BackfillRPC {
    pub fn connect(addr: String) -> Self {
        let client = RpcClient::new(addr);
        Self { client: BackoffRpcClient::new_with_default_backoff(client) }
    }

    pub(crate) async fn get_signatures_by_address(
        &self,
        until: Option<Signature>,
        before: Option<Signature>,
        address: &Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError> {
        let signatures = self
            .client
            .execute_with_backoff(|| async {
                self.client
                    .get_signatures_for_address_with_config(
                        address,
                        GetConfirmedSignaturesForAddress2Config {
                            until,
                            commitment: Some(CommitmentConfig {
                                commitment: CommitmentLevel::Finalized,
                            }),
                            before,
                            ..Default::default()
                        },
                    )
                    .await
            })
            .await;
        signatures
            .map_err(Into::<UsecaseError>::into)?
            .into_iter()
            .map(|response| {
                Ok(SignatureWithSlot {
                    signature: Signature::from_str(&response.signature)?,
                    slot: response.slot,
                })
            })
            .collect::<Result<Vec<_>, UsecaseError>>()
    }
}

#[async_trait]
impl TransactionsGetter for BackfillRPC {
    async fn get_signatures_by_address(
        &self,
        until: SignatureWithSlot,
        address: Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError> {
        let mut before = None;
        let mut txs = Vec::new();
        let last_finalized_slot = self.get_finalized_slot().await?;
        loop {
            let signatures =
                self.get_signatures_by_address(Some(until.signature), before, &address).await?;
            if signatures.is_empty() {
                break;
            }
            let last = signatures.last().unwrap();
            for sig in signatures.iter() {
                if sig.slot <= last_finalized_slot {
                    txs.push(*sig);
                }
            }
            before = Some(last.signature);
            if last.slot < until.slot || last.signature == until.signature {
                break;
            }
            if txs.len() > MAX_SIGNATURES_LIMIT {
                tracing::warn!("Too many signatures {} for address {}", txs.len(), address);
                Err(UsecaseError::SolanaRPC("Too many signatures".to_string()))?;
            }
        }
        Ok(txs)
    }

    async fn get_txs_by_signatures(
        &self,
        signatures: Vec<Signature>,
        retry_interval_millis: u64,
    ) -> Result<Vec<BufferedTransaction>, UsecaseError> {
        stream::iter(signatures)
            .map(|signature| {
                let client = self.client.clone();
                async move {
                    let mut response = Ok(BufferedTransaction::default());
                    for _ in 0..MAX_RPC_RETRIES {
                        response = client
                            .get_transaction_with_config(
                                &signature,
                                RpcTransactionConfig {
                                    encoding: Some(UiTransactionEncoding::Base64),
                                    commitment: Some(CommitmentConfig {
                                        commitment: CommitmentLevel::Finalized,
                                    }),
                                    max_supported_transaction_version: Some(0),
                                },
                            )
                            .await
                            .map_err(Into::<UsecaseError>::into)
                            .and_then(|transaction| {
                                if transaction
                                    .transaction
                                    .meta
                                    .clone()
                                    .map(|tx| tx.err.is_some())
                                    .unwrap_or_default()
                                {
                                    return Ok(BufferedTransaction::default());
                                }
                                seralize_encoded_transaction_with_status(
                                    FlatBufferBuilder::new(),
                                    transaction,
                                )
                                .map(|fb| BufferedTransaction {
                                    transaction: fb.finished_data().to_vec(),
                                    map_flatbuffer: false,
                                })
                                .map_err(Into::<UsecaseError>::into)
                            });
                        if response.is_ok() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(retry_interval_millis)).await;
                    }
                    response
                }
            })
            .buffered(500) // max count of simultaneous requests
            .try_collect::<Vec<_>>()
            .await
    }
}

#[async_trait]
impl FinalizedSlotGetter for BackfillRPC {
    async fn get_finalized_slot(&self) -> Result<u64, UsecaseError> {
        Ok(self
            .client
            .execute_with_backoff(|| async move {
                self.client
                    .get_slot_with_commitment(CommitmentConfig {
                        commitment: CommitmentLevel::Finalized,
                    })
                    .await
            })
            .await?)
    }
    async fn get_finalized_slot_no_error(&self) -> u64 {
        match self.get_finalized_slot().await {
            Err(e) => {
                tracing::error!("Failed to get finalized slot: {}", e);
                None
            },
            Ok(last_ingested_slot) => Some(last_ingested_slot),
        }
        .unwrap_or(u64::MAX)
    }
}

#[derive(Clone)]
pub struct BackoffRpcClient {
    client: Arc<RpcClient>,
    backoff: ExponentialBackoff,
    is_in_backoff: Arc<AtomicBool>,
    backoff_finish_notify: Arc<Notify>,
}

impl BackoffRpcClient {
    pub fn new_with_default_backoff(client: RpcClient) -> Self {
        // 2 seconds base delay
        let backoff = ExponentialBackoff::from_millis(INITIAL_BACKOFF_DELAY_SECS)
            .factor(1000) // factor by 1000 to get millis
            .max_delay(Duration::from_secs(30));

        Self {
            client: Arc::new(client),
            backoff,
            is_in_backoff: Arc::new(AtomicBool::new(false)),
            backoff_finish_notify: Arc::new(Notify::new()),
        }
    }

    #[cfg(all(test, feature = "rpc_tests"))]
    pub(crate) fn new_test(client: RpcClient) -> Self {
        // constant 1 second interval
        let backoff = ExponentialBackoff::from_millis(1).factor(1000);

        Self {
            client: Arc::new(client),
            backoff,
            is_in_backoff: Arc::new(AtomicBool::new(false)),
            backoff_finish_notify: Arc::new(Notify::new()),
        }
    }

    /// NOTE: this method is best-effort, meaning that if two tasks, `t1` and `t2`
    /// start executing this function, and `t2` gets to execute `RetryIf`
    pub async fn execute_with_backoff<
        R,
        T: Future<Output = Result<R, ClientError>>,
        F: FnMut() -> T,
    >(
        &self,
        call: F,
    ) -> Result<R, ClientError> {
        if self.is_in_backoff.load(Ordering::Relaxed) {
            self.backoff_finish_notify.notified().await;
        }
        let result =
            RetryIf::spawn(self.backoff.clone().take(MAX_RPC_RETRIES), call, |e: &ClientError| {
                let kind = e.kind();
                match kind {
                    ClientErrorKind::RpcError(RpcError::RpcRequestError(s)) => {
                        let retry = s.contains("429 Too Many Requests");
                        if retry {
                            self.is_in_backoff.store(true, Ordering::Relaxed);
                        }
                        retry
                    },
                    _ => false,
                }
            })
            .await;
        self.is_in_backoff.store(false, Ordering::Relaxed);
        self.backoff_finish_notify.notify_one();
        result
    }
}

impl Deref for BackoffRpcClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[cfg(all(test, feature = "rpc_tests"))]
mod tests {
    use std::sync::atomic::AtomicU8;

    use solana_client::{
        client_error::reqwest,
        rpc_request::RpcRequest,
        rpc_sender::{RpcSender, RpcTransportStats},
    };
    use tokio::task::JoinSet;
    use warp::Filter;

    use super::*;

    struct TooManyRequestsRpcSender(String);

    #[async_trait]
    impl RpcSender for TooManyRequestsRpcSender {
        async fn send(
            &self,
            request: RpcRequest,
            _params: serde_json::Value,
        ) -> Result<serde_json::Value, ClientError> {
            let err = reqwest::get(format!("http://{}/", self.0))
                .await
                .expect("request to test server to succeed");
            Err(ClientError::new_with_request(
                ClientErrorKind::Reqwest(
                    err.error_for_status().expect_err("429 server must indeed return 429"),
                ),
                request,
            ))
        }
        fn get_transport_stats(&self) -> RpcTransportStats {
            Default::default()
        }
        fn url(&self) -> String {
            Default::default()
        }
    }

    #[tokio::test]
    async fn test_rpc_get_signatures_by_address() {
        let client = BackfillRPC::connect("https://api.mainnet-beta.solana.com".to_string());
        let signatures = client
            .get_signatures_by_address(
                Some(Signature::default()),
                None,
                &Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(signatures.len(), 1000)
    }

    #[tokio::test]
    async fn test_backoff_client_failure() {
        // Create a route that matches requests to "/" (the root path).
        let route = warp::path::end().map(|| {
            warp::reply::with_status(
                warp::reply::json(&serde_json::Value::Null),
                warp::http::StatusCode::TOO_MANY_REQUESTS,
            )
        });

        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        let _server_handle = usecase::executor::spawn(server);

        let client =
            RpcClient::new_sender(TooManyRequestsRpcSender(addr.to_string()), Default::default());

        let client = BackfillRPC { client: BackoffRpcClient::new_test(client) };
        let err = client
            .get_signatures_by_address(
                Some(Signature::default()),
                None,
                &Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("429"));
    }

    #[tokio::test]
    async fn test_backoff_disallows_concurrent_retries() {
        let route = warp::path::end().map(|| {
            warp::reply::with_status(
                warp::reply::json(&serde_json::Value::Null),
                warp::http::StatusCode::TOO_MANY_REQUESTS,
            )
        });

        let (addr, server) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
        let _server_handle = usecase::executor::spawn(server);

        let client =
            RpcClient::new_sender(TooManyRequestsRpcSender(addr.to_string()), Default::default());

        let client = BackfillRPC { client: BackoffRpcClient::new_test(client) };
        // create counters that show how many retry attempts each of the tasks performed
        let counter_one: Arc<AtomicU8> = Arc::new(AtomicU8::new(0));
        let counter_two: Arc<AtomicU8> = Arc::new(AtomicU8::new(0));
        let counter_three: Arc<AtomicU8> = Arc::new(AtomicU8::new(0));

        let get_signatures_fn = |counter: Arc<AtomicU8>| {
            let client = client.client.clone();
            async move {
                let _ = client
                    .execute_with_backoff(|| async {
                        counter.fetch_add(1, Ordering::Relaxed);
                        let signatures = client
                            .client
                            .get_signatures_for_address_with_config(
                                &Pubkey::from_str("Vote111111111111111111111111111111111111111")
                                    .unwrap(),
                                GetConfirmedSignaturesForAddress2Config {
                                    until: None,
                                    commitment: Some(CommitmentConfig {
                                        commitment: CommitmentLevel::Finalized,
                                    }),
                                    before: None,
                                    ..Default::default()
                                },
                            )
                            .await?;
                        signatures
                            .into_iter()
                            .map(|response| {
                                Ok(SignatureWithSlot {
                                    signature: Signature::from_str(&response.signature).unwrap(),
                                    slot: response.slot,
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .await;
            }
        };

        let mut joinset = JoinSet::<()>::new();
        joinset.spawn(get_signatures_fn(counter_one.clone()));
        joinset.spawn(get_signatures_fn(counter_two.clone()));
        // sleep a bit to give time to lock the backoff boolean to the first task
        // the second task is also executing at this point in time
        tokio::join!(tokio::time::sleep(Duration::from_millis(1)), joinset.join_next());
        let abort_handle = joinset.spawn(get_signatures_fn(counter_three.clone()));
        // interrupt the third waiting task - it's most definitely waiting to be notified
        abort_handle.abort();

        // check that the first task performed one initial and MAX_RPC_RETRIES more attempts
        assert_eq!(counter_one.load(Ordering::Relaxed), 1 + MAX_RPC_RETRIES as u8);
        // check that the second task was spawned quicky enough and performed the same number of
        // attempts, given the best-effort nature of our lock
        assert_eq!(counter_two.load(Ordering::Relaxed), 1 + MAX_RPC_RETRIES as u8);
        // check that the third task was spawned late enough to not even attempt to perform the
        // work
        assert_eq!(counter_three.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_rpc_get_txs_by_signatures() {
        let client =
            BackfillRPC::connect("https://docs-demo.solana-mainnet.quiknode.pro/".to_string());
        let signatures = vec![
            Signature::from_str("2H4c1LcgWG2VuxE4rb318spyiMe1Aet5AysQHAB3Pm3z9nadxJH4C1GZD8yMeAgjdzojmLZGQppuiZqG2oKrtwF2").unwrap(),
            Signature::from_str("265JP2HS6DwJPS4Htk2msUbxbrdeHLFVXUTFVSZ7CyMrHM8xXTxZJpLpt67kKHPAUVtEj7i3fWb5Z9vqMHREHmVm").unwrap(),
        ];

        let txs = client.get_txs_by_signatures(signatures, 0).await.unwrap();

        let parsed_txs = txs
            .iter()
            .map(|tx| {
                plerkle_serialization::root_as_transaction_info(tx.transaction.as_slice()).unwrap()
            })
            .collect::<Vec<_>>();

        assert_eq!(parsed_txs.len(), 2);
        assert_eq!(parsed_txs[0].signature(), Some("2H4c1LcgWG2VuxE4rb318spyiMe1Aet5AysQHAB3Pm3z9nadxJH4C1GZD8yMeAgjdzojmLZGQppuiZqG2oKrtwF2"));
        assert_eq!(parsed_txs[1].slot(), 240869063)
    }

    #[tokio::test]
    #[should_panic]
    async fn test_rpc_get_txs_by_signatures_error() {
        let client =
            BackfillRPC::connect("https://docs-demo.solana-mainnet.quiknode.pro/".to_string());
        let signatures = vec![
        Signature::from_str("2H4c1LcgWG2VuxE4rb318spyiMe1Aet5AysQHAB3Pm3z9nadxJH4C1GZD8yMeAgjdzojmLZGQppuiZqG2oKrtwF3").unwrap(), // transaction that does not exists
    ];

        client.get_txs_by_signatures(signatures, 0).await.unwrap();
    }
}
