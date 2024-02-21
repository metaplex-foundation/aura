use async_trait::async_trait;
use entities::models::{BufferedTransaction, SignatureWithSlot};
use flatbuffers::FlatBufferBuilder;
use futures::{stream, StreamExt, TryStreamExt};
use interface::error::UsecaseError;
use interface::slot_getter::FinalizedSlotGetter;
use interface::solana_rpc::TransactionsGetter;
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

const MAX_SIGNATURES_LIMIT: usize = 50_000_000;
const GET_TX_RETRIES: usize = 7;
pub struct BackfillRPC {
    client: Arc<RpcClient>,
}

impl BackfillRPC {
    pub fn connect(addr: String) -> Self {
        Self {
            client: Arc::new(RpcClient::new(addr)),
        }
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
            let signatures = self
                .get_signatures_by_address(until.signature, before, address)
                .await?;
            if signatures.is_empty() {
                break;
            }
            let last = signatures.last().unwrap();
            for sig in signatures.iter() {
                if sig.slot <= last_finalized_slot {
                    txs.push(sig.clone());
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
                    for _ in 0..GET_TX_RETRIES {
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

impl BackfillRPC {
    async fn get_signatures_by_address(
        &self,
        until: Signature,
        before: Option<Signature>,
        address: Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError> {
        self.client
            .get_signatures_for_address_with_config(
                &address,
                GetConfirmedSignaturesForAddress2Config {
                    until: Some(until),
                    commitment: Some(CommitmentConfig {
                        commitment: CommitmentLevel::Finalized,
                    }),
                    before,
                    ..Default::default()
                },
            )
            .await
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
impl FinalizedSlotGetter for BackfillRPC {
    async fn get_finalized_slot(&self) -> Result<u64, UsecaseError> {
        Ok(self
            .client
            .get_slot_with_commitment(CommitmentConfig {
                commitment: CommitmentLevel::Finalized,
            })
            .await?)
    }
    async fn get_finalized_slot_no_error(&self) -> u64 {
        match self.get_finalized_slot().await {
            Err(e) => {
                tracing::error!("Failed to get finalized slot: {}", e);
                None
            }
            Ok(last_ingested_slot) => Some(last_ingested_slot),
        }
        .unwrap_or(u64::MAX)
    }
}

#[cfg(feature = "rpc_tests")]
#[tokio::test]
async fn test_rpc_get_signatures_by_address() {
    let client = BackfillRPC::connect("https://api.mainnet-beta.solana.com".to_string());
    let signatures = client
        .get_signatures_by_address(
            Signature::default(),
            None,
            Pubkey::from_str("Vote111111111111111111111111111111111111111").unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(signatures.len(), 1000)
}

#[cfg(feature = "rpc_tests")]
#[tokio::test]
async fn test_rpc_get_txs_by_signatures() {
    let client = BackfillRPC::connect("https://docs-demo.solana-mainnet.quiknode.pro/".to_string());
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

#[cfg(feature = "rpc_tests")]
#[tokio::test]
#[should_panic]
async fn test_rpc_get_txs_by_signatures_error() {
    let client = BackfillRPC::connect("https://docs-demo.solana-mainnet.quiknode.pro/".to_string());
    let signatures = vec![
        Signature::from_str("2H4c1LcgWG2VuxE4rb318spyiMe1Aet5AysQHAB3Pm3z9nadxJH4C1GZD8yMeAgjdzojmLZGQppuiZqG2oKrtwF3").unwrap(), // transaction that does not exists
    ];

    client.get_txs_by_signatures(signatures, 0).await.unwrap();
}
