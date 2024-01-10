use async_trait::async_trait;
use entities::models::{BufferedTransaction, SignatureWithSlot};
use interface::error::UsecaseError;
use interface::solana_rpc::{GetSignaturesByAddress, GetTransactionsBySignatures};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::signature::Signature;
use std::str::FromStr;

pub struct BackfillRPC {
    client: RpcClient,
}

impl BackfillRPC {
    pub fn connect(addr: String) -> Self {
        Self {
            client: RpcClient::new(addr),
        }
    }
}

#[async_trait]
impl GetSignaturesByAddress for BackfillRPC {
    async fn get_signatures_by_address(
        &self,
        until: Signature,
        before: Option<Signature>,
        address: Pubkey,
    ) -> Result<Vec<SignatureWithSlot>, UsecaseError> {
        Ok(self
            .client
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
            .collect::<Result<Vec<_>, UsecaseError>>()?)
    }
}

#[async_trait]
impl GetTransactionsBySignatures for BackfillRPC {
    async fn get_txs_by_signatures(
        &self,
        _signatures: Vec<Signature>,
    ) -> Result<Vec<BufferedTransaction>, UsecaseError> {
        todo!()
    }
}

#[tokio::test]
async fn test_get_signatures_by_address() {
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
