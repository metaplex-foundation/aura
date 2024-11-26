use std::str::FromStr;
use std::sync::Arc;

use interface::checksums_storage::{
    AccBucketCksm, AccChecksumServiceApi, AccGrandBucketCksm, AccLastChange, BbgmChangePos,
    BbgmChangeRecord, BbgmChecksumServiceApi, BbgmEpochCksm, BbgmGrandEpochCksm,
    BbgmGrandEpochCksmWithNumber,
};
use metrics_utils::{
    AccoountConsistencyGrpcClientMetricsConfig, BubblegumConsistencyGrpcClientMetricsConfig,
};
use solana_sdk::pubkey::Pubkey;
use tokio::time::Instant;
use tonic::async_trait;
use tonic::transport::{Channel, Uri};

use crate::consistencyapi::acc_consistency_service_client::AccConsistencyServiceClient;
use crate::consistencyapi::acc_consistency_service_server::AccConsistencyService;
use crate::consistencyapi::bbgm_consistency_service_client::BbgmConsistencyServiceClient;
use crate::consistencyapi::bbgm_consistency_service_server::BbgmConsistencyService;
use crate::consistencyapi::{
    Acc, AccBucketChecksum, AccBucketChecksumsList, AccGrandBucketChecksum,
    AccGrandBucketChecksumsList, AccList, BbgmChange, BbgmChangeList, BbgmChangePosition,
    BbgmEarlistGrandEpoch, BbgmEpoch, BbgmEpochList, BbgmGrandEpoch, BbgmGrandEpochChecksumForTree,
    BbgmGrandEpochForTreeList, BbgmGrandEpochList, GetAccBucketsReq, GetAccReq, GetBbgmChangesReq,
    GetBbgmEpochsReq, GetBbgmGrandEpochsForTreeReq, GetBbgmGrandEpochsReq,
};
use crate::error::GrpcError;

pub struct ConsistencyApiServerImpl {
    bbgm_service: Arc<dyn BbgmChecksumServiceApi + Sync + Send>,
    acc_service: Arc<dyn AccChecksumServiceApi + Sync + Send>,
}

#[async_trait]
impl BbgmConsistencyService for ConsistencyApiServerImpl {
    async fn get_bbgm_earliest_grand_epoch(
        &self,
        _request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<BbgmEarlistGrandEpoch>, tonic::Status> {
        let earliest_grand_epoch = match self.bbgm_service.get_earliest_grand_epoch().await {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let response = BbgmEarlistGrandEpoch {
            grand_epoch: earliest_grand_epoch.map(|v| v as u32),
        };
        Ok(tonic::Response::new(response))
    }

    async fn get_bbgm_grand_epochs_for_tree(
        &self,
        request: tonic::Request<GetBbgmGrandEpochsForTreeReq>,
    ) -> std::result::Result<tonic::Response<BbgmGrandEpochForTreeList>, tonic::Status> {
        let GetBbgmGrandEpochsForTreeReq { tree_pubkey } = request.into_inner();
        let Ok(pk) = Pubkey::try_from(tree_pubkey.clone()) else {
            return Err(tonic::Status::invalid_argument("Invalid tree pubkey"));
        };
        let grand_epochs_for_tree = match self.bbgm_service.list_bbgm_grand_epoch_for_tree(pk).await
        {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let response_records = grand_epochs_for_tree
            .into_iter()
            .map(|rec| BbgmGrandEpochChecksumForTree {
                grand_epoch: rec.grand_epoch as u32,
                checksum: rec.checksum.map(|c| c.to_vec()),
            })
            .collect::<Vec<_>>();
        let response = BbgmGrandEpochForTreeList {
            list: response_records,
        };
        Ok(tonic::Response::new(response))
    }

    async fn get_bbgm_grand_epoch_checksums(
        &self,
        request: tonic::Request<GetBbgmGrandEpochsReq>,
    ) -> std::result::Result<tonic::Response<BbgmGrandEpochList>, tonic::Status> {
        let GetBbgmGrandEpochsReq {
            grand_epoch,
            limit,
            after,
        } = request.into_inner();
        let grand_epoch = grand_epoch as u16;

        let after_pk = if let Some(bytes) = after {
            let Ok(pk) = Pubkey::try_from(bytes) else {
                return Err(tonic::Status::invalid_argument(
                    "Invalid continuation value 'after'",
                ));
            };
            Some(pk)
        } else {
            None
        };
        let grand_epoch_checksums = match self
            .bbgm_service
            .list_grand_epoch_checksums(grand_epoch, limit, after_pk)
            .await
        {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let response_records = grand_epoch_checksums
            .into_iter()
            .map(|ge| convert_granch_epoch_checksum(ge, grand_epoch))
            .collect::<Vec<_>>();
        let response = BbgmGrandEpochList {
            list: response_records,
        };
        Ok(tonic::Response::new(response))
    }

    async fn get_bbgm_epoch_checksums_in_grand_epoch(
        &self,
        request: tonic::Request<GetBbgmEpochsReq>,
    ) -> std::result::Result<tonic::Response<BbgmEpochList>, tonic::Status> {
        let GetBbgmEpochsReq {
            tree_pubkey,
            grand_epoch,
        } = request.into_inner();
        let Ok(tree) = Pubkey::try_from(tree_pubkey) else {
            return Err(tonic::Status::invalid_argument("Invalid tree pubkey"));
        };

        let db_epochs = match self
            .bbgm_service
            .list_epoch_checksums(grand_epoch as u16, tree)
            .await
        {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let epochs = db_epochs.into_iter().map(|e| e.into()).collect::<Vec<_>>();
        let response = BbgmEpochList { list: epochs };
        Ok(tonic::Response::new(response))
    }

    async fn get_bbgm_changes_in_epoch(
        &self,
        request: tonic::Request<GetBbgmChangesReq>,
    ) -> std::result::Result<tonic::Response<BbgmChangeList>, tonic::Status> {
        let GetBbgmChangesReq {
            tree_pubkey,
            epoch,
            limit,
            after,
        } = request.into_inner();
        let Ok(tree) = Pubkey::try_from(tree_pubkey) else {
            return Err(tonic::Status::invalid_argument("Invalid tree pubkey"));
        };

        let db_changes = match self
            .bbgm_service
            .list_epoch_changes(epoch, tree, limit, after.map(|p| p.into()))
            .await
        {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let changes = db_changes
            .into_iter()
            .map(|change| change.into())
            .collect::<Vec<_>>();

        let response = BbgmChangeList { list: changes };
        Ok(tonic::Response::new(response))
    }

    async fn propose_missing_bbgm_changes(
        &self,
        request: tonic::Request<BbgmChangeList>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let change_list = request.into_inner().list;
        let mut changes = Vec::with_capacity(change_list.len());

        for BbgmChange {
            tree_pubkey,
            slot,
            seq,
            signature,
        } in change_list
        {
            let Ok(tree) = Pubkey::try_from(tree_pubkey) else {
                return Err(tonic::Status::invalid_argument("Invalid tree pubkey"));
            };
            let record = BbgmChangeRecord {
                tree_pubkey: tree,
                slot,
                seq,
                signature,
            };
            changes.push(record);
        }
        self.bbgm_service.propose_missing_changes(&changes).await;
        Ok(tonic::Response::new(()))
    }
}

#[async_trait]
impl AccConsistencyService for ConsistencyApiServerImpl {
    async fn get_acc_grand_bucket_checksums(
        &self,
        _request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Response<AccGrandBucketChecksumsList>, tonic::Status> {
        let db_grand_buckets = match self.acc_service.list_grand_buckets().await {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let grand_buckets = db_grand_buckets
            .into_iter()
            .map(|gb| gb.into())
            .collect::<Vec<_>>();
        let response = AccGrandBucketChecksumsList {
            list: grand_buckets,
        };
        Ok(tonic::Response::new(response))
    }

    async fn get_acc_bucket_checksums_in_grand_bucket(
        &self,
        request: tonic::Request<GetAccBucketsReq>,
    ) -> std::result::Result<tonic::Response<AccBucketChecksumsList>, tonic::Status> {
        let GetAccBucketsReq { grand_bucket } = request.into_inner();
        let db_buckets = match self
            .acc_service
            .list_bucket_checksums(grand_bucket as u16)
            .await
        {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let buckets = db_buckets.into_iter().map(|b| b.into()).collect::<Vec<_>>();
        let response = AccBucketChecksumsList { list: buckets };
        Ok(tonic::Response::new(response))
    }

    async fn get_accs_in_bucket(
        &self,
        request: tonic::Request<GetAccReq>,
    ) -> std::result::Result<tonic::Response<AccList>, tonic::Status> {
        let GetAccReq {
            bucket,
            limit,
            after,
        } = request.into_inner();

        let after_pk = if let Some(bytes) = after {
            let Ok(pk) = Pubkey::try_from(bytes) else {
                return Err(tonic::Status::invalid_argument(
                    "Invalid continuation value 'after'",
                ));
            };
            Some(pk)
        } else {
            None
        };

        let db_accs = match self
            .acc_service
            .list_accounts(bucket as u16, limit, after_pk)
            .await
        {
            Ok(v) => v,
            Err(e) => return Err(tonic::Status::internal(e.to_string())),
        };
        let accs = db_accs
            .into_iter()
            .map(|acc| acc.into())
            .collect::<Vec<_>>();
        let response = AccList { list: accs };
        Ok(tonic::Response::new(response))
    }

    async fn propose_missing_acc_changes(
        &self,
        request: tonic::Request<AccList>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let AccList { list } = request.into_inner();
        let mut accs = Vec::with_capacity(list.len());
        for Acc {
            account_pubkey,
            slot,
            write_version,
            data_hash,
        } in list
        {
            let Ok(acc_pk) = Pubkey::try_from(account_pubkey) else {
                return Err(tonic::Status::invalid_argument("Invalid account pubkey"));
            };
            let acc = AccLastChange {
                account_pubkey: acc_pk,
                slot,
                write_version,
                data_hash,
            };
            accs.push(acc);
        }
        self.acc_service.propose_missing_changes(accs).await;
        Ok(tonic::Response::new(()))
    }
}

impl From<AccGrandBucketCksm> for AccGrandBucketChecksum {
    fn from(value: AccGrandBucketCksm) -> Self {
        let AccGrandBucketCksm {
            grand_bucket,
            checksum,
        } = value;
        AccGrandBucketChecksum {
            grand_bucket: grand_bucket as u32,
            checksum: checksum.map(|c| c.to_vec()),
        }
    }
}

impl From<AccBucketCksm> for AccBucketChecksum {
    fn from(value: AccBucketCksm) -> Self {
        let AccBucketCksm { bucket, checksum } = value;
        AccBucketChecksum {
            bucket: bucket as u32,
            checksum: checksum.map(|c| c.to_vec()),
        }
    }
}

impl From<AccLastChange> for Acc {
    fn from(value: AccLastChange) -> Self {
        let AccLastChange {
            account_pubkey,
            slot,
            write_version,
            data_hash,
        } = value;
        Acc {
            account_pubkey: account_pubkey.to_bytes().to_vec(),
            slot,
            write_version,
            data_hash,
        }
    }
}

fn convert_granch_epoch_checksum(value: BbgmGrandEpochCksm, grand_epoch: u16) -> BbgmGrandEpoch {
    let BbgmGrandEpochCksm {
        tree_pubkey,
        checksum,
    } = value;
    BbgmGrandEpoch {
        grand_epoch: grand_epoch as u32,
        tree_pubkey: tree_pubkey.to_bytes().to_vec(),
        checksum: checksum.map(|arr| arr.to_vec()),
    }
}

impl From<BbgmEpochCksm> for BbgmEpoch {
    fn from(value: BbgmEpochCksm) -> Self {
        let BbgmEpochCksm {
            epoch,
            tree_pubkey,
            checksum,
        } = value;
        BbgmEpoch {
            epoch,
            tree_pubkey: tree_pubkey.to_bytes().to_vec(),
            checksum: checksum.map(|arr| arr.to_vec()),
        }
    }
}

impl From<BbgmChangeRecord> for BbgmChange {
    fn from(value: BbgmChangeRecord) -> Self {
        let BbgmChangeRecord {
            tree_pubkey,
            slot,
            seq,
            signature,
        } = value;
        BbgmChange {
            tree_pubkey: tree_pubkey.to_bytes().to_vec(),
            slot,
            seq,
            signature,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<BbgmChangePos> for BbgmChangePosition {
    fn into(self) -> BbgmChangePos {
        BbgmChangePos {
            slot: self.slot,
            seq: self.seq,
        }
    }
}

pub struct BbgmConsistencyApiClientImpl {
    client: tokio::sync::Mutex<BbgmConsistencyServiceClient<Channel>>,
    peer: String,
    metrics: Option<Arc<BubblegumConsistencyGrpcClientMetricsConfig>>,
}

impl BbgmConsistencyApiClientImpl {
    pub async fn new(
        peer: &str,
        metrics: Option<Arc<BubblegumConsistencyGrpcClientMetricsConfig>>,
    ) -> Result<BbgmConsistencyApiClientImpl, GrpcError> {
        let url = Uri::from_str(peer).map_err(|e| GrpcError::UriCreate(e.to_string()))?;
        let channel = Channel::builder(url).connect().await?;

        Ok(BbgmConsistencyApiClientImpl {
            client: tokio::sync::Mutex::new(BbgmConsistencyServiceClient::new(channel)),
            peer: peer.to_string(),
            metrics,
        })
    }
}

#[async_trait]
impl BbgmChecksumServiceApi for BbgmConsistencyApiClientImpl {
    async fn get_earliest_grand_epoch(&self) -> anyhow::Result<Option<u16>> {
        let grpc_request = tonic::Request::new(());
        let mut client = self.client.lock().await;
        let grpc_response = client.get_bbgm_earliest_grand_epoch(grpc_request).await?;
        let result = grpc_response.into_inner().grand_epoch.map(|v| v as u16);
        Ok(result)
    }

    async fn list_bbgm_grand_epoch_for_tree(
        &self,
        tree_pubkey: Pubkey,
    ) -> anyhow::Result<Vec<BbgmGrandEpochCksmWithNumber>> {
        let grpc_request = tonic::Request::new(GetBbgmGrandEpochsForTreeReq {
            tree_pubkey: tree_pubkey.to_bytes().to_vec(),
        });
        let mut client = self.client.lock().await;
        let start = Instant::now();
        let call_result = client.get_bbgm_grand_epochs_for_tree(grpc_request).await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_grand_epochs_for_tree_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_bubblegum_get_grand_epochs_for_tree_latency
                .observe(start.elapsed().as_secs_f64());
        }
        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for BbgmGrandEpochChecksumForTree {
            grand_epoch,
            checksum,
        } in list
        {
            let chksm: Option<[u8; 32]> =
                checksum.map(|c| c.try_into()).transpose().map_err(|v| {
                    anyhow::anyhow!("Invalid checksum for epoch tree {tree_pubkey}: {v:?}")
                })?;
            result.push(BbgmGrandEpochCksmWithNumber {
                grand_epoch: grand_epoch as u16,
                checksum: chksm,
            });
        }
        Ok(result)
    }

    async fn list_grand_epoch_checksums(
        &self,
        grand_epoch: u16,
        limit: Option<u64>,
        after: Option<Pubkey>,
    ) -> anyhow::Result<Vec<BbgmGrandEpochCksm>> {
        let grpc_request = tonic::Request::new(GetBbgmGrandEpochsReq {
            grand_epoch: grand_epoch as u32,
            limit,
            after: after.map(|pk| pk.to_bytes().to_vec()),
        });
        let mut client = self.client.lock().await;

        let start = Instant::now();
        let call_result = client.get_bbgm_grand_epoch_checksums(grpc_request).await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_grand_epochs_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_bubblegum_get_grand_epochs_latency
                .observe(start.elapsed().as_secs_f64());
        }

        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for BbgmGrandEpoch {
            grand_epoch: _,
            tree_pubkey,
            checksum,
        } in list
        {
            let pk = Pubkey::try_from(tree_pubkey)
                .map_err(|v| anyhow::anyhow!("Invalid grand epoch tree pubkey bytes: {v:?}"))?;
            let chksm: Option<[u8; 32]> = checksum
                .map(|c| c.try_into())
                .transpose()
                .map_err(|v| anyhow::anyhow!("Invalid checksum for epoch tree {pk}: {v:?}"))?;
            result.push(BbgmGrandEpochCksm {
                tree_pubkey: pk,
                checksum: chksm,
            });
        }
        Ok(result)
    }

    async fn list_epoch_checksums(
        &self,
        grand_epoch: u16,
        tree_pubkey: Pubkey,
    ) -> anyhow::Result<Vec<BbgmEpochCksm>> {
        let grpc_request = tonic::Request::new(GetBbgmEpochsReq {
            tree_pubkey: tree_pubkey.to_bytes().to_vec(),
            grand_epoch: grand_epoch as u32,
        });
        let mut client = self.client.lock().await;

        let start = Instant::now();
        let call_result = client
            .get_bbgm_epoch_checksums_in_grand_epoch(grpc_request)
            .await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_epochs_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_bubblegum_get_epochs_latency
                .observe(start.elapsed().as_secs_f64());
        }

        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for BbgmEpoch {
            epoch,
            tree_pubkey,
            checksum,
        } in list
        {
            let pk = Pubkey::try_from(tree_pubkey)
                .map_err(|v| anyhow::anyhow!("Invalid epoch tree pubkey bytes: {v:?}"))?;
            let chksm: Option<[u8; 32]> = checksum
                .map(|c| c.try_into())
                .transpose()
                .map_err(|v| anyhow::anyhow!("Invalid checksum for epoch tree {pk}: {v:?}"))?;
            result.push(BbgmEpochCksm {
                epoch,
                tree_pubkey: pk,
                checksum: chksm,
            });
        }
        Ok(result)
    }

    async fn list_epoch_changes(
        &self,
        epoch: u32,
        tree_pubkey: Pubkey,
        limit: Option<u64>,
        after: Option<BbgmChangePos>,
    ) -> anyhow::Result<Vec<BbgmChangeRecord>> {
        let grpc_request = tonic::Request::new(GetBbgmChangesReq {
            tree_pubkey: tree_pubkey.to_bytes().to_vec(),
            epoch,
            limit,
            after: after.map(|BbgmChangePos { slot, seq }| BbgmChangePosition { slot, seq }),
        });
        let mut client = self.client.lock().await;

        let start = Instant::now();
        let call_result = client.get_bbgm_changes_in_epoch(grpc_request).await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_changes_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_bubblegum_get_changes_latency
                .observe(start.elapsed().as_secs_f64());
        }

        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for BbgmChange {
            tree_pubkey,
            slot,
            seq,
            signature,
        } in list
        {
            let pk = Pubkey::try_from(tree_pubkey)
                .map_err(|v| anyhow::anyhow!("Invalid epoch tree pubkey bytes: {v:?}"))?;
            result.push(BbgmChangeRecord {
                tree_pubkey: pk,
                slot,
                seq,
                signature,
            });
        }
        Ok(result)
    }

    async fn propose_missing_changes(&self, changes: &[BbgmChangeRecord]) {
        let req_changes = changes
            .iter()
            .map(
                |BbgmChangeRecord {
                     tree_pubkey,
                     slot,
                     seq,
                     signature,
                 }| BbgmChange {
                    tree_pubkey: tree_pubkey.to_bytes().to_vec(),
                    slot: *slot,
                    seq: *seq,
                    signature: signature.to_owned(),
                },
            )
            .collect::<Vec<_>>();
        let grpc_request = tonic::Request::new(BbgmChangeList { list: req_changes });
        let mut client = self.client.lock().await;
        let _ = client.propose_missing_bbgm_changes(grpc_request).await;
    }
}

pub struct AccConsistencyApiClientImpl {
    client: tokio::sync::Mutex<AccConsistencyServiceClient<Channel>>,
    peer: String,
    metrics: Option<Arc<AccoountConsistencyGrpcClientMetricsConfig>>,
}

impl AccConsistencyApiClientImpl {
    pub async fn new(
        peer: &str,
        metrics: Option<Arc<AccoountConsistencyGrpcClientMetricsConfig>>,
    ) -> Result<AccConsistencyApiClientImpl, GrpcError> {
        let url = Uri::from_str(peer).map_err(|e| GrpcError::UriCreate(e.to_string()))?;
        let channel = Channel::builder(url).connect().await?;

        Ok(AccConsistencyApiClientImpl {
            client: tokio::sync::Mutex::new(AccConsistencyServiceClient::new(channel)),
            peer: peer.to_string(),
            metrics,
        })
    }
}

#[async_trait]
impl AccChecksumServiceApi for AccConsistencyApiClientImpl {
    async fn list_grand_buckets(&self) -> anyhow::Result<Vec<AccGrandBucketCksm>> {
        let grpc_request = tonic::Request::new(());
        let mut client = self.client.lock().await;

        let start = Instant::now();
        let call_result = client.get_acc_grand_bucket_checksums(grpc_request).await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_grand_buckets_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_account_get_grand_buckets_latency
                .observe(start.elapsed().as_secs_f64());
        }

        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for AccGrandBucketChecksum {
            grand_bucket,
            checksum,
        } in list
        {
            let chksm: Option<[u8; 32]> =
                checksum.map(|c| c.try_into()).transpose().map_err(|v| {
                    anyhow::anyhow!("Invalid checksum for grand bucket {grand_bucket}: {v:?}")
                })?;
            result.push(AccGrandBucketCksm {
                grand_bucket: grand_bucket as u16,
                checksum: chksm,
            });
        }
        Ok(result)
    }

    async fn list_bucket_checksums(&self, grand_bucket: u16) -> anyhow::Result<Vec<AccBucketCksm>> {
        let grpc_request = tonic::Request::new(GetAccBucketsReq {
            grand_bucket: grand_bucket as u32,
        });
        let mut client = self.client.lock().await;

        let start = Instant::now();
        let call_result = client
            .get_acc_bucket_checksums_in_grand_bucket(grpc_request)
            .await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_buckets_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_account_get_buckets_latency
                .observe(start.elapsed().as_secs_f64());
        }

        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for AccBucketChecksum { bucket, checksum } in list {
            let chksm: Option<[u8; 32]> = checksum
                .map(|c| c.try_into())
                .transpose()
                .map_err(|v| anyhow::anyhow!("Invalid checksum for bucket {bucket}: {v:?}"))?;
            result.push(AccBucketCksm {
                bucket: bucket as u16,
                checksum: chksm,
            });
        }
        Ok(result)
    }

    async fn list_accounts(
        &self,
        bucket: u16,
        limit: Option<u64>,
        after: Option<Pubkey>,
    ) -> anyhow::Result<Vec<AccLastChange>> {
        let grpc_request = tonic::Request::new(GetAccReq {
            bucket: bucket as u32,
            limit,
            after: after.map(|v| v.to_bytes().to_vec()),
        });
        let mut client = self.client.lock().await;

        let start = Instant::now();
        let call_result = client.get_accs_in_bucket(grpc_request).await;
        if call_result.is_err() && self.metrics.is_some() {
            self.metrics
                .as_ref()
                .unwrap()
                .track_get_latests_call_error(&self.peer);
        }
        let grpc_response = call_result?;
        if let Some(m) = self.metrics.as_ref() {
            m.peers_account_get_latests_latency
                .observe(start.elapsed().as_secs_f64());
        }

        let list = grpc_response.into_inner().list;
        let mut result = Vec::with_capacity(list.len());
        for Acc {
            account_pubkey,
            slot,
            write_version,
            data_hash,
        } in list
        {
            let pk = Pubkey::try_from(account_pubkey)
                .map_err(|v| anyhow::anyhow!("Invalid account pubkey bytes: {v:?}"))?;
            result.push(AccLastChange {
                account_pubkey: pk,
                slot,
                write_version,
                data_hash,
            });
        }
        Ok(result)
    }

    async fn propose_missing_changes(&self, changes: Vec<AccLastChange>) {
        let req_changes = changes
            .iter()
            .map(
                |AccLastChange {
                     account_pubkey,
                     slot,
                     write_version,
                     data_hash,
                 }| Acc {
                    account_pubkey: account_pubkey.to_bytes().to_vec(),
                    slot: *slot,
                    write_version: *write_version,
                    data_hash: *data_hash,
                },
            )
            .collect::<Vec<_>>();
        let grpc_request = tonic::Request::new(AccList { list: req_changes });
        let mut client = self.client.lock().await;
        let _ = client.propose_missing_acc_changes(grpc_request).await;
    }
}
