use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use radius_sdk::signature::Address;

use spin::Mutex;

static MUTEX1: Mutex<()> = Mutex::new(());
static MUTEX2: Mutex<()> = Mutex::new(());

use crate::{
    rpc::{
        cluster::{
            BatchCreationMessage, SyncBatchCreation, SyncEpochRawTransaction, SyncRawTransaction,
        },
        external::issue_order_commitment,
        prelude::*,
    },
    types::*,
};

static PROCESSED_TX_COUNT: AtomicU64 = AtomicU64::new(0);

/// Wall times for a single `send_raw_transaction` handler on one node; **milliseconds** since Unix epoch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendRawTransactionHandlerTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendRawTransactionClusterMetadataTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendRawTransactionEpochMetadataTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SendRawTransactionSignerTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionResponse {
    pub order_commitment: OrderCommitment,
    pub handler_timings: SendRawTransactionHandlerTimings,
    /// Present when a non-leader node forwarded: `handler_timings` is this hop, this holds the leader's timings.
    pub cluster_metadata_timings: Option<SendRawTransactionClusterMetadataTimings>,
    pub epoch_metadata_timings: Option<SendRawTransactionEpochMetadataTimings>,
    pub signer_timings: Option<SendRawTransactionSignerTimings>,
}

fn now_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransaction {
    pub rollup_id: RollupId,
    pub raw_transaction: RawEpochTransaction,
    pub sender_address: Option<Address>, // None if the transaction is from the client
}

impl RpcParameter<AppState> for SendRawTransaction {
    type Response = SendRawTransactionResponse;

    fn method() -> &'static str {
        "send_raw_transaction"
    }

    async fn handler(mut self, context: AppState) -> Result<Self::Response, RpcError> {
        let handler_start_ms = now_epoch_ms(); // test code

        let rollup = Rollup::get(&self.rollup_id)?;

        let signer_start_ms = now_epoch_ms(); // test code

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        let signer_end_ms = now_epoch_ms(); // test code

        // Get the address of the current tx orderer
        let tx_orderer_address = signer.address().clone();

        let cluster_epoch = {
            let _lock = MUTEX2.lock();
            
            let cluster_epoch = CLUSTER_EPOCH.load(Ordering::Relaxed);

            REDIRECT_RING.incr(cluster_epoch);

            cluster_epoch
        };

        self.sender_address = Some(tx_orderer_address.clone());

        let cluster_metadata_start_ms = now_epoch_ms(); // test code

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )
        .map_err(|error| {
            tracing::error!("Failed to get cluster metadata: {:?}", error);
            Error::ClusterMetadataNotFound
        })?;

        let mut provisional_leader = false;

        let cluster_metadata_end_ms = now_epoch_ms(); // test code

        // If the transaction is from a client, set its epoch to the ClusterMetadata's epoch
        let epoch = match &mut self.raw_transaction {
            RawEpochTransaction::Eth(eth_tx) => {
                if eth_tx.epoch.is_none() {
                    // If the transaction is from the client
                    // Set the epoch
                    eth_tx.set_epoch(cluster_epoch);

                    eth_tx.epoch.unwrap()
                } else {
                    let eth_tx_epoch = eth_tx.epoch.unwrap();
                    if eth_tx_epoch > cluster_epoch {
                        // The transaction already has epoch/leader set (not from a client)
                        // Provisional leader
                        provisional_leader = true;

                        let mut_cluster_metadata_start_ms = now_epoch_ms(); // test code

                        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
                            rollup.platform,
                            rollup.liveness_service_provider,
                            &rollup.cluster_id,
                        )
                        .map_err(|e| {
                            if e.is_none_type() {
                                tracing::warn!(
                                    "ClusterMetadata missing (NoneType). key=({:?}, {:?}, {:?})",
                                    rollup.platform,
                                    rollup.liveness_service_provider,
                                    rollup.cluster_id
                                );
                            } else {
                                tracing::error!("ClusterMetadata get_mut failed: {:?}", e);
                            }
                            Error::ClusterMetadataNotFound
                        })?;

                        let mut_cluster_metadata_end_ms = now_epoch_ms(); // test code

                        if mut_cluster_metadata
                            .epoch_leader_map
                            .get(&eth_tx_epoch)
                            .is_none()
                        {
                            mut_cluster_metadata
                                .epoch_leader_map
                                .insert(eth_tx_epoch, tx_orderer_address.clone());
                        }

                        mut_cluster_metadata.update()?;
                    }
                    eth_tx_epoch
                }
            }
            RawEpochTransaction::EthBundle(_) => cluster_epoch,
        };

        let (epoch_leader_address, is_current_leader) = match provisional_leader {
            true => (Some(tx_orderer_address.clone()), true),
            false => {
                // Get the address of the epoch leader node
                let epoch_leader_address = match &self.raw_transaction {
                    RawEpochTransaction::Eth(eth_tx) => eth_tx
                        .epoch
                        .and_then(|epoch| cluster_metadata.epoch_leader_map.get(&epoch).cloned()),
                    RawEpochTransaction::EthBundle(_) => None,
                };
    
                // 현재 노드가 현재 epoch의 리더인지 확인
                let is_current_leader = match &self.raw_transaction {
                    RawEpochTransaction::Eth(_) => {
                        let Some(leader_addr) = epoch_leader_address.as_ref() else {
                            tracing::error!(
                                "No leader in epoch_leader_map for this transaction epoch; epoch={}",
                                epoch
                            );
                            return Err(Error::GeneralError(
                                format!("No leader registered for this transaction epoch in cluster metadata; epoch={}", epoch)
                            ).into());
                        };
                        *leader_addr == tx_orderer_address
                    }
                    RawEpochTransaction::EthBundle(_) => cluster_metadata.is_leader,
                };

                (epoch_leader_address, is_current_leader)
            }
        };

        let platform_block_height = cluster_metadata.platform_block_height;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            platform_block_height,
        )
        .map_err(|error| {
            tracing::error!("Failed to get cluster: {:?}", error);
            Error::ClusterNotFound
        })?;

        if is_current_leader {
            // 현재 노드가 현재 epoch의 리더인 경우
            let epoch_metadata_start_ms = now_epoch_ms(); // test code

            let mut mut_epoch_metadata = EpochMetadata::get_mut(&self.rollup_id)?;

            let epoch_metadata_end_ms = now_epoch_ms(); // test code

            /*
            if epoch != cluster_metadata.epoch {
                return Err(Error::GeneralError(format!(
                    "Epoch mismatch: EpochMetadata epoch={}, ClusterMetadata epoch={}",
                    epoch, cluster_metadata.epoch,
                )).into());
            }
            */

            let transaction_order = mut_epoch_metadata.transaction_order(epoch);
            let transaction_hash = self.raw_transaction.raw_transaction_hash();

            mut_epoch_metadata.increment_transaction_order(epoch);

            let is_from_client = self.sender_address.is_none();

            if !is_from_client {
                let sender_address = self
                    .sender_address
                    .as_ref()
                    .ok_or_else(|| Error::GeneralError("sender_address is None".into()))?;
                let node_index = cluster
                    .tx_orderer_rpc_infos
                    .iter()
                    .find_map(|(index, info)| {
                        if info.tx_orderer_address == *sender_address {
                            Some(*index)
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| {
                        tracing::error!(
                            "Failed to find node index for sender_address: {:?} in cluster. rollup_id: {:?}, epoch: {}",
                            sender_address,
                            self.rollup_id,
                            epoch
                        );
                        Error::GeneralError("Sender address not found in cluster".into())
                    })?;

                mut_epoch_metadata.increment_received_transaction_count(epoch, node_index);
            }

            mut_epoch_metadata.update()?;

            RawEpochTransactionModel::put_with_transaction_hash(
                &self.rollup_id,
                &transaction_hash,
                self.raw_transaction.clone(),
                true,
            )?;

            RawEpochTransactionModel::put(
                &self.rollup_id,
                epoch,
                transaction_order,
                self.raw_transaction.clone(),
                true,
            )?;

            let merkle_tree = context.merkle_tree_manager().get(&self.rollup_id).await?;
            let (_, pre_merkle_path) = merkle_tree.add_data(transaction_hash.as_ref()).await;
            drop(merkle_tree);

            // rollup.order_commitment_type 뭔지 출력하는 코드
            // tracing::info!("rollup.order_commitment_type: {:?}", rollup.order_commitment_type); // 결과: Sign

            let order_commitment = issue_order_commitment(
                context.clone(),
                rollup.platform,
                self.rollup_id.clone(),
                rollup.order_commitment_type,
                transaction_hash.clone(),
                epoch,
                transaction_order,
                pre_merkle_path,
            )
            .await?;

            order_commitment.put(&self.rollup_id, epoch, transaction_order)?;

            sync_epoch_raw_transaction(
                context.clone(),
                cluster,
                self.rollup_id,
                epoch,
                transaction_order,
                self.raw_transaction.clone(),
                order_commitment.clone(),
                true,
            );

            let builder_rpc_url = context.config().builder_rpc_url.clone();
            let cloned_rpc_client = context.rpc_client();

            if builder_rpc_url.is_some() {
                match self.raw_transaction {
                    RawEpochTransaction::Eth(eth_raw_transaction) => {
                        let params = serde_json::json!([
                            eth_raw_transaction.raw_transaction,
                            epoch,
                            transaction_order
                        ]);

                        let _transaction_hash: String = cloned_rpc_client
                            .request(
                                &builder_rpc_url.unwrap(),
                                "eth_sendRawTransaction",
                                &params,
                                Id::Null,
                            )
                            .await
                            .map_err(|error| {
                                tracing::error!("Failed to send raw transaction: {:?}", error);
                                Error::RpcClient(error)
                            })?;
                    }
                    RawEpochTransaction::EthBundle(_eth_bundle_raw_transaction) => {
                        unimplemented!("EthBundle raw transaction is not supported yet");
                    }
                }
            }

            let order_commitment = match rollup.order_commitment_type {
                OrderCommitmentType::TransactionHash => {
                    OrderCommitment::Single(SingleOrderCommitment::TransactionHash(
                        TransactionHashOrderCommitment::new(transaction_hash.as_string()),
                    ))
                }
                OrderCommitmentType::Sign => order_commitment,
            };

            let handler_end_ms = now_epoch_ms(); // test code

            Ok(SendRawTransactionResponse {
                order_commitment,
                handler_timings: SendRawTransactionHandlerTimings {
                    start_ms: handler_start_ms,
                    end_ms: handler_end_ms,
                },
                cluster_metadata_timings: Some(SendRawTransactionClusterMetadataTimings {
                    start_ms: cluster_metadata_start_ms,
                    end_ms: cluster_metadata_end_ms,
                }),
                epoch_metadata_timings: Some(SendRawTransactionEpochMetadataTimings {
                    start_ms: epoch_metadata_start_ms,
                    end_ms: epoch_metadata_end_ms,
                }),
                signer_timings: Some(SendRawTransactionSignerTimings {
                    start_ms: signer_start_ms,
                    end_ms: signer_end_ms,
                }),
            })
        } else {
            self.sender_address = Some(tx_orderer_address.clone());

            let cluster_metadata = ClusterMetadata::get(
                rollup.platform,
                rollup.liveness_service_provider,
                &rollup.cluster_id,
            )
            .map_err(|error| {
                tracing::error!("Failed to get cluster metadata: {:?}", error);
                Error::ClusterMetadataNotFound
            })?;

            let leader_tx_orderer_rpc_info = epoch_leader_address
                .as_ref()
                .and_then(|addr| cluster.get_tx_orderer_rpc_info(addr))
                .or_else(|| cluster_metadata.leader_tx_orderer_rpc_info.clone());

            match leader_tx_orderer_rpc_info {
                Some(leader_tx_orderer_rpc_info) => {
                    let leader_external_rpc_url = leader_tx_orderer_rpc_info
                        .external_rpc_url
                        .clone()
                        .ok_or(Error::EmptyLeaderClusterRpcUrl)?;

                    match context
                        .rpc_client()
                        .request::<&SendRawTransaction, SendRawTransactionResponse>(
                            leader_external_rpc_url,
                            SendRawTransaction::method(),
                            &self,
                            Id::Null,
                        )
                        .await
                    {
                        Ok(response) => {
                            let handler_end_ms = now_epoch_ms(); // test code

                            Ok(SendRawTransactionResponse {
                                order_commitment: response.order_commitment,
                                handler_timings: SendRawTransactionHandlerTimings {
                                    start_ms: handler_start_ms,
                                    end_ms: handler_end_ms,
                                },
                                cluster_metadata_timings: Some(
                                    SendRawTransactionClusterMetadataTimings {
                                        start_ms: cluster_metadata_start_ms,
                                        end_ms: cluster_metadata_end_ms,
                                    },
                                ),
                                epoch_metadata_timings: None,
                                signer_timings: Some(SendRawTransactionSignerTimings {
                                    start_ms: signer_start_ms,
                                    end_ms: signer_end_ms,
                                }),
                            })
                        }
                        Err(error) => {
                            tracing::error!(
                                "Send raw transaction - leader external rpc error: {:?}",
                                error
                            );
                            Err(error.into())
                        }
                    }
                }
                None => {
                    tracing::error!("Send raw transaction - leader tx orderer rpc info is None");
                    return Err(Error::EmptyLeader)?;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn sync_raw_transaction(
    context: AppState,
    cluster: Cluster,
    rollup_id: RollupId,
    batch_number: u64,
    batch_tx_order: u64,
    raw_transaction: RawTransaction,
    order_commitment: OrderCommitment,
    is_direct_sent: bool,
) {
    tokio::spawn(async move {
        let other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
        if other_cluster_rpc_url_list.is_empty() {
            return;
        }

        let sync_raw_transaction = SyncRawTransaction {
            rollup_id,
            batch_number,
            batch_tx_order,
            raw_transaction,
            order_commitment: order_commitment,
            is_direct_sent,
        };

        context
            .rpc_client()
            .fire_and_forget_multicast(
                other_cluster_rpc_url_list,
                SyncRawTransaction::method(),
                &sync_raw_transaction,
                Id::Null,
            )
            .await
    });
}

#[allow(clippy::too_many_arguments)]
pub fn sync_batch_creation(
    context: AppState,
    cluster: Cluster,
    platform: Platform,
    rollup_id: RollupId,
    batch_number: u64,
    batch_commitment: [u8; 32],
    batch_creator_signature: Signature,
) {
    tokio::spawn(async move {
        /*
        tracing::info!(
            "Sync batch creation - rollup_id: {:?} / batch_number: {:?}",
            rollup_id,
            batch_number
        );
        */

        let other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
        if other_cluster_rpc_url_list.is_empty() {
            return;
        }

        let batch_creation_massage = BatchCreationMessage {
            rollup_id: rollup_id.clone(),
            batch_number,
            batch_commitment,
            batch_creator_signature,
        };
        let leader_tx_orderer_signature = match context
            .get_signer(platform)
            .await
            .map_err(|e| tracing::error!("Failed to get signer: {}", e))
            .and_then(|signer| {
                signer
                    .sign_message(&batch_creation_massage)
                    .map_err(|e| tracing::error!("Failed to sign message: {}", e))
            }) {
            Ok(signature) => signature,
            Err(_) => return,
        };

        let sync_batch_creation = SyncBatchCreation {
            batch_creation_massage,
            leader_tx_orderer_signature,
        };

        context
            .rpc_client()
            .fire_and_forget_multicast(
                other_cluster_rpc_url_list.clone(),
                SyncBatchCreation::method(),
                &sync_batch_creation,
                Id::Null,
            )
            .await
    });
}

#[allow(clippy::too_many_arguments)]
pub fn sync_epoch_raw_transaction(
    context: AppState,
    cluster: Cluster,
    rollup_id: RollupId,
    epoch: u64,
    transaction_order: u64,
    raw_transaction: RawEpochTransaction,
    order_commitment: OrderCommitment,
    is_direct_sent: bool,
) {
    tokio::spawn(async move {
        let other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
        if other_cluster_rpc_url_list.is_empty() {
            return;
        }

        let sync_epoch_raw_transaction = SyncEpochRawTransaction {
            rollup_id,
            epoch,
            transaction_order,
            raw_transaction,
            order_commitment: order_commitment,
            is_direct_sent,
        };

        context
            .rpc_client()
            .fire_and_forget_multicast(
                other_cluster_rpc_url_list,
                SyncEpochRawTransaction::method(),
                &sync_epoch_raw_transaction,
                Id::Null,
            )
            .await
    });
}
