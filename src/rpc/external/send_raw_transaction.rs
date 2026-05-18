use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use radius_sdk::signature::Address;

use crate::{
    rpc::{
        cluster::{BatchCreationMessage, SyncBatchCreation, SyncEpochRawTransaction, SyncRawTransaction},
        external::issue_order_commitment,
        prelude::*,
    },
    types::*,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionHandlerTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionClusterMetadataTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionEpochMetadataTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionEpochUpdateTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionSignerTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionRedirectRpcTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionClusterUpdateTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionIssueOCTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionMerkleTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionTXPutTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionOCPutTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionSyncEpochTXTimings {
    pub start_ms: u128,
    pub end_ms: u128,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendRawTransactionResponse {
    pub order_commitment: OrderCommitment,
    pub redirect: bool,
    pub handler_timings: SendRawTransactionHandlerTimings,
    pub cluster_metadata_timings: SendRawTransactionClusterMetadataTimings,
    pub epoch_metadata_timings: SendRawTransactionEpochMetadataTimings,
    pub epoch_update_timings: SendRawTransactionEpochUpdateTimings,
    pub signer_timings: SendRawTransactionSignerTimings,
    pub redirect_rpc_timings: Option<SendRawTransactionRedirectRpcTimings>,
    pub leader_cluster_metadata_timings: Option<SendRawTransactionClusterMetadataTimings>,
    pub cluster_update_timings: SendRawTransactionClusterUpdateTimings,
    pub leader_cluster_update_timings: Option<SendRawTransactionClusterUpdateTimings>,
    pub issue_order_commitment_timings: SendRawTransactionIssueOCTimings,
    pub merkle_timings: SendRawTransactionMerkleTimings,
    pub TX_put_timings: SendRawTransactionTXPutTimings,
    pub OC_put_timings: SendRawTransactionOCPutTimings,
    pub sync_epoch_TX_timings: SendRawTransactionSyncEpochTXTimings,
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
        let start_send_raw_transaction_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        let handler_start_ms = now_epoch_ms();

        let rollup = Rollup::get(&self.rollup_id)?;

        let signer_start_ms = now_epoch_ms(); // test code

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        let signer_end_ms = now_epoch_ms(); // test code

        // Get the address of the current tx orderer
        let tx_orderer_address = signer.address().clone();

        let cluster_metadata_start_ms = now_epoch_ms(); // test code
        
        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )
        .map_err(|e| {
            if e.is_none_type() {
                tracing::warn!("ClusterMetadata missing (NoneType). key=({:?}, {:?}, {:?})", rollup.platform, rollup.liveness_service_provider, rollup.cluster_id);
            } else {
                tracing::error!("ClusterMetadata get_mut failed: {:?}", e);
            }
            Error::ClusterMetadataNotFound
        })?;

        let cluster_metadata_end_ms = now_epoch_ms(); // test code

        // If the transaction is from a client, set its epoch to the ClusterMetadata's epoch
        match &mut self.raw_transaction {
            RawEpochTransaction::Eth(eth_tx) => {
                if eth_tx.epoch.is_none() { // If the transaction is from the client
                    // Set the epoch
                    eth_tx.set_epoch(mut_cluster_metadata.epoch); 
                }
                else {
                    // The transaction already has epoch/leader set (not from a client)
                    let eth_tx_epoch = eth_tx.epoch.unwrap();
                    if eth_tx_epoch > mut_cluster_metadata.epoch {
                        if mut_cluster_metadata.epoch_leader_map.get(&eth_tx_epoch).is_none() {
                            mut_cluster_metadata.epoch_leader_map.insert(eth_tx_epoch, tx_orderer_address.clone());
                        }
                    }
                }
            }
            RawEpochTransaction::EthBundle(_) => {}
        }

        // Get the address of the epoch leader node
        let epoch_leader_address = match &self.raw_transaction {
            RawEpochTransaction::Eth(eth_tx) => eth_tx
                .epoch
                .and_then(|epoch| mut_cluster_metadata.epoch_leader_map.get(&epoch).cloned()),
            RawEpochTransaction::EthBundle(_) => None,
        };

        // 현재 노드가 현재 epoch의 리더인지 확인
        let is_current_leader = match &self.raw_transaction {
            RawEpochTransaction::Eth(_) => {
                let Some(leader_addr) = epoch_leader_address.as_ref() else {
                    tracing::error!(
                        "No leader in epoch_leader_map for this transaction epoch; rollup_id={:?}",
                        self.rollup_id
                    );
                    return Err(
                        Error::GeneralError(
                            "No leader registered for this transaction epoch in cluster metadata"
                                .into(),
                        )
                        .into(),
                    );
                };
                tx_orderer_address == *leader_addr
            }
            RawEpochTransaction::EthBundle(_) => mut_cluster_metadata.is_leader,
        };

        if is_current_leader { // 현재 노드가 현재 epoch의 리더인 경우
            mut_cluster_metadata.update()?; // release the lock on ClusterMetadata

            let cluster_update_commit_ms = now_epoch_ms();

            let cluster_metadata = ClusterMetadata::get(
                rollup.platform,
                rollup.liveness_service_provider,
                &rollup.cluster_id,
            )
            .map_err(|error| {
                tracing::error!("Failed to get cluster metadata: {:?}", error);
                Error::ClusterMetadataNotFound
            })?;

            let cluster = Cluster::get(
                rollup.platform,
                rollup.liveness_service_provider,
                &rollup.cluster_id,
                cluster_metadata.platform_block_height,
            )
            .map_err(|error| {
                tracing::error!("Failed to get cluster: {:?}", error);
                Error::ClusterNotFound
            })?;

            let epoch_metadata_start_ms = now_epoch_ms(); // test code

            let mut mut_epoch_metadata = EpochMetadata::get_mut(&self.rollup_id)?;

            let epoch_metadata_end_ms = now_epoch_ms(); // test code

            // let epoch = mut_epoch_metadata.current_epoch();
            let epoch = match &self.raw_transaction {
                RawEpochTransaction::Eth(eth_tx) => eth_tx.epoch.unwrap_or(cluster_metadata.epoch),
                RawEpochTransaction::EthBundle(_) => cluster_metadata.epoch,
            };

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

            let epoch_update_commit_ms = now_epoch_ms();

            let tx_put_start_ms = now_epoch_ms();

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
            
            let tx_put_end_ms = now_epoch_ms();
            
            let merkle_start_ms = now_epoch_ms();

            let merkle_tree = context.merkle_tree_manager().get(&self.rollup_id).await?;
            let (_, pre_merkle_path) = merkle_tree.add_data(transaction_hash.as_ref()).await;
            drop(merkle_tree);

            let merkle_end_ms = now_epoch_ms();

            // rollup.order_commitment_type 뭔지 출력하는 코드
            // tracing::info!("rollup.order_commitment_type: {:?}", rollup.order_commitment_type); // 결과: Sign

            let issue_order_commitment_start_ms = now_epoch_ms();

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

            let issue_order_commitment_end_ms = now_epoch_ms();

            let OC_put_start_ms = now_epoch_ms();

            order_commitment.put(&self.rollup_id, epoch, transaction_order)?;

            let OC_put_end_ms = now_epoch_ms();

            let sync_epoch_tx_start_ms = now_epoch_ms();

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

            let sync_epoch_tx_end_ms = now_epoch_ms();

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

            let end_send_raw_transaction_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

            /*
            tracing::info!(
                "send_raw_transaction(leader) - total take time: {:?}",
                end_send_raw_transaction_time - start_send_raw_transaction_time
            );
            */

            let order_commitment = match rollup.order_commitment_type {
                OrderCommitmentType::TransactionHash => OrderCommitment::Single(
                    SingleOrderCommitment::TransactionHash(TransactionHashOrderCommitment::new(
                        transaction_hash.as_string(),
                    )),
                ),
                OrderCommitmentType::Sign => order_commitment,
            };

            let handler_end_ms = now_epoch_ms(); // test code

            Ok(SendRawTransactionResponse {
                order_commitment,
                redirect: false,
                handler_timings: SendRawTransactionHandlerTimings {
                    start_ms: handler_start_ms,
                    end_ms: handler_end_ms,
                },
                cluster_metadata_timings: SendRawTransactionClusterMetadataTimings {
                    start_ms: cluster_metadata_start_ms,
                    end_ms: cluster_metadata_end_ms,
                },
                epoch_metadata_timings: SendRawTransactionEpochMetadataTimings {
                    start_ms: epoch_metadata_start_ms,
                    end_ms: epoch_metadata_end_ms,
                },
                epoch_update_timings: SendRawTransactionEpochUpdateTimings {
                    start_ms: epoch_metadata_start_ms,
                    end_ms: epoch_update_commit_ms,
                },
                signer_timings: SendRawTransactionSignerTimings {
                    start_ms: signer_start_ms,
                    end_ms: signer_end_ms,
                },
                redirect_rpc_timings: None,
                leader_cluster_metadata_timings: None,
                cluster_update_timings: SendRawTransactionClusterUpdateTimings {
                    start_ms: cluster_metadata_start_ms,
                    end_ms: cluster_update_commit_ms,
                },
                leader_cluster_update_timings: None,
                issue_order_commitment_timings: SendRawTransactionIssueOCTimings {
                    start_ms: issue_order_commitment_start_ms,
                    end_ms: issue_order_commitment_end_ms,
                },
                merkle_timings: SendRawTransactionMerkleTimings {
                    start_ms: merkle_start_ms,
                    end_ms: merkle_end_ms,
                },
                TX_put_timings: SendRawTransactionTXPutTimings {
                    start_ms: tx_put_start_ms,
                    end_ms: tx_put_end_ms,
                },
                OC_put_timings: SendRawTransactionOCPutTimings {
                    start_ms: OC_put_start_ms,
                    end_ms: OC_put_end_ms,
                },
                sync_epoch_TX_timings: SendRawTransactionSyncEpochTXTimings {
                    start_ms: sync_epoch_tx_start_ms,
                    end_ms: sync_epoch_tx_end_ms,
                },
            })
        } else {
            // === Not the leader: forward to the leader node ===
            let epoch = match &self.raw_transaction {
                RawEpochTransaction::Eth(eth_tx) => eth_tx.epoch.unwrap_or(mut_cluster_metadata.epoch),
                RawEpochTransaction::EthBundle(_) => mut_cluster_metadata.epoch,
            };

            // mut_cluster_metadata 의 increment_sent_transaction_count 에서 지금 epoch의 트랜잭션 수를 증가시키기
            mut_cluster_metadata.increment_sent_transaction_count(epoch);

            self.sender_address = Some(tx_orderer_address.clone());

            mut_cluster_metadata.update()?; // release the lock on ClusterMetadata

            let cluster_update_commit_ms = now_epoch_ms();

            let cluster_metadata = ClusterMetadata::get(
                rollup.platform,
                rollup.liveness_service_provider,
                &rollup.cluster_id,
            )
            .map_err(|error| {
                tracing::error!("Failed to get cluster metadata: {:?}", error);
                Error::ClusterMetadataNotFound
            })?;

            let cluster = Cluster::get(
                rollup.platform,
                rollup.liveness_service_provider,
                &rollup.cluster_id,
                cluster_metadata.platform_block_height,
            )
            .map_err(|error| {
                tracing::error!("Failed to get cluster: {:?}", error);
                Error::ClusterNotFound
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

                    let redirect_rpc_start_ms = now_epoch_ms();
                    let rpc_result = context
                        .rpc_client()
                        .request::<&SendRawTransaction, SendRawTransactionResponse>(
                            leader_external_rpc_url,
                            SendRawTransaction::method(),
                            &self,
                            Id::Null,
                        )
                        .await;
                    let redirect_rpc_end_ms = now_epoch_ms();

                    match rpc_result {
                        Ok(response) => {
                            let handler_end_ms = now_epoch_ms(); // test code

                            /*
                            let end_send_raw_transaction_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_nanos();
                            tracing::info!(
                                "send_raw_transaction(non-leader) - total take time: {:?}",
                                end_send_raw_transaction_time - start_send_raw_transaction_time
                            );
                            */

                            Ok(SendRawTransactionResponse {
                                order_commitment: response.order_commitment.clone(),
                                redirect: true,
                                handler_timings: SendRawTransactionHandlerTimings {
                                    start_ms: handler_start_ms,
                                    end_ms: handler_end_ms,
                                },
                                cluster_metadata_timings: SendRawTransactionClusterMetadataTimings {
                                    start_ms: cluster_metadata_start_ms,
                                    end_ms: cluster_metadata_end_ms,
                                },
                                epoch_metadata_timings: response.epoch_metadata_timings,
                                epoch_update_timings: response.epoch_update_timings,
                                signer_timings: SendRawTransactionSignerTimings {
                                    start_ms: signer_start_ms,
                                    end_ms: signer_end_ms,
                                },
                                redirect_rpc_timings: Some(SendRawTransactionRedirectRpcTimings {
                                    start_ms: redirect_rpc_start_ms,
                                    end_ms: redirect_rpc_end_ms,
                                }),
                                leader_cluster_metadata_timings: Some(
                                    response.cluster_metadata_timings.clone(),
                                ),
                                cluster_update_timings: SendRawTransactionClusterUpdateTimings {
                                    start_ms: cluster_metadata_start_ms,
                                    end_ms: cluster_update_commit_ms,
                                },
                                leader_cluster_update_timings: Some(
                                    response.cluster_update_timings.clone(),
                                ),
                                issue_order_commitment_timings: response.issue_order_commitment_timings,
                                merkle_timings: response.merkle_timings,
                                TX_put_timings: response.TX_put_timings,
                                OC_put_timings: response.OC_put_timings,
                                sync_epoch_TX_timings: response.sync_epoch_TX_timings,
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