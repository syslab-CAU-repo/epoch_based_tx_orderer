use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use radius_sdk::{json_rpc::client::Priority, signature::Address};
use tokio::{sync::mpsc::UnboundedReceiver, time::Instant};

use super::{send_end_signal_to_epoch_leader, SyncLeaderTxOrderer};
use crate::{
    rpc::{
        cluster::{GetOrderCommitmentInfo, GetOrderCommitmentInfoResponse, SyncEpochMetadata},
        external::sync_raw_transaction,
        prelude::*,
    },
    task::{finalize_batch, send_transaction_list_to_mev_searcher, MevTargetTransaction},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRawTransactionList {
    pub leader_change_message: LeaderChangeMessage,
    pub rollup_signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LeaderChangeMessage {
    pub rollup_id: RollupId,
    pub executor_address: Address,
    pub platform_block_height: u64,

    pub current_leader_tx_orderer_address: Address,
    pub next_leader_tx_orderer_address: Address,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignMessage {
    pub rollup_id: RollupId,
    pub executor_address: String,
    pub platform_block_height: u64,

    pub current_leader_tx_orderer_address: Address,
    pub next_leader_tx_orderer_address: Address,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GetRawTransactionListResponse {
    pub raw_transaction_list: Vec<String>,
}

impl RpcParameter<AppState> for GetRawTransactionList {
    type Response = GetRawTransactionListResponse;

    fn method() -> &'static str {
        "get_raw_transaction_list"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        /*
        let start_get_raw_transaction_list_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();
        */

        /*
        tracing::info!("================================="); // test code
        tracing::info!("[get_raw_transaction_list]: start"); // test code
        */

        let mut raw_transaction_list = Vec::new();

        let rollup_id = self.leader_change_message.rollup_id.clone();

        let rollup = Rollup::get(&rollup_id)?;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            self.leader_change_message.platform_block_height,
        )?;
        
        let can_provide_epoch_info = match CanProvideEpochInfo::get(&rollup_id) {
            Ok(info) => info,
            Err(err) => {
                tracing::warn!(
                    "CanProvideEpochInfo not found - rollup_id: {:?}, error: {:?}. Using default.",
                    rollup_id,
                    err,
                );
                CanProvideEpochInfo::default()
            }
        };
        
        if let Err(e) = create_batches_from_epoch(
            context.clone(),
            &rollup_id,
            cluster.clone(),
            can_provide_epoch_info,
            self.leader_change_message.next_leader_tx_orderer_address.clone(),
        ).await {
            tracing::error!("Failed to create batches from epoch - rollup_id: {:?}, error: {:?}", rollup_id, e);
        }

        let rollup_metadata = match RollupMetadata::get(&rollup_id) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::error!(
                    "Failed to get rollup metadata - rollup_id: {:?} / error: {:?}",
                    rollup_id,
                    err,
                );

                return Ok(GetRawTransactionListResponse {
                    raw_transaction_list: Vec::new(),
                });
            }
        };

        let start_batch_number = rollup_metadata.provided_batch_number;
        let mut current_provided_batch_number = start_batch_number;
        let mut current_provided_transaction_order = rollup_metadata.provided_transaction_order;

        // tracing::info!("get_raw_transaction_list - (before)current_provided_batch_number: {:?}", current_provided_batch_number); // test code
        // tracing::info!("get_raw_transaction_list - (before)current_provided_transaction_order: {:?}", current_provided_transaction_order); // test code
        // let mut iteration_count = 0; // test code

        while let Ok(batch) = Batch::get(&rollup_id, current_provided_batch_number) {
            // tracing::info!("get_raw_transaction_list - *** {:?}th batch interation(Batch 번호: {:?}) ***", iteration_count, current_provided_batch_number); // test code

            let start_transaction_order = if current_provided_batch_number == start_batch_number {
                current_provided_transaction_order + 1
            } else {
                0
            };

            raw_transaction_list.extend(extract_raw_transactions(
                batch,
                start_transaction_order as u64,
            ));

            current_provided_batch_number += 1;
            current_provided_transaction_order = -1;

            // iteration_count += 1; // test code
        }

        // tracing::info!("get_raw_transaction_list - (after)current_provided_batch_number: {:?}", current_provided_batch_number); // test code
        // tracing::info!("get_raw_transaction_list - (after)current_provided_transaction_order: {:?}", current_provided_transaction_order); // test code

        if let Ok(can_provide_transaction_info) = CanProvideTransactionInfo::get(&rollup_id) {
            if let Some(can_provide_transaction_orderers) = can_provide_transaction_info
                .can_provide_transaction_orders_per_batch
                .get(&current_provided_batch_number)
            {
                let valid_end_transaction_order = get_last_valid_transaction_order(
                    can_provide_transaction_orderers,
                    current_provided_transaction_order,
                );

                // tracing::info!("get_raw_transaction_list - current_provided_batch_number: {:?} / valid_end_transaction_order: {:?}", current_provided_batch_number, valid_end_transaction_order); // test code

                fetch_and_append_transactions(
                    &rollup_id,
                    current_provided_batch_number,
                    (current_provided_transaction_order + 1) as u64,
                    valid_end_transaction_order,
                    &mut raw_transaction_list,
                )?;

                current_provided_transaction_order = valid_end_transaction_order;

                if current_provided_transaction_order
                    == rollup.max_transaction_count_per_batch as i64 - 1
                {
                    current_provided_batch_number += 1;
                    current_provided_transaction_order = -1;
                }
            }
        }

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&rollup_id)?;

        let mut batch_number_list_to_delete = Vec::new();
        for batch_number in start_batch_number..current_provided_batch_number {
            batch_number_list_to_delete.push(batch_number);
        }

        mut_rollup_metadata.provided_batch_number = current_provided_batch_number;
        mut_rollup_metadata.provided_transaction_order = current_provided_transaction_order;

        let leader_tx_orderer_rpc_info = cluster
            .get_tx_orderer_rpc_info(&self.leader_change_message.next_leader_tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "TxOrderer RPC info not found for address {:?}",
                    self.leader_change_message.next_leader_tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;

        let tx_orderer_address = signer.address().clone();

        let is_next_leader =
            tx_orderer_address == self.leader_change_message.next_leader_tx_orderer_address;

        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        if mut_cluster_metadata.is_leader == false {
            if let Some(current_leader_tx_orderer_rpc_info) =
                mut_cluster_metadata.leader_tx_orderer_rpc_info.clone()
            {
                let current_leader_tx_orderer_cluster_rpc_url = current_leader_tx_orderer_rpc_info
                    .cluster_rpc_url
                    .clone()
                    .unwrap();

                let parameter = GetOrderCommitmentInfo {
                    rollup_id: self.leader_change_message.rollup_id.clone(),
                };

                match context
                    .rpc_client()
                    .request_with_priority::<&GetOrderCommitmentInfo, GetOrderCommitmentInfoResponse>(
                        current_leader_tx_orderer_cluster_rpc_url.clone(),
                        GetOrderCommitmentInfo::method(),
                        &parameter,
                        Id::Null,
                        Priority::High,
                    )
                    .await
                {
                    Ok(response) => {

                        /*
                        tracing::info!(
                            "Get order commitment info - current leader external rpc response: {:?}", // test code
                            response
                        );
                        */

                        mut_rollup_metadata.batch_number = response.batch_number;
                        mut_rollup_metadata.transaction_order = response.transaction_order;
                    }
                    Err(error) => {
                        tracing::error!(
                            "Get order commitment info - current leader external rpc error: {:?}",
                            error
                        );
                    }
                }
            } else {
                tracing::warn!(
                    "Current leader tx orderer RPC info not found for address {:?}",
                    self.leader_change_message.current_leader_tx_orderer_address
                );
            }
        }

        mut_cluster_metadata.platform_block_height =
            self.leader_change_message.platform_block_height;
        mut_cluster_metadata.is_leader = is_next_leader;
        mut_cluster_metadata.leader_tx_orderer_rpc_info = Some(leader_tx_orderer_rpc_info.clone());

        let old_epoch = mut_cluster_metadata.epoch;

        // tracing::info!("[get_raw_transaction_list]: old_epoch: {:?}", old_epoch); // test code

        let epoch_leader_cluster_rpc_url = cluster
            .get_tx_orderer_rpc_info(&self.leader_change_message.current_leader_tx_orderer_address)
            .and_then(|info| info.cluster_rpc_url)
            .ok_or_else(|| {
                tracing::error!(
                    "cluster_rpc_url not found for epoch leader {:?} (old_epoch: {})",
                    self.leader_change_message.current_leader_tx_orderer_address,
                    old_epoch
                );
                Error::GeneralError("epoch leader cluster_rpc_url not found".into())
            })?;

        // old_epoch의 리더 RPC URL을 epoch_leader_map에 저장 (이미 존재하지 않을 때만)
        if !mut_cluster_metadata.epoch_leader_map.contains_key(&old_epoch) {
            // tracing::info!("old_epoch의 리더 RPC URL을 epoch_leader_map에 저장 (이미 존재하지 않을 때만)"); // test code
            mut_cluster_metadata.epoch_leader_map.insert(old_epoch, self.leader_change_message.current_leader_tx_orderer_address.clone());
        }
        mut_cluster_metadata.epoch = old_epoch + 1;

        let new_epoch = mut_cluster_metadata.epoch;

        // tracing::info!("[get_raw_transaction_list]: new_epoch: {:?}", new_epoch); // test code

        // new_epoch의 리더 RPC URL을 epoch_leader_map에 저장
        mut_cluster_metadata.epoch_leader_map.insert(new_epoch, self.leader_change_message.next_leader_tx_orderer_address.clone());

        let epoch_metadata = EpochMetadata::get(&rollup_id)?;

        sync_leader_tx_orderer(
            context.clone(),
            cluster,
            self.leader_change_message.clone(),
            self.rollup_signature,
            mut_rollup_metadata.batch_number,
            mut_rollup_metadata.transaction_order,
            mut_rollup_metadata.provided_batch_number,
            mut_rollup_metadata.provided_transaction_order,
            old_epoch,
            new_epoch,
            epoch_metadata,
        )
        .await;

        let epoch_sent_transaction_count = mut_cluster_metadata.epoch_sent_transaction_count.get(&old_epoch).copied().unwrap_or(0);

        send_end_signal_to_epoch_leader(
            context.clone(),
            rollup_id.clone(),
            old_epoch,
            epoch_leader_cluster_rpc_url,
            epoch_sent_transaction_count,
        );

        mut_cluster_metadata.update()?;
        let _ = mut_rollup_metadata.update().map_err(|error| {
            tracing::error!(
                "rollup_metadata update error - rollup id: {:?}, error: {:?}",
                self.leader_change_message.rollup_id,
                error
            );
        });

        /*
        let end_get_raw_transaction_list_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        tracing::info!(
            "get_raw_transaction_list - total take time: {:?}",
            end_get_raw_transaction_list_time - start_get_raw_transaction_list_time
        );
        */

        let shared_channel_infos = context.shared_channel_infos();
        let mev_searcher_infos = MevSearcherInfos::get_or(MevSearcherInfos::default).unwrap();

        send_transaction_list_to_mev_searcher(
            &rollup_id,
            raw_transaction_list.clone(),
            shared_channel_infos,
            &mev_searcher_infos,
        );

        let ip_list = mev_searcher_infos.get_ip_list_by_rollup_id(&rollup_id);
        let receivers: Vec<Arc<tokio::sync::Mutex<UnboundedReceiver<MevTargetTransaction>>>> = {
            let map = shared_channel_infos.lock().unwrap();
            ip_list
                .iter()
                .filter_map(|ip| map.get(ip).map(|(_, rx)| Arc::clone(rx)))
                .collect()
        };

        let collected_mev_target_transaction = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let mut sub_tasks = vec![];

        for receiver in receivers {
            let collected_clone = Arc::clone(&collected_mev_target_transaction);
            let rx = Arc::clone(&receiver);

            let sub_task = tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_millis(5000);

                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {}
                    maybe_mev_target_transaction = async {
                        let mut guard = rx.lock().await;
                        guard.recv().await
                    } => {
                        if let Some(mev_target_transaction) = maybe_mev_target_transaction {
                            // tracing::info!("Received mev target transaction: {:?}", mev_target_transaction);
                            collected_clone.lock().await.push(mev_target_transaction);
                        }
                    }
                }
            });

            sub_tasks.push(sub_task);
        }

        let _ = futures::future::join_all(sub_tasks).await;

        {
            let result = collected_mev_target_transaction.lock().await;
            // tracing::info!("Collected mev target transactions: {:?}", *result); // test code

            for mev_target_transaction in result.iter() {
                raw_transaction_list
                    .extend(mev_target_transaction.backrunning_transaction_list.clone());
            }
        }

        /*
        tracing::info!("[get_raw_transaction_list]: end"); // test code
        tracing::info!("================================="); // test code
        */

        Ok(GetRawTransactionListResponse {
            raw_transaction_list,
        })
    }
}

pub async fn sync_leader_tx_orderer(
    context: AppState,
    cluster: Cluster,
    leader_change_message: LeaderChangeMessage,
    rollup_signature: Signature,
    batch_number: u64,
    transaction_order: u64,
    provided_batch_number: u64,
    provided_transaction_order: i64,
    old_epoch: u64, 
    new_epoch: u64,
    epoch_metadata: EpochMetadata,
) {
    let mut other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
    if other_cluster_rpc_url_list.is_empty() {
        // tracing::info!("No cluster RPC URLs available for synchronization"); // test code
        return;
    }

    if let Some(next_leader_tx_orderer_rpc_info) =
        cluster.get_tx_orderer_rpc_info(&leader_change_message.next_leader_tx_orderer_address)
    {
        let next_leader_tx_orderer_cluster_rpc_url = next_leader_tx_orderer_rpc_info
            .cluster_rpc_url
            .clone()
            .unwrap();

        // Filter out the next leader's cluster URL from the list
        other_cluster_rpc_url_list = other_cluster_rpc_url_list
            .into_iter()
            .filter(|rpc_url| rpc_url != &next_leader_tx_orderer_cluster_rpc_url)
            .collect();

        let parameter = SyncLeaderTxOrderer {
            leader_change_message:      leader_change_message.clone(),
            rollup_signature:           rollup_signature,
            batch_number:               batch_number,
            transaction_order:          transaction_order,
            provided_batch_number:      provided_batch_number,
            provided_transaction_order: provided_transaction_order,
            old_epoch:                  old_epoch, 
            new_epoch:                  new_epoch,
            epoch_metadata:             epoch_metadata,
        };

        let current_leader_tx_orderer_address = leader_change_message.current_leader_tx_orderer_address.clone();

        if next_leader_tx_orderer_rpc_info.tx_orderer_address != current_leader_tx_orderer_address {
            // Directly request the next leader tx_orderer to sync
            /*
            let start_sync_leader_tx_order_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();
            */

            let _result: Result<(), radius_sdk::json_rpc::client::RpcClientError> = context
                .rpc_client()
                .request_with_priority(
                    next_leader_tx_orderer_cluster_rpc_url.clone(),
                    SyncLeaderTxOrderer::method(),
                    &parameter,
                    Id::Null,
                    Priority::High,
                )
                .await;

            /*
            let end_sync_leader_tx_order_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos();

            tracing::info!(
                "SyncLeaderTxOrderer - start: {:?} / end: {:?} / gap: {:?} / next_leader_tx_orderer_cluster_rpc_url: {:?}, parameter: {:?}",
                start_sync_leader_tx_order_time,
                end_sync_leader_tx_order_time,
                end_sync_leader_tx_order_time - start_sync_leader_tx_order_time,
                next_leader_tx_orderer_cluster_rpc_url,
                parameter
            );
            */

            // Fire and forget to the rest of the cluster nodes asynchronously
            let urls = other_cluster_rpc_url_list.clone();
            tokio::spawn(async move {
                let _ = context
                    .rpc_client()
                    .fire_and_forget_multicast(
                        urls,
                        SyncLeaderTxOrderer::method(),
                        &parameter,
                        Id::Null,
                    )
                    .await;
            });
        }
    } else {
        tracing::error!(
            "Next leader tx orderer RPC info not found for address {:?}",
            leader_change_message.next_leader_tx_orderer_address
        );
    }
}

fn extract_raw_transactions(batch: Batch, start_transaction_order: u64) -> Vec<String> {
    batch
        .raw_transaction_list
        .into_iter()
        .enumerate()
        .filter_map(|(i, transaction)| {
            if (i as u64) >= start_transaction_order {
                Some(match transaction {
                    RawTransaction::Eth(EthRawTransaction(data)) => data,
                    RawTransaction::EthBundle(EthRawBundleTransaction(data)) => data,
                })
            } else {
                None
            }
        })
        .collect()
}

fn get_last_valid_transaction_order(
    can_provide_transaction_orders: &BTreeSet<u64>,
    provided_transaction_order: i64,
) -> i64 {
    let mut last_valid_transaction_order = provided_transaction_order;

    for &transaction_order in can_provide_transaction_orders {
        let transaction_order = transaction_order as i64;

        if transaction_order == last_valid_transaction_order + 1 {
            last_valid_transaction_order += 1;
        } else if transaction_order > last_valid_transaction_order {
            break;
        }
    }

    last_valid_transaction_order as i64
}

fn fetch_and_append_transactions(
    rollup_id: &RollupId,
    batch_number: u64,
    start_transaction_order: u64,
    last_valid_transaction_order: i64,
    raw_transaction_list: &mut Vec<String>,
) -> Result<(), RpcError> {
    if last_valid_transaction_order < start_transaction_order as i64 {
        return Ok(());
    }

    for transaction_order in
        start_transaction_order..=last_valid_transaction_order.try_into().unwrap()
    {
        let (raw_transaction, _) =
            RawTransactionModel::get(rollup_id, batch_number, transaction_order)?;
        let raw_transaction = match raw_transaction {
            RawTransaction::Eth(EthRawTransaction(data)) => data,
            RawTransaction::EthBundle(EthRawBundleTransaction(data)) => data,
        };
        raw_transaction_list.push(raw_transaction);
    }
    Ok(())
}

// 1. 함수 목적 : current_leader가 현 시점에 처리 완료된 epoch까지 가져와서 batch 기반으로 다시 ordering 하는 함수
// 2. 입력값 : can_provide_epoch_info, next_leader_tx_orderer_address
// 3. 출력값 : 없음
// 4. side effects : get_raw_transaction_list 요청을 받는 노드가 current_leader라고 가정하고 만들었음. 
//      current_leader가 아닌 노드가 get_raw_transaction_list 요청을 받아 이 함수가 실행되면 결과 장담 X
async fn create_batches_from_epoch(
    context: AppState,
    rollup_id: &RollupId,
    cluster: Cluster,
    can_provide_epoch_info: CanProvideEpochInfo,
    next_leader_tx_orderer_address: Address,
) -> Result<(), Error> {
    let epoch_metadata = EpochMetadata::get(rollup_id)?;

    // tracing::info!("    [create_batches_from_epoch]: epoch_metadata.epoch_transaction_orders: {:?}", epoch_metadata.epoch_transaction_orders); // test code

    let last_batched_epoch = epoch_metadata.last_batched_epoch;

    // CanProvideEpochInfo에 들어 있는 epoch 중에서 연속된 epoch까지만 가져옴
    // 예를 들어, CanProvideEpochInfo에 들어 있는 epoch가 [1, 2, 3, 4, 5, 6, 7, 8, 10]이고 last_batched_epoch가 6이면,
    // get_consecutive_epochs 함수는 [7, 8]을 반환함
    let epochs_to_process = get_consecutive_epochs(
        &can_provide_epoch_info.completed_epoch,
        last_batched_epoch.unwrap_or(0),
    );

    // tracing::info!("    [create_batches_from_epoch]: last_batched_epoch: {:?}", last_batched_epoch); // test code
    // tracing::info!("    [create_batches_from_epoch]: epochs_to_process: {:?}", epochs_to_process); // test code

    let mut epochs_ok_to_process = Vec::new();

    // 각 epoch의 트랜잭션이 RawEpochTransactionModel에 모두 존재하는지 확인
    // 존재하는 epoch는 epochs_ok_to_process에 추가
    // 존재하지 않는 epoch가 나오면 for 문을 종료(해당 epoch와 그 이후의 epoch는 이번 get_raw_transaction_list에서 처리하지 않음)
    for epoch in &epochs_to_process {
        let mut ok_to_process = true;
        for epoch_tx_order in 0..epoch_metadata.transaction_order(*epoch) {
            let (_raw_epoch_transaction, _is_direct_sent) =
                match RawEpochTransactionModel::get(rollup_id, *epoch, epoch_tx_order) {
                    Ok(data) => data,
                    Err(_) => {
                        ok_to_process = false;
                        break;
                    },
                };
        }

        if ok_to_process {
            epochs_ok_to_process.push(*epoch);
        } else {
            break;
        }
    }

    // tracing::info!("    [create_batches_from_epoch]: epochs_ok_to_process: {:?}", epochs_ok_to_process); // test code

    let mut mut_epoch_metadata = EpochMetadata::get_mut(rollup_id)?;
    let mut mut_rollup_metadata = RollupMetadata::get_mut(rollup_id)?;

    if epochs_ok_to_process.is_empty() {
        mut_epoch_metadata.update()?;
        mut_rollup_metadata.update()?;
        return Ok(());
    }

    for epoch in &epochs_ok_to_process {
        for epoch_tx_order in 0..mut_epoch_metadata.transaction_order(*epoch) {
            let (raw_epoch_transaction, _is_direct_sent) =
                match RawEpochTransactionModel::get(rollup_id, *epoch, epoch_tx_order) {
                    Ok(data) => data,
                    Err(e) => return Err(e.into()),
                };

            let batch_number = mut_rollup_metadata.batch_number;
            let batch_tx_order = mut_rollup_metadata.transaction_order;

            // RawEpochTransaction to RawTransaction
            let raw_transaction = match raw_epoch_transaction {
                RawEpochTransaction::Eth(eth) => {
                    RawTransaction::Eth(EthRawTransaction::from(eth.raw_transaction))
                }
                RawEpochTransaction::EthBundle(EthRawEpochBundleTransaction(data)) => {
                    RawTransaction::EthBundle(EthRawBundleTransaction::from(data))
                }
            };

            RawTransactionModel::put(
                rollup_id,
                batch_number,
                batch_tx_order,
                raw_transaction.clone(),
                true,
            )?;

            mut_rollup_metadata.transaction_order += 1;

            CanProvideTransactionInfo::add_can_provide_transaction_orders(
                rollup_id,
                batch_number,
                vec![batch_tx_order],
            )?;

            let is_updated = mut_rollup_metadata.check_and_update_batch_info();

            if is_updated {
                context
                    .merkle_tree_manager()
                    .insert(rollup_id, MerkleTree::new())
                    .await;

                finalize_batch(context.clone(), &rollup_id, batch_number);
            }

            let order_commitment = OrderCommitment::get(rollup_id, *epoch, epoch_tx_order)?;

            sync_raw_transaction(
                context.clone(),
                cluster.clone(),
                rollup_id.clone(),
                batch_number,
                batch_tx_order,
                raw_transaction.clone(),
                order_commitment.clone(),
                true,
            );
        }

        mut_epoch_metadata.last_batched_epoch = Some(*epoch);
    }

    /*
    let final_batch_number = mut_rollup_metadata.batch_number;
    let final_tx_order = mut_rollup_metadata.transaction_order;
    let final_last_epoch = mut_epoch_metadata.last_batched_epoch;
    */

    let last_batched_epoch_after_update = mut_epoch_metadata.last_batched_epoch.unwrap();

    mut_epoch_metadata.update()?;
    mut_rollup_metadata.update()?;

    sync_epoch_metadata(
        context.clone(),
        rollup_id.clone(),
        cluster.clone(),
        last_batched_epoch_after_update,
        next_leader_tx_orderer_address.clone(),
    )
    .await?;

    /*
    tracing::info!(
        "create_batches_from_epoch - rollup_id: {:?}, epochs: {:?}, batch: {}, tx_order: {}, last_batched_epoch: {:?}",
        rollup_id,
        epochs_to_process,
        final_batch_number,
        final_tx_order,
        final_last_epoch,
    );
    */

    Ok(())
}

fn get_consecutive_epochs(
    completed_epoch: &BTreeSet<u64>,
    last_batched_epoch: u64,
) -> Vec<u64> {
    let mut result = Vec::new();
    let mut expected = last_batched_epoch + 1;

    /*
    tracing::info!(
        "get_consecutive_epochs - last_batched_epoch: {}, completed_epoch: {:?}, result (before loop): {:?}",
        last_batched_epoch,
        completed_epoch,
        result,
    );
    */

    for &epoch in completed_epoch {
        if epoch == expected {
            result.push(epoch);
            expected += 1;
        } else if epoch > expected {
            break;
        }
    }

    /*
    tracing::info!(
        "get_consecutive_epochs - last_batched_epoch: {}, completed_epoch: {:?}, result (after loop): {:?}",
        last_batched_epoch,
        completed_epoch,
        result,
    );
    */

    result
}

// 1. 함수 목적: current_leader가 epoch-based에서 batch-based로 다시 오더링한 후, 갱신된 last_batched_epoch를 다른 노드들에게 전파하는 함수
// 2. 입력값: last_batched_epoch, next_leader_tx_orderer_address
// 3. 출력값: 없음
// 4. side effects: current_leader가 아닌 노드가 get_raw_transaction_list 요청을 받아 이 함수가 실행되면 결과 장담 X
pub async fn sync_epoch_metadata(
    context: AppState,
    rollup_id: RollupId,
    cluster: Cluster,
    last_batched_epoch: u64,
    next_leader_tx_orderer_address: Address,
) -> Result<(), Error> {
    let mut other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
    if other_cluster_rpc_url_list.is_empty() {
        // tracing::info!("        [sync_epoch_metadata]: No cluster RPC URLs available for synchronization");
        return Err(Error::GeneralError("No cluster RPC URLs available for synchronization".into()));
    }

    if let Some(next_leader_tx_orderer_rpc_info) =
        cluster.get_tx_orderer_rpc_info(&next_leader_tx_orderer_address)
    {
        // tracing::info!("        [sync_epoch_metadata]: next_leader_tx_orderer_rpc_info found"); // test code

        let next_leader_tx_orderer_cluster_rpc_url = next_leader_tx_orderer_rpc_info
                .cluster_rpc_url
                .clone()
                .unwrap();

        // tracing::info!("        [sync_epoch_metadata]: next_leader_tx_orderer_cluster_rpc_url: {:?}", next_leader_tx_orderer_cluster_rpc_url); // test code

        // Filter out the next leader's cluster URL from the list
        other_cluster_rpc_url_list = other_cluster_rpc_url_list
            .into_iter()
            .filter(|rpc_url| rpc_url != &next_leader_tx_orderer_cluster_rpc_url)
            .collect();

        // tracing::info!("        [sync_epoch_metadata]: other_cluster_rpc_url_list: {:?}", other_cluster_rpc_url_list); // test code

        let parameter = SyncEpochMetadata {
            last_batched_epoch,
            rollup_id,
        };

        /*
        tracing::info!(
            "        [sync_epoch_metadata]: sending to next_leader url={} method={} rollup_id={:?} last_batched_epoch={}",
            next_leader_tx_orderer_cluster_rpc_url,
            SyncEpochMetadata::method(),
            parameter.rollup_id,
            parameter.last_batched_epoch,
        );
        */

        let next_leader_result = context
            .rpc_client()
            .request_with_priority(
                next_leader_tx_orderer_cluster_rpc_url.clone(),
                SyncEpochMetadata::method(),
                &parameter,
                Id::Null,
                Priority::High,
            )
            .await;

        match &next_leader_result {
            Ok(()) => {
                // tracing::info!("        [sync_epoch_metadata]: next_leader request finished ok url={}", next_leader_tx_orderer_cluster_rpc_url)
            },
            Err(e) => {
                // tracing::warn!("        [sync_epoch_metadata]: next_leader request failed url={} error={:?}", next_leader_tx_orderer_cluster_rpc_url, e)
            },
        }

        // Fire and forget to the rest of the cluster nodes asynchronously
        let urls = other_cluster_rpc_url_list.clone();
        let multicast_count = urls.len();

        tokio::spawn(async move {
            /*
            tracing::info!(
                "        [sync_epoch_metadata]: sending multicast method={} count={} urls={:?} rollup_id={:?} last_batched_epoch={}",
                SyncEpochMetadata::method(),
                multicast_count,
                urls,
                parameter.rollup_id,
                parameter.last_batched_epoch,
            );
            */

            context
                .rpc_client()
                .fire_and_forget_multicast(
                    urls,
                    SyncEpochMetadata::method(),
                    &parameter,
                    Id::Null,
                )
                .await;

            /*
            tracing::info!(
                "        [sync_epoch_metadata]: multicast fire_and_forget dispatch completed count={} (per-URL RPC results are not awaited)",
                multicast_count
            );
            */
        });
    }

    Ok(())
}