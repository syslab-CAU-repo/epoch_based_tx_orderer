use std::collections::BTreeSet;

use radius_sdk::signature::Address;

use crate::rpc::{external::{sync_batch_creation, sync_raw_transaction}, prelude::*};
use crate::task::finalize_batch;

use super::{EnableLeaderProcessing, SyncCanProvideEpochInfo};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendEndSignal {
    pub rollup_id: RollupId,
    pub epoch: u64,
    pub sender_address: Address,
}

impl RpcParameter<AppState> for SendEndSignal {
    type Response = ();

    fn method() -> &'static str {
        "send_end_signal"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        let rollup = Rollup::get(&self.rollup_id).map_err(|e| {
            tracing::error!("Failed to retrieve rollup: {:?}", e);
            Error::RollupNotFound
        })?;

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        ).map_err(|e| {
            tracing::error!(
                "Failed to retrieve cluster metadata for rollup_id: {:?}, cluster_id: {:?}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                e
            );
            e
        })?;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            cluster_metadata.platform_block_height,
        ).map_err(|e| {
            tracing::error!(
                "Failed to retrieve cluster for rollup_id: {:?}, cluster_id: {:?}, platform_block_height: {}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                cluster_metadata.platform_block_height,
                e
            );
            e
        })?;

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;
        let tx_orderer_address = signer.address().clone();

        // 현재 노드의 cluster RPC URL 가져오기
        let current_node_rpc_info = cluster.get_tx_orderer_rpc_info(&tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "Failed to get RPC info for current node. tx_orderer_address: {:?}",
                    tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        let current_node_cluster_rpc_url = current_node_rpc_info.cluster_rpc_url.clone()
            .ok_or_else(|| {
                tracing::error!(
                    "Cluster RPC URL not found for current node. tx_orderer_address: {:?}",
                    tx_orderer_address
                );
                Error::GeneralError("Cluster RPC URL not found".into())
            })?;

        let epoch_leader_matches = cluster_metadata
            .epoch_leader_map
            .get(&self.epoch)
            .map(|leader| *leader == tx_orderer_address)
            .unwrap_or(false);

        if !epoch_leader_matches {
            tracing::error!(
                "Received end_signal but current node is not the epoch's leader. rollup_id: {:?}, epoch: {}, sender_address: {:?}, current_address: {:?}, epoch_leader: {:?}",
                self.rollup_id,
                self.epoch,
                self.sender_address,
                tx_orderer_address,
                cluster_metadata.epoch_leader_map.get(&self.epoch)
            );
            return Err(Error::GeneralError("Not a leader node".into()).into());
        }
        
        let node_index = cluster.tx_orderer_rpc_infos.iter().find_map(|(index, info)| {
            if info.tx_orderer_address == self.sender_address {
                Some(*index)
            } else {
                None
            }
        }).ok_or_else(|| {
            tracing::error!(
                "Failed to find node index for sender_address: {:?} in cluster. rollup_id: {:?}, epoch: {}",
                self.sender_address,
                self.rollup_id,
                self.epoch
            );
            Error::GeneralError("Sender address not found in cluster".into())
        })?;

        // end_signal_bitmap 업데이트
        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        ).map_err(|e| {
            tracing::error!(
                "Failed to get mutable cluster metadata for rollup_id: {:?}, cluster_id: {:?}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                e
            );
            e
        })?;

        // 노드 인덱스에 대응되는 비트 설정
        mut_cluster_metadata.set_node_bit(self.epoch, node_index);

        // 모든 노드가 시그널을 보냈는지 확인
        let total_nodes = cluster.tx_orderer_rpc_infos.len();
        let mut epoch_completed = false;
        if mut_cluster_metadata.all_nodes_sent_signal(self.epoch, total_nodes) {
            epoch_completed = true;
            CanProvideEpochInfo::add_completed_epoch(&self.rollup_id, self.epoch).map_err(|e| {
                tracing::error!(
                    "Failed to add completed epoch to CanProvideEpochInfo. rollup_id: {:?}, epoch: {}, error: {:?}",
                    self.rollup_id,
                    self.epoch,
                    e
                );
                e
            })?;
        }

        tracing::info!("SendEndSignal handler() - epoch completed: (epoch: {:?}, completed: {:?})", self.epoch, mut_cluster_metadata.all_nodes_sent_signal(self.epoch, total_nodes)); // test code

        mut_cluster_metadata.update().map_err(|e| {
            tracing::error!(
                "Failed to update cluster metadata. rollup_id: {:?}, cluster_id: {:?}, epoch: {}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                self.epoch,
                e
            );
            e
        })?;

        sync_can_provide_epoch_info(
            context.clone(),
            cluster.clone(),
            self.rollup_id.clone(),
            self.epoch,
            current_node_cluster_rpc_url,
        );

        /*
        if epoch_completed {
            let context = context.clone();
            let rollup_id = self.rollup_id.clone();
            let epoch = self.epoch;
            let can_provide_epoch_info = CanProvideEpochInfo::get(&self.rollup_id)?;
            tokio::spawn(async move {
                if let Err(e) = create_batches_from_epoch(
                    context,
                    &rollup_id,
                    cluster,
                    can_provide_epoch_info,
                ).await {
                    tracing::error!(
                        "Failed to create batches from epoch. rollup_id: {:?}, epoch: {}, error: {:?}",
                        rollup_id, epoch, e
                    );
                }
            });
        }
        */

        Ok(())
    }
}

pub fn sync_can_provide_epoch_info(
    context: AppState,
    cluster: Cluster,
    rollup_id: RollupId,
    epoch: u64,
    current_node_cluster_rpc_url: String,
) {
    // tracing::info!("=== 🔄🕐 sync_can_provide_epoch_info 시작(epoch: {:?}) 🕐🔄 ===", epoch); // test code

    let mut other_cluster_rpc_url_list = cluster.get_other_cluster_rpc_url_list();
    if other_cluster_rpc_url_list.is_empty() {
        tracing::info!("No cluster RPC URLs available for synchronization");
        return;
    }

    other_cluster_rpc_url_list = other_cluster_rpc_url_list
        .into_iter()
        .filter(|rpc_url| rpc_url != &current_node_cluster_rpc_url)
        .collect();

    let parameter = SyncCanProvideEpochInfo {
        epoch,
        rollup_id,
    };

    tokio::spawn(async move {
        let _ = context
            .rpc_client()
            .fire_and_forget_multicast(
                other_cluster_rpc_url_list,
                SyncCanProvideEpochInfo::method(),
                &parameter,
                Id::Null,
            )
            .await;
    });

    // tracing::info!("=== 🔄🕐 sync_can_provide_epoch_info 종료(epoch: {:?}) 🕐🔄 ===", epoch); // test codes
}

