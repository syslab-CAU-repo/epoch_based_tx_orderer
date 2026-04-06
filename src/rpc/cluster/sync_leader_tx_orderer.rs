use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use radius_sdk::json_rpc::server::ProcessPriority;

use super::LeaderChangeMessage;
use crate::rpc::prelude::*;

use crate::rpc::cluster::SendEndSignal;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncLeaderTxOrderer {
    pub leader_change_message: LeaderChangeMessage,
    pub rollup_signature: Signature,

    pub batch_number: u64,
    pub transaction_order: u64,

    pub provided_batch_number: u64,
    pub provided_transaction_order: i64,

    pub old_epoch: u64,
    pub new_epoch: u64,

    pub epoch_metadata: EpochMetadata,
}

impl RpcParameter<AppState> for SyncLeaderTxOrderer {
    type Response = ();

    fn method() -> &'static str {
        "sync_leader_tx_orderer"
    }

    fn priority(&self) -> ProcessPriority {
        ProcessPriority::High
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        let rollup_id = self.leader_change_message.rollup_id.clone();

        /*
        let start_sync_leader_tx_orderer_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();
        */

        let rollup = Rollup::get(&rollup_id).map_err(|e| {
            tracing::error!("Failed to retrieve rollup: {:?}", e);
            Error::RollupNotFound
        })?;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            self.leader_change_message.platform_block_height,
        )?;

        let signer = context.get_signer(rollup.platform).await.map_err(|_| {
            tracing::error!("Signer not found for platform {:?}", rollup.platform);
            Error::SignerNotFound
        })?;
        let tx_orderer_address = signer.address().clone();
        let is_leader =
            tx_orderer_address == self.leader_change_message.next_leader_tx_orderer_address;

        let leader_tx_orderer_rpc_info = cluster
            .get_tx_orderer_rpc_info(&self.leader_change_message.next_leader_tx_orderer_address)
            .ok_or_else(|| {
                tracing::error!(
                    "TxOrderer RPC info not found for address {:?}",
                    self.leader_change_message.next_leader_tx_orderer_address
                );
                Error::TxOrdererInfoNotFound
            })?;

        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        mut_cluster_metadata.platform_block_height =
            self.leader_change_message.platform_block_height;
        mut_cluster_metadata.is_leader = is_leader;
        mut_cluster_metadata.leader_tx_orderer_rpc_info = Some(leader_tx_orderer_rpc_info.clone());

        // old_epoch → leader address (only if not already recorded)
        if !mut_cluster_metadata.epoch_leader_map.contains_key(&self.old_epoch) {
            mut_cluster_metadata.epoch_leader_map.insert(
                self.old_epoch,
                self.leader_change_message
                    .current_leader_tx_orderer_address
                    .clone(),
            );
        }

        mut_cluster_metadata.epoch = self.new_epoch;

        if let Some(provisional_leader) = mut_cluster_metadata.epoch_leader_map.get(&self.new_epoch) {
            if *provisional_leader != self.leader_change_message.next_leader_tx_orderer_address {
                tracing::error!(
                    "Epoch leader mismatch for epoch {}: provisionally registered {:?}, but actual leader is {:?}; rollup_id={:?}",
                    self.new_epoch,
                    provisional_leader,
                    self.leader_change_message.next_leader_tx_orderer_address,
                    rollup_id,
                );
            }
        }

        mut_cluster_metadata.epoch_leader_map.insert(
            self.new_epoch,
            self.leader_change_message
                .next_leader_tx_orderer_address
                .clone(),
        );

        mut_cluster_metadata.update()?;

        let mut mut_rollup_metadata = RollupMetadata::get_mut(&rollup_id)?;

        mut_rollup_metadata.batch_number = self.batch_number;
        mut_rollup_metadata.transaction_order = self.transaction_order;
        mut_rollup_metadata.provided_batch_number = self.provided_batch_number;
        mut_rollup_metadata.provided_transaction_order = self.provided_transaction_order;

        mut_rollup_metadata.update()?;

        let mut mut_epoch_metadata = EpochMetadata::get_mut(&rollup_id)?;
        mut_epoch_metadata.epoch_transaction_orders = self.epoch_metadata.epoch_transaction_orders.clone();
        mut_epoch_metadata.last_batched_epoch = self.epoch_metadata.last_batched_epoch;
        mut_epoch_metadata.update().map_err(|e| {
            tracing::error!("Failed to update epoch metadata: {:?}", e);
            Error::GeneralError("Failed to update epoch metadata".into())
        })?;

        let cluster_metadata = ClusterMetadata::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )?;

        let epoch_leader_address = cluster_metadata.epoch_leader_map.get(&self.old_epoch).ok_or_else(|| {
            tracing::error!(
                "epoch_leader_address not found for old_epoch: {:?} - rollup_id: {:?}, cluster_id: {:?}",
                self.old_epoch,
                rollup_id,
                rollup.cluster_id
            );
            Error::GeneralError("epoch_leader_address not found".into())
        })?;

        let epoch_leader_cluster_rpc_url = cluster
            .get_tx_orderer_rpc_info(epoch_leader_address)
            .and_then(|info| info.cluster_rpc_url)
            .ok_or_else(|| {
                tracing::error!(
                    "cluster_rpc_url not found for epoch leader {:?} (old_epoch: {})",
                    epoch_leader_address,
                    self.old_epoch
                );
                Error::GeneralError("epoch leader cluster_rpc_url not found".into())
            })?;

        send_end_signal_to_epoch_leader(
            context.clone(),
            rollup_id,
            self.old_epoch,
            epoch_leader_cluster_rpc_url,
        );

        /*
        let end_sync_leader_tx_orderer_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos();

        tracing::info!(
            "sync_leader_tx_orderer - total take time: {:?} / self: {:?}",
            end_sync_leader_tx_orderer_time - start_sync_leader_tx_orderer_time,
            self
        );
        */

        Ok(())
    }
}

pub fn send_end_signal_to_epoch_leader(
    context: AppState,
    rollup_id: RollupId,
    epoch: u64,
    epoch_leader_rpc_url: String,
) {
    tokio::spawn(async move {
        let rollup = match Rollup::get(&rollup_id) {
            Ok(rollup) => rollup,
            Err(e) => {
                tracing::error!("Failed to retrieve rollup: {:?}", e);
                return;
            }
        };

        let signer = match context.get_signer(rollup.platform).await {
            Ok(signer) => signer,
            Err(e) => {
                tracing::error!("Failed to get signer: {:?}", e);
                return;
            }
        };

        let sender_address = signer.address().clone();
        let sender_address_clone = sender_address.clone();

        let parameter = SendEndSignal {
            rollup_id,
            epoch,
            sender_address: sender_address_clone,
        };

        context
            .rpc_client()
            .fire_and_forget_multicast(
                vec![epoch_leader_rpc_url],
                SendEndSignal::method(),
                &parameter,
                Id::Null,
            )
            .await;
    });
}