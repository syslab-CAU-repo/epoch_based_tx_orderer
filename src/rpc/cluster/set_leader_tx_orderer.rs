use super::LeaderChangeMessage;
use crate::rpc::{cluster::sync_leader_tx_orderer, prelude::*};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SetLeaderTxOrderer {
    pub leader_change_message: LeaderChangeMessage,
    pub rollup_signature: Signature,
}

impl RpcParameter<AppState> for SetLeaderTxOrderer {
    type Response = ();

    fn method() -> &'static str {
        "set_leader_tx_orderer"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        let rollup_id = self.leader_change_message.rollup_id.clone();

        let rollup_metadata = match RollupMetadata::get(&rollup_id) {
            Ok(metadata) => metadata,
            Err(err) => {
                tracing::error!(
                    "Failed to get rollup metadata - rollup_id: {:?} / error: {:?}",
                    rollup_id,
                    err,
                );

                return Ok(());
            }
        };

        let rollup = Rollup::get(&rollup_id)?;

        let cluster = Cluster::get(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
            self.leader_change_message.platform_block_height,
        )?;

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

        mut_cluster_metadata.platform_block_height =
            self.leader_change_message.platform_block_height;
        mut_cluster_metadata.is_leader = is_next_leader;
        mut_cluster_metadata.leader_tx_orderer_rpc_info = Some(leader_tx_orderer_rpc_info.clone());

        let signer = context.get_signer(rollup.platform).await?;
        let current_tx_orderer_address = signer.address();

        let old_epoch = mut_cluster_metadata.epoch;

        // old_epoch의 리더 RPC URL을 epoch_leader_map에 저장 (이미 존재하지 않을 때만)
        if !mut_cluster_metadata.epoch_leader_map.contains_key(&old_epoch) {
            tracing::info!("old_epoch의 리더 RPC URL을 epoch_leader_map에 저장 (이미 존재하지 않을 때만)"); // test code
            mut_cluster_metadata.epoch_leader_map.insert(old_epoch, self.leader_change_message.current_leader_tx_orderer_address.clone());
        }
        mut_cluster_metadata.epoch = old_epoch + 1;

        let new_epoch = mut_cluster_metadata.epoch;

        if let Some(provisional_leader) = mut_cluster_metadata.epoch_leader_map.get(&new_epoch) {
            if *provisional_leader != self.leader_change_message.next_leader_tx_orderer_address {
                tracing::error!(
                    "Epoch leader mismatch for epoch {}: provisionally registered {:?}, but actual leader is {:?}; rollup_id={:?}",
                    new_epoch,
                    provisional_leader,
                    self.leader_change_message.next_leader_tx_orderer_address,
                    rollup_id,
                );
            }
        }

        mut_cluster_metadata.epoch_leader_map.insert(new_epoch, self.leader_change_message.next_leader_tx_orderer_address.clone());

        let epoch_metadata = EpochMetadata::get(&rollup_id).unwrap_or_default();

        sync_leader_tx_orderer(
            context.clone(),
            cluster,
            self.leader_change_message.clone(),
            self.rollup_signature,
            rollup_metadata.batch_number,
            rollup_metadata.transaction_order,
            rollup_metadata.provided_batch_number,
            rollup_metadata.provided_transaction_order,
            old_epoch,
            new_epoch,
            epoch_metadata,
        )
        .await;

        mut_cluster_metadata.update()?;

        Ok(())
    }
}
