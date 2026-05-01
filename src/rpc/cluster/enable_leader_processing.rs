use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EnableLeaderProcessing {
    pub rollup_id: RollupId,
}

impl RpcParameter<AppState> for EnableLeaderProcessing {
    type Response = ();

    fn method() -> &'static str {
        "enable_leader_processing"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        let rollup = Rollup::get(&self.rollup_id).map_err(|e| {
            tracing::error!("Failed to retrieve rollup: {:?}", e);
            Error::RollupNotFound
        })?;

        let mut mut_cluster_metadata = ClusterMetadata::get_mut(
            rollup.platform,
            rollup.liveness_service_provider,
            &rollup.cluster_id,
        )
        .map_err(|e| {
            tracing::error!(
                "Failed to get mutable cluster metadata for rollup_id: {:?}, cluster_id: {:?}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                e
            );
            e
        })?;

        mut_cluster_metadata.can_process_as_leader = true;

        mut_cluster_metadata.update().map_err(|e| {
            tracing::error!(
                "Failed to update cluster metadata (enable_leader_processing). rollup_id: {:?}, cluster_id: {:?}, error: {:?}",
                self.rollup_id,
                rollup.cluster_id,
                e
            );
            e
        })?;

        tracing::info!(
            "enable_leader_processing: set can_process_as_leader=true for rollup_id: {:?}",
            self.rollup_id
        );

        Ok(())
    }
}
