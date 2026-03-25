use crate::rpc::prelude::*;

use radius_sdk::signature::Address;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncCanProvideEpochInfo {
    pub epoch: u64,
    pub rollup_id: RollupId,
}

impl RpcParameter<AppState> for SyncCanProvideEpochInfo {
    type Response = ();

    fn method() -> &'static str {
        "sync_can_provide_epoch_info"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        CanProvideEpochInfo::add_completed_epoch(&self.rollup_id, self.epoch).map_err(|e| {
            tracing::error!(
                "Failed to add completed epoch to CanProvideEpochInfo. rollup_id: {:?}, epoch: {}, error: {:?}",
                self.rollup_id,
                self.epoch,
                e
            );
            e
        })?;

        Ok(())
    }
}