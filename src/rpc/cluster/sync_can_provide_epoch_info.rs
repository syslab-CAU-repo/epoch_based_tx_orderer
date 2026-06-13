use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncCanProvideEpochInfo {
    pub epoch: u64,
    pub transaction_order: u64,
    pub rollup_id: RollupId,
}

impl RpcParameter<AppState> for SyncCanProvideEpochInfo {
    type Response = ();

    fn method() -> &'static str {
        "sync_can_provide_epoch_info"
    }

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        let atomic_epoch_metadata = context.atomic_epoch_metadata().get_or_init(&self.rollup_id);

        if let Some(existing_transaction_order) = atomic_epoch_metadata
            .set_transaction_order_if_absent(self.epoch, self.transaction_order)
        {
            tracing::warn!(
                "epoch_transaction_orders already contains epoch. rollup_id={:?} epoch={} existing_transaction_order={} transaction_order_from_epoch_leader={}",
                self.rollup_id,
                self.epoch,
                existing_transaction_order,
                self.transaction_order
            );
        }

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