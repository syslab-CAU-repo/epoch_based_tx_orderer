use std::collections::btree_map::Entry;

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
        let mut mut_epoch_metadata = EpochMetadata::get_mut(&self.rollup_id)?;
        match mut_epoch_metadata
            .epoch_transaction_orders
            .entry(self.epoch)
        {
            Entry::Occupied(entry) => {
                tracing::warn!(
                    "epoch_transaction_orders already contains epoch. rollup_id={:?} epoch={} existing_transaction_order={} transaction_order_from_epoch_leader={}",
                    self.rollup_id,
                    self.epoch,
                    entry.get(),
                    self.transaction_order
                );
            }
            Entry::Vacant(entry) => {
                entry.insert(self.transaction_order);
            }
        }
        mut_epoch_metadata.update()?;

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
