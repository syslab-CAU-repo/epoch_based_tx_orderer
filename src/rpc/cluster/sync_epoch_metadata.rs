use crate::rpc::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SyncEpochMetadata {
    pub last_batched_epoch: u64,
    pub rollup_id: RollupId,
}

impl RpcParameter<AppState> for SyncEpochMetadata {
    type Response = ();

    fn method() -> &'static str {
        "sync_epoch_metadata"
    }

    async fn handler(self, _context: AppState) -> Result<Self::Response, RpcError> {
        tracing::info!("[sync_epoch_metadata]: sync_epoch_metadata called"); // test code

        let mut mut_epoch_metadata = EpochMetadata::get_mut(&self.rollup_id)?;

        tracing::info!("[sync_epoch_metadata]: mut_epoch_metadata.last_batched_epoch(before): {:?}", mut_epoch_metadata.last_batched_epoch); // test code
        
        mut_epoch_metadata.last_batched_epoch = Some(self.last_batched_epoch);

        tracing::info!("[sync_epoch_metadata]: mut_epoch_metadata.last_batched_epoch(after): {:?}", mut_epoch_metadata.last_batched_epoch); // test code

        mut_epoch_metadata.update()?;

        Ok(())
    }
}