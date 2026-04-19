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
        let mut mut_epoch_metadata = EpochMetadata::get_mut(&self.rollup_id)?;
        
        mut_epoch_metadata.last_batched_epoch = Some(self.last_batched_epoch);

        mut_epoch_metadata.update()?;

        Ok(())
    }
}