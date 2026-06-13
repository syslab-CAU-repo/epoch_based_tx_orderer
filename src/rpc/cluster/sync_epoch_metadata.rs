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

    async fn handler(self, context: AppState) -> Result<Self::Response, RpcError> {
        // tracing::info!("SyncEpochMetadata handler() called"); // test code

        let mut mut_epoch_metadata = EpochMetadata::get_mut(&self.rollup_id)?;

        // tracing::info!("mut_epoch_metadata.last_batched_epoch(before): {:?}", mut_epoch_metadata.last_batched_epoch); // test code
        
        mut_epoch_metadata.last_batched_epoch = Some(self.last_batched_epoch);

        // tracing::info!("mut_epoch_metadata.last_batched_epoch(after): {:?}", mut_epoch_metadata.last_batched_epoch); // test code

        mut_epoch_metadata.update()?;

        // non-leader 노드에서도 배치 처리가 끝난 epoch 들의 in-memory 카운터를
        // 회수한다(메모리 누수 방지).
        context
            .atomic_epoch_metadata()
            .get_or_init(&self.rollup_id)
            .retain_epochs_after(self.last_batched_epoch);

        Ok(())
    }
}