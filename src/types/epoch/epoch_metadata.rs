use radius_sdk::kvstore::Model;
use serde::{Deserialize, Serialize};

use crate::types::RollupId;

#[derive(Clone, Debug, Deserialize, Serialize, Model)]
#[kvstore(key(rollup_id: &RollupId))]
pub struct EpochMetadata {
    pub epoch: u64,
    pub transaction_order: u64,
    pub last_batched_epoch: Option<u64>,
}

impl Default for EpochMetadata {
    fn default() -> Self {
        Self {
            epoch: 0,
            transaction_order: 0,
        }
    }
}
