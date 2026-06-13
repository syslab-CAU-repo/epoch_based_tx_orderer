use std::collections::BTreeSet;

use radius_sdk::kvstore::Model;
use serde::{Deserialize, Serialize};

use crate::{error::Error, types::RollupId};

#[derive(Clone, Debug, Deserialize, Serialize, Model)]
#[kvstore(key(rollup_id: &RollupId))]
pub struct CanProvideEpochInfo {
    pub completed_epoch: BTreeSet<u64>,  
}

impl Default for CanProvideEpochInfo {
    fn default() -> Self {
        Self {
            completed_epoch: BTreeSet::new(),
        }
    }
}

impl CanProvideEpochInfo {
    pub fn add_completed_epoch(
        rollup_id: &RollupId,
        epoch: u64,
    ) -> Result<(), Error> {
        let mut can_provide_epoch_info = Self::get_mut_or(rollup_id, Self::default)?;
        can_provide_epoch_info.completed_epoch.insert(epoch);
        can_provide_epoch_info.update()?;
        Ok(())
    }
}

/// epoch 관련 영속 메타데이터.
///
/// 과거에는 `epoch_transaction_orders` 와 `received_transaction_count_per_node` 도
/// 이 구조체에 함께 담겨 RocksDB 락 아래에서 갱신됐으나, `send_raw_transaction`
/// 핫 패스의 병목을 제거하기 위해 두 카운터는 in-memory atomic 구조체
/// ([`AtomicEpochMetadata`]) 로 분리됐다. 영속성이 필요한 `last_batched_epoch` 만
/// 여기에 남는다.
#[derive(Clone, Debug, Deserialize, Serialize, Model)]
#[kvstore(key(rollup_id: &RollupId))]
pub struct EpochMetadata {
    pub last_batched_epoch: Option<u64>,
}

impl Default for EpochMetadata {
    fn default() -> Self {
        Self {
            last_batched_epoch: None,
        }
    }
}
