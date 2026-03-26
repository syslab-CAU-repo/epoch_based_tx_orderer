use std::collections::BTreeMap;

use radius_sdk::kvstore::Model;
use serde::{Deserialize, Serialize};

use crate::types::RollupId;

#[derive(Clone, Debug, Deserialize, Serialize, Model)]
#[kvstore(key(rollup_id: &RollupId))]
pub struct EpochMetadata {
    pub epoch_transaction_orders: BTreeMap<u64, u64>,
    pub last_batched_epoch: Option<u64>,
}

impl Default for EpochMetadata {
    fn default() -> Self {
        Self {
            epoch_transaction_orders: BTreeMap::new(),
            last_batched_epoch: None,
        }
    }
}

impl EpochMetadata {
    pub fn current_epoch(&self) -> u64 {
        self.epoch_transaction_orders
            .keys()
            .last()
            .copied()
            .unwrap_or(0)
    }

    pub fn transaction_order(&self, epoch: u64) -> u64 {
        self.epoch_transaction_orders
            .get(&epoch)
            .copied()
            .unwrap_or(0)
    }

    pub fn increment_transaction_order(&mut self, epoch: u64) -> u64 {
        let order = self.epoch_transaction_orders.entry(epoch).or_insert(0);
        let current = *order;
        *order += 1;
        current
    }
}
