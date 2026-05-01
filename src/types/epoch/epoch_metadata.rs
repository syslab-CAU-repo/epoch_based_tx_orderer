use std::collections::{BTreeMap, BTreeSet};

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
    pub fn add_completed_epoch(rollup_id: &RollupId, epoch: u64) -> Result<(), Error> {
        let mut can_provide_epoch_info = Self::get_mut_or(rollup_id, Self::default)?;
        can_provide_epoch_info.completed_epoch.insert(epoch);
        can_provide_epoch_info.update()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Model)]
#[kvstore(key(rollup_id: &RollupId))]
pub struct EpochMetadata {
    pub epoch_transaction_orders: BTreeMap<u64, u64>,
    pub last_batched_epoch: Option<u64>,

    // epoch별 각 노드가 전송한 트랜잭션 수
    // HashMap<epoch, Vec<sent_transaction_count>> 형태
    pub received_transaction_count_per_node: BTreeMap<u64, Vec<u64>>,
}

impl Default for EpochMetadata {
    fn default() -> Self {
        Self {
            epoch_transaction_orders: BTreeMap::new(),
            last_batched_epoch: None,
            received_transaction_count_per_node: BTreeMap::new(),
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

    pub fn increment_received_transaction_count(&mut self, epoch: u64, node_index: usize) {
        let counts = self
            .received_transaction_count_per_node
            .entry(epoch)
            .or_insert_with(Vec::new);

        if counts.len() <= node_index {
            counts.resize(node_index + 1, 0);
        }

        counts[node_index] = counts[node_index].saturating_add(1);
    }

    pub fn update_received_transaction_count(
        &mut self,
        epoch: u64,
        node_index: usize,
        received_transaction_count: u64,
    ) {
        let counts = self
            .received_transaction_count_per_node
            .entry(epoch)
            .or_insert_with(Vec::new);

        if counts.len() <= node_index {
            counts.resize(node_index + 1, 0);
        }

        counts[node_index] = received_transaction_count;
    }
}
