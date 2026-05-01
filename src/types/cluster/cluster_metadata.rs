use dashmap::DashMap;
use radius_sdk::{kvstore::Model, signature::Address};
use serde::{Deserialize, Serialize};

use crate::{
    client::seeder::TxOrdererRpcInfo,
    types::{LivenessServiceProvider, Platform},
};

use super::ClusterId;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub static CLUSTER_EPOCH: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Default, Deserialize, Serialize, Model)]
#[kvstore(key(platform: Platform, liveness_service_provider: LivenessServiceProvider, cluster_id: &str))]
pub struct ClusterMetadata {
    pub cluster_id: ClusterId,
    pub platform_block_height: u64,

    pub is_leader: bool,

    // TODO: remove this field
    pub can_process_as_leader: bool, // not used

    pub leader_tx_orderer_rpc_info: Option<TxOrdererRpcInfo>,

    // epoch별 리더 주소
    // HashMap<epoch, leader node address> 형태
    pub epoch_leader_map: HashMap<u64, Address>,

    // epoch별 전송된 트랜잭션 수
    // HashMap<epoch, sent_transaction_count> 형태
    pub epoch_sent_transaction_count: HashMap<u64, u64>,
}

impl ClusterMetadata {
    pub fn new(cluster_id: ClusterId, platform_block_height: u64) -> Self {
        Self {
            cluster_id,
            platform_block_height,
            is_leader: false,
            can_process_as_leader: false,
            leader_tx_orderer_rpc_info: None,
            epoch_leader_map: HashMap::new(),
            epoch_sent_transaction_count: HashMap::new(),
        }
    }

    pub fn increment_sent_transaction_count(&mut self, epoch: u64) {
        let count = self.epoch_sent_transaction_count.entry(epoch).or_insert(0);
        *count += 1;
    }
}