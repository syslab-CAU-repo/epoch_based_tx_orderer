use radius_sdk::{
    kvstore::Model,
    signature::Address,
};
use serde::{Deserialize, Serialize};

use crate::{
    client::seeder::TxOrdererRpcInfo,
    types::{LivenessServiceProvider, Platform},
};

use super::ClusterId;

use std::collections::HashMap;

#[derive(Clone, Debug, Default, Deserialize, Serialize, Model)]
#[kvstore(key(platform: Platform, liveness_service_provider: LivenessServiceProvider, cluster_id: &str))]
pub struct ClusterMetadata {
    pub cluster_id: ClusterId,
    pub platform_block_height: u64,

    pub is_leader: bool,
    pub can_process_as_leader: bool,
    pub leader_tx_orderer_rpc_info: Option<TxOrdererRpcInfo>,

    pub epoch: u64,

    // epoch별 노드 end_signal 비트맵
    // HashMap<epoch, bitmap> 형태
    // bitmap의 각 비트는 해당 인덱스의 노드가 end_signal을 보냈는지를 나타냄
    pub end_signal_bitmap: HashMap<u64, u64>,

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
            epoch: 0, // start with epoch 0
            end_signal_bitmap: HashMap::new(),
            epoch_leader_map: HashMap::new(),
            epoch_sent_transaction_count: HashMap::new(),
        }
    }

    // 특정 epoch의 특정 노드 인덱스에 비트 설정
    pub fn set_node_bit(&mut self, epoch: u64, node_index: usize) {
        let bitmap = self.end_signal_bitmap.entry(epoch).or_insert(0);
        *bitmap |= 1 << node_index;
    }

    // 특정 epoch의 특정 노드 인덱스 비트 확인
    pub fn get_node_bit(&self, epoch: u64, node_index: usize) -> bool {
        self.end_signal_bitmap
            .get(&epoch)
            .map(|bitmap| (*bitmap >> node_index) & 1 == 1)
            .unwrap_or(false)
    }

    // 특정 epoch의 모든 노드가 end_signal을 보냈는지 확인
    pub fn all_nodes_sent_signal(&self, epoch: u64, total_nodes: usize) -> bool {
        if total_nodes == 0 {
            return false;
        }
        
        // 모든 노드의 비트가 설정되어 있는지 확인
        // 예: 5개 노드면 0b11111 (0x1F)와 비교
        let expected_bitmap = if total_nodes < 64 {
            (1u64 << total_nodes) - 1
        } else {
            u64::MAX
        };

        self.end_signal_bitmap
            .get(&epoch)
            .map(|bitmap| (*bitmap & expected_bitmap) == expected_bitmap)
            .unwrap_or(false)
    }

    // 특정 epoch의 비트맵 가져오기
    pub fn get_epoch_bitmap(&self, epoch: u64) -> u64 {
        self.end_signal_bitmap.get(&epoch).copied().unwrap_or(0)
    }

    pub fn increment_sent_transaction_count(&mut self, epoch: u64) {
        let count = self.epoch_sent_transaction_count.entry(epoch).or_insert(0);
        *count += 1;
    }
}
