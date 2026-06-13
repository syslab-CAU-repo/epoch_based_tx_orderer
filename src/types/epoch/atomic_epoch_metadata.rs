use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use dashmap::DashMap;

use crate::types::RollupId;

/// In-memory, 비영속 epoch 카운터.
///
/// `send_raw_transaction` 핫 패스에서 RocksDB row lock + (de)serialize + commit 비용을
/// 제거하기 위해, 기존 `EpochMetadata` 의 `epoch_transaction_orders` 와
/// `received_transaction_count_per_node` 를 atomic 카운터로 분리해 보관한다.
///
/// 프로세스 재시작 시 진행 중 epoch 카운터는 유실되어도 무방하다는 정책에 따라
/// RocksDB 에 저장하지 않는다.
#[derive(Debug, Default)]
pub struct AtomicEpochMetadata {
    // epoch -> 발급된 transaction_order 수 (다음에 발급할 값)
    epoch_transaction_orders: DashMap<u64, AtomicU64>,

    // epoch -> 노드별 수신 트랜잭션 수 (Vec 인덱스 = node_index)
    received_transaction_count_per_node: DashMap<u64, Vec<AtomicU64>>,
}

impl AtomicEpochMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// 트랜잭션 순서를 발급한다. 발급 직전 값(= 이 트랜잭션의 transaction_order)을
    /// 반환하고 내부 카운터를 1 증가시킨다. read + increment 가 단일 atomic 연산이라
    /// 멀티스레드에서 두 호출이 같은 order 를 받는 race 가 없다.
    pub fn issue_transaction_order(&self, epoch: u64) -> u64 {
        // 빠른 경로: 이미 존재하면 공유 read guard 로 lock-free fetch_add.
        if let Some(counter) = self.epoch_transaction_orders.get(&epoch) {
            return counter.fetch_add(1, Ordering::Relaxed);
        }

        // 느린 경로: 최초 1회만 write guard 로 생성.
        self.epoch_transaction_orders
            .entry(epoch)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed)
    }

    /// 현재까지 발급된 transaction_order 수를 조회한다.
    pub fn transaction_order(&self, epoch: u64) -> u64 {
        self.epoch_transaction_orders
            .get(&epoch)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// non-leader 노드가 leader 로부터 전달받은 최종 transaction_order 를 설정한다.
    /// 이미 값이 있으면 기존 값을 `Some` 으로 반환하여 호출부가 경고를 남길 수 있게
    /// 한다(원래 `sync_can_provide_epoch_info` 의 중복 감지 동작 보존).
    pub fn set_transaction_order_if_absent(
        &self,
        epoch: u64,
        transaction_order: u64,
    ) -> Option<u64> {
        use dashmap::mapref::entry::Entry;

        match self.epoch_transaction_orders.entry(epoch) {
            Entry::Occupied(entry) => Some(entry.get().load(Ordering::Relaxed)),
            Entry::Vacant(entry) => {
                entry.insert(AtomicU64::new(transaction_order));
                None
            }
        }
    }

    /// `node_index` 의 수신 카운터를 1 증가시킨다. 필요 시 Vec 을 0 으로 확장한다
    /// (원래 `increment_received_transaction_count` 의 동적 확장 동작 보존).
    pub fn increment_received_transaction_count(&self, epoch: u64, node_index: usize) {
        // 빠른 경로: 이미 존재하고 인덱스가 범위 안이면 공유 read guard 로 fetch_add.
        {
            if let Some(counts) = self.received_transaction_count_per_node.get(&epoch) {
                if let Some(counter) = counts.get(node_index) {
                    counter.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        } // read guard drop (아래 write guard 와의 self-deadlock 방지)

        // 느린 경로: 생성 또는 확장 (write guard).
        let mut counts = self
            .received_transaction_count_per_node
            .entry(epoch)
            .or_insert_with(Vec::new);

        while counts.len() <= node_index {
            counts.push(AtomicU64::new(0));
        }

        counts[node_index].fetch_add(1, Ordering::Relaxed);
    }

    /// `node_index` 의 수신 카운터를 조회한다.
    pub fn received_transaction_count(&self, epoch: u64, node_index: usize) -> u64 {
        self.received_transaction_count_per_node
            .get(&epoch)
            .and_then(|counts| counts.get(node_index).map(|c| c.load(Ordering::Relaxed)))
            .unwrap_or(0)
    }

    /// `last_removed_epoch` 이하의 모든 epoch 카운터를 제거한다(메모리 누수 방지).
    /// 이미 배치 처리가 끝난 epoch 는 더 이상 참조되지 않으므로 안전하게 회수한다.
    pub fn retain_epochs_after(&self, last_removed_epoch: u64) {
        self.epoch_transaction_orders
            .retain(|epoch, _| *epoch > last_removed_epoch);
        self.received_transaction_count_per_node
            .retain(|epoch, _| *epoch > last_removed_epoch);
    }
}

/// `rollup_id -> AtomicEpochMetadata` 를 관리하는 매니저. `AppState` 에 보관되어
/// 모든 핸들러가 공유한다.
#[derive(Clone, Default)]
pub struct AtomicEpochMetadataManager {
    inner: Arc<DashMap<RollupId, Arc<AtomicEpochMetadata>>>,
}

impl AtomicEpochMetadataManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// `rollup_id` 의 `AtomicEpochMetadata` 핸들을 가져온다. 없으면 생성한다.
    /// 외부 맵의 shard lock 을 즉시 해제하기 위해 `Arc` 를 복제해 반환한다.
    pub fn get_or_init(&self, rollup_id: &RollupId) -> Arc<AtomicEpochMetadata> {
        self.inner
            .entry(rollup_id.clone())
            .or_insert_with(|| Arc::new(AtomicEpochMetadata::new()))
            .clone()
    }
}
