use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use std::sync::LazyLock;

pub(crate) static REDIRECT_RING: LazyLock<RedirectRing> =
    LazyLock::new(RedirectRing::new);

struct Slot {
    epoch: AtomicU64,
    count: AtomicU64,
}

const WINDOW: usize = 10000;
const EMPTY: u64 = u64::MAX;
const EPOCH_REPLACING: u64 = u64::MAX - 1;

pub struct RedirectRing {
    slots: [Slot; WINDOW],
    overflow: DashMap<u64, AtomicU64>,
}

impl RedirectRing {
    pub fn new() -> Self {
        RedirectRing {
            slots: std::array::from_fn(|_| Slot{
                epoch: AtomicU64::new(u64::MAX),
                count: AtomicU64::new(0),
            }),
            overflow: DashMap::new(),
        }
    }

    pub fn incr(&self, epoch: u64) {
        let idx = (epoch as usize) % WINDOW;
        let slot = &self.slots[idx];

        loop {
            let cur = slot.epoch.load(Ordering::Acquire);
            if cur == epoch {
                slot.count.fetch_add(1, Ordering::Relaxed);
                return;
            }

            // the slot is being replaced
            if cur == EPOCH_REPLACING {
                self.overflow
                    .entry(epoch)
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }

            // try to become the replacer
            if slot
                .epoch
                .compare_exchange(cur, EPOCH_REPLACING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // we own the replacement
                slot.count.store(0, Ordering::Relaxed); // optional: depends on semantics
                slot.epoch.store(epoch, Ordering::Release);
                slot.count.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // lost the race; retry
        }
    }

    pub fn get(&self, epoch: u64) -> u64 {
        let idx = (epoch as usize) % WINDOW;
        let slot = &self.slots[idx];

        let slot_epoch = slot.epoch.load(Ordering::Relaxed);
        if slot_epoch == epoch {
            return slot.count.load(Ordering::Relaxed);
        }

        self.overflow
            .get(&epoch)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}