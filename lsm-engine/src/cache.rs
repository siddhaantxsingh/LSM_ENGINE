//! Sharded LRU cache for decoded SSTable blocks, bounded by bytes.
//!
//! Each shard is a HashMap for lookup plus a BTreeMap ordered by "last used
//! tick" for O(log n) eviction of the least-recently-used entry. Sharding by
//! key hash reduces lock contention between concurrent readers.

use std::collections::{BTreeMap, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const SHARDS: usize = 16;

pub type CacheKey = (u64, u64); // (file number, block offset)

struct Shard {
    map: HashMap<CacheKey, (Arc<Vec<u8>>, u64)>,
    lru: BTreeMap<u64, CacheKey>,
    bytes: usize,
    tick: u64,
}

pub struct BlockCache {
    shards: Vec<Mutex<Shard>>,
    capacity_per_shard: usize,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        BlockCache {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        map: HashMap::new(),
                        lru: BTreeMap::new(),
                        bytes: 0,
                        tick: 0,
                    })
                })
                .collect(),
            capacity_per_shard: (capacity_bytes / SHARDS).max(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: &CacheKey) -> &Mutex<Shard> {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        &self.shards[(h.finish() as usize) % SHARDS]
    }

    pub fn get(&self, key: &CacheKey) -> Option<Arc<Vec<u8>>> {
        let mut s = self.shard(key).lock().unwrap();
        s.tick += 1;
        let tick = s.tick;
        let found = match s.map.get_mut(key) {
            Some((v, t)) => {
                let old = *t;
                *t = tick;
                Some((v.clone(), old))
            }
            None => None,
        };
        match found {
            Some((v, old)) => {
                s.lru.remove(&old);
                s.lru.insert(tick, *key);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, key: CacheKey, value: Arc<Vec<u8>>) {
        let mut s = self.shard(&key).lock().unwrap();
        s.tick += 1;
        let tick = s.tick;
        let size = value.len();
        if let Some((old, t)) = s.map.insert(key, (value, tick)) {
            s.bytes -= old.len();
            s.lru.remove(&t);
        }
        s.lru.insert(tick, key);
        s.bytes += size;
        while s.bytes > self.capacity_per_shard && s.lru.len() > 1 {
            let (&t, &k) = s.lru.iter().next().unwrap();
            s.lru.remove(&t);
            if let Some((v, _)) = s.map.remove(&k) {
                s.bytes -= v.len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_lru() {
        let c = BlockCache::new(16 * 100); // 100 bytes per shard
        // Same shard isn't guaranteed, so test the invariant: total bytes bounded.
        for i in 0..1000u64 {
            c.insert((i, 0), Arc::new(vec![0; 40]));
        }
        let total: usize = c.shards.iter().map(|s| s.lock().unwrap().bytes).sum();
        assert!(total <= 16 * 100 + 16 * 40);
        assert!(c.get(&(999, 0)).is_some(), "most recent entry kept");
    }
}
