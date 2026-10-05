//! In-memory write buffer, ordered by internal key.
//!
//! A BTreeMap behind an RwLock: writers are already serialised by the DB
//! write lock, and readers take a shared lock. (LevelDB uses a lock-free
//! skiplist; a B-tree has better cache locality and the write path here is
//! bottlenecked on the WAL anyway.)

use crate::iterator::InternalIterator;
use crate::key::*;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

pub enum LookupResult {
    Found(Vec<u8>),
    Deleted,
    NotFound,
}

#[derive(Default)]
pub struct MemTable {
    map: RwLock<BTreeMap<InternalKey, Vec<u8>>>,
    size: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&self, seq: SeqNum, kind: Kind, key: &[u8], value: &[u8]) {
        let ik = make_internal_key(key, seq, kind);
        self.size
            .fetch_add(ik.len() + value.len() + 32, Ordering::Relaxed);
        self.map
            .write()
            .unwrap()
            .insert(InternalKey(ik), value.to_vec());
    }

    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.read().unwrap().is_empty()
    }

    /// Newest version of `key` with seq <= `snapshot`.
    pub fn get(&self, key: &[u8], snapshot: SeqNum) -> LookupResult {
        let target = InternalKey(make_internal_key(key, snapshot, SEEK_KIND));
        let map = self.map.read().unwrap();
        if let Some((k, v)) = map
            .range((Bound::Included(target), Bound::Unbounded))
            .next()
            && user_key(&k.0) == key
        {
            return match kind(&k.0) {
                Kind::Put => LookupResult::Found(v.clone()),
                Kind::Delete => LookupResult::Deleted,
            };
        }
        LookupResult::NotFound
    }

    /// Point-in-time iterator. Copies entries (memtables are bounded in
    /// size, and this keeps the iterator independent of later writes).
    pub fn iter(&self) -> VecIterator {
        let map = self.map.read().unwrap();
        VecIterator::new(map.iter().map(|(k, v)| (k.0.clone(), v.clone())).collect())
    }
}

/// Iterator over a sorted vector of (internal key, value).
pub struct VecIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecIterator {
    pub fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let pos = entries.len();
        VecIterator { entries, pos }
    }
}

impl InternalIterator for VecIterator {
    fn valid(&self) -> bool {
        self.pos < self.entries.len()
    }
    fn seek_to_first(&mut self) {
        self.pos = 0;
    }
    fn seek(&mut self, target: &[u8]) {
        self.pos = self
            .entries
            .partition_point(|(k, _)| cmp_internal(k, target).is_lt());
    }
    fn next(&mut self) {
        self.pos += 1;
    }
    fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }
    fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }
}
