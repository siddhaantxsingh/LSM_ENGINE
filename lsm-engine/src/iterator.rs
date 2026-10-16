//! Iterators over internal keys, plus the k-way merge and the user-facing
//! iterator that applies snapshot visibility and hides tombstones.

use crate::key::*;
use std::cmp::Ordering;

pub trait InternalIterator: Send {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    /// Position at the first entry with internal key >= target.
    fn seek(&mut self, target: &[u8]);
    fn next(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    /// Any I/O or corruption error hit while iterating. An iterator that
    /// stops early because of an error looks "exhausted", so callers that
    /// must not lose data (compaction) check this at the end.
    fn status(&self) -> crate::Result<()> {
        Ok(())
    }
}

/// Merges N sorted iterators. With ~10 children (memtables + L0 files +
/// one per level) a linear scan for the minimum beats a heap in practice
/// and keeps the code obvious; complexity is O(N) per step.
pub struct MergingIterator {
    children: Vec<Box<dyn InternalIterator>>,
    current: Option<usize>,
}

impl MergingIterator {
    pub fn new(children: Vec<Box<dyn InternalIterator>>) -> Self {
        MergingIterator {
            children,
            current: None,
        }
    }

    fn find_smallest(&mut self) {
        let mut best: Option<usize> = None;
        for (i, c) in self.children.iter().enumerate() {
            if !c.valid() {
                if c.status().is_err() {
                    // A child stopped on an error: stop the whole merge
                    // instead of continuing with a gap in the key space.
                    self.current = None;
                    return;
                }
                continue;
            }
            best = match best {
                None => Some(i),
                // Ties (same internal key in two sources) can't happen for
                // distinct sequence numbers; earlier children win anyway.
                Some(b) if cmp_internal(c.key(), self.children[b].key()) == Ordering::Less => {
                    Some(i)
                }
                b => b,
            };
        }
        self.current = best;
    }
}

impl InternalIterator for MergingIterator {
    fn valid(&self) -> bool {
        self.current.is_some()
    }
    fn seek_to_first(&mut self) {
        for c in &mut self.children {
            c.seek_to_first();
        }
        self.find_smallest();
    }
    fn seek(&mut self, target: &[u8]) {
        for c in &mut self.children {
            c.seek(target);
        }
        self.find_smallest();
    }
    fn next(&mut self) {
        if let Some(i) = self.current {
            self.children[i].next();
            self.find_smallest();
        }
    }
    fn key(&self) -> &[u8] {
        self.children[self.current.unwrap()].key()
    }
    fn value(&self) -> &[u8] {
        self.children[self.current.unwrap()].value()
    }
    fn status(&self) -> crate::Result<()> {
        for c in &self.children {
            c.status()?;
        }
        Ok(())
    }
}

/// User-facing iterator: yields each user key at most once, using the
/// newest version with seq <= snapshot, and skips keys whose newest visible
/// version is a tombstone. Optionally bounded above by `end` (exclusive).
pub struct DbIterator {
    inner: Box<dyn InternalIterator>,
    snapshot: SeqNum,
    end: Option<Vec<u8>>,
    key: Vec<u8>,
    value: Vec<u8>,
    valid: bool,
    error_reported: bool,
    // Keeps the version (and its files) alive while iterating.
    _pin: Box<dyn Send + Sync>,
}

impl DbIterator {
    pub(crate) fn new(
        inner: Box<dyn InternalIterator>,
        snapshot: SeqNum,
        pin: Box<dyn Send + Sync>,
    ) -> Self {
        DbIterator {
            inner,
            snapshot,
            end: None,
            key: vec![],
            value: vec![],
            valid: false,
            error_reported: false,
            _pin: pin,
        }
    }

    pub(crate) fn set_end(&mut self, end: Option<Vec<u8>>) {
        self.end = end;
    }

    pub fn seek_to_first(&mut self) {
        self.error_reported = false;
        self.inner.seek_to_first();
        self.find_next_user_entry(None);
    }

    pub fn seek(&mut self, user_key: &[u8]) {
        self.error_reported = false;
        self.inner
            .seek(&make_internal_key(user_key, self.snapshot, SEEK_KIND));
        self.find_next_user_entry(None);
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn status(&self) -> crate::Result<()> {
        self.inner.status()
    }

    pub fn next(&mut self) {
        let skip = std::mem::take(&mut self.key);
        self.find_next_user_entry(Some(&skip));
    }

    /// Advance to the next user key (> skip) whose newest visible entry is a Put.
    fn find_next_user_entry(&mut self, skip: Option<&[u8]>) {
        let mut skip: Option<Vec<u8>> = skip.map(|s| s.to_vec());
        while self.inner.valid() {
            let ik = self.inner.key();
            let uk = user_key(ik);
            if let Some(end) = &self.end
                && uk >= end.as_slice()
            {
                break;
            }
            if seq(ik) <= self.snapshot && skip.as_deref() != Some(uk) {
                match kind(ik) {
                    Kind::Delete => {
                        // Hide this key and all its older versions.
                        skip = Some(uk.to_vec());
                    }
                    Kind::Put => {
                        self.key = uk.to_vec();
                        self.value = self.inner.value().to_vec();
                        self.valid = true;
                        return;
                    }
                }
            }
            self.inner.next();
        }
        self.valid = false;
    }
}

/// Yields `Ok((key, value))` in key order. If a read error stops the scan
/// early, the last item is `Err` (once), so an incomplete scan can never
/// be mistaken for a complete one (audit defect D6).
impl Iterator for DbIterator {
    type Item = crate::Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid {
            if self.error_reported {
                return None;
            }
            self.error_reported = true;
            return self.status().err().map(Err);
        }
        let item = (self.key.clone(), self.value.clone());
        DbIterator::next(self);
        Some(Ok(item))
    }
}
