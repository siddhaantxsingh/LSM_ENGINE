//! Leveled compaction (LevelDB-style).
//!
//! * L0 holds freshly flushed memtables; files overlap.
//! * L1..L6 are each one sorted run of non-overlapping files; Ln may hold
//!   `max_bytes_level1 * 10^(n-1)` bytes.
//! * When a level is over budget we pick input files in it, add every
//!   overlapping file from the next level, merge them, and write the result
//!   into the next level. Old versions shadowed by newer ones (and not
//!   needed by any snapshot) and tombstones that no longer hide anything are
//!   dropped along the way — that's where space is reclaimed.

use crate::cache::BlockCache;
use crate::error::Result;
use crate::iterator::{InternalIterator, MergingIterator};
use crate::key::*;
use crate::options::Options;
use crate::sstable::TableBuilder;
use crate::version::*;
use std::path::Path;
use std::sync::Arc;

pub struct Compaction {
    pub level: usize,
    pub inputs: [Vec<Arc<FileMeta>>; 2], // [level, level+1]
    pub version: Arc<Version>,
}

impl Compaction {
    pub fn is_trivial_move(&self) -> bool {
        self.level > 0 && self.inputs[0].len() == 1 && self.inputs[1].is_empty()
    }

    pub fn input_bytes(&self) -> u64 {
        self.inputs.iter().flatten().map(|f| f.size).sum()
    }

    /// True if no level deeper than the output level could contain `key`,
    /// so a tombstone for it can be dropped.
    fn is_base_level_for_key(&self, key: &[u8]) -> bool {
        for lvl in self.level + 2..NUM_LEVELS {
            for f in &self.version.levels[lvl] {
                if key >= f.smallest_user() && key <= f.largest_user() {
                    return false;
                }
            }
        }
        true
    }
}

/// Score each level (>= 1.0 means it needs compaction) and pick the worst.
pub fn compaction_score(v: &Version, opts: &Options) -> (usize, f64) {
    let mut best = (0, 0.0);
    for level in 0..NUM_LEVELS - 1 {
        let score = if level == 0 {
            v.levels[0].len() as f64 / opts.l0_compaction_trigger as f64
        } else {
            v.level_bytes(level) as f64 / opts.max_bytes_for_level(level) as f64
        };
        if score > best.1 {
            best = (level, score);
        }
    }
    best
}

fn key_range(files: &[Arc<FileMeta>]) -> (Vec<u8>, Vec<u8>) {
    let lo = files
        .iter()
        .map(|f| f.smallest_user())
        .min()
        .unwrap()
        .to_vec();
    let hi = files
        .iter()
        .map(|f| f.largest_user())
        .max()
        .unwrap()
        .to_vec();
    (lo, hi)
}

pub fn pick_compaction(
    vs: &VersionSet,
    opts: &Options,
    force_level: Option<usize>,
) -> Option<Compaction> {
    let v = vs.current.clone();
    let level = match force_level {
        Some(l) => l,
        None => {
            let (l, score) = compaction_score(&v, opts);
            if score < 1.0 {
                return None;
            }
            l
        }
    };
    if v.levels[level].is_empty() {
        return None;
    }
    let mut first = if level == 0 {
        // L0 files overlap each other, so take them all.
        v.levels[0].clone()
    } else {
        // Round-robin through the key space using the compact pointer so
        // every file eventually gets compacted.
        let ptr = &vs.compact_pointers[level];
        let f = v.levels[level]
            .iter()
            .find(|f| ptr.is_empty() || cmp_internal(&f.largest, ptr).is_gt())
            .unwrap_or(&v.levels[level][0]);
        vec![f.clone()]
    };
    let (lo, hi) = key_range(&first);
    let mut next = v.overlapping(level + 1, &lo, &hi);
    if level > 0 && !next.is_empty() {
        // Expand the level inputs to cover the whole next-level range
        // without pulling in more next-level files (keeps runs disjoint).
        let (nlo, nhi) = key_range(&next);
        let (alo, ahi) = (lo.clone().min(nlo), hi.clone().max(nhi));
        let expanded = v.overlapping(level, &alo, &ahi);
        let (elo, ehi) = key_range(&expanded);
        if v.overlapping(level + 1, &elo, &ehi).len() == next.len() {
            first = expanded;
            next = v.overlapping(level + 1, &elo, &ehi);
        }
    }
    Some(Compaction {
        level,
        inputs: [first, next],
        version: v,
    })
}

pub struct CompactionOutput {
    pub files: Vec<(u64, u64, Vec<u8>, Vec<u8>)>, // number, size, smallest, largest
    pub bytes_written: u64,
    pub dropped: u64,
}

/// Merge the inputs and write output tables. Runs without the DB lock.
/// `alloc` hands out file numbers (needs the lock briefly).
pub fn run_compaction(
    c: &Compaction,
    dir: &Path,
    opts: &Options,
    cache: &Arc<BlockCache>,
    smallest_snapshot: SeqNum,
    mut alloc: impl FnMut() -> u64,
) -> Result<CompactionOutput> {
    let mut children: Vec<Box<dyn InternalIterator>> = Vec::new();
    for f in c.inputs.iter().flatten() {
        children.push(Box::new(f.table(cache)?.iter()));
    }
    let mut it = MergingIterator::new(children);
    it.seek_to_first();

    let mut out = CompactionOutput {
        files: vec![],
        bytes_written: 0,
        dropped: 0,
    };
    let mut builder: Option<(u64, TableBuilder)> = None;
    let mut current_user: Option<Vec<u8>> = None;
    let mut last_seq_for_key = MAX_SEQ;

    let finish = |b: (u64, TableBuilder), out: &mut CompactionOutput| -> Result<()> {
        let (num, tb) = b;
        let (sm, lg) = (tb.smallest.clone(), tb.largest.clone());
        let size = tb.finish()?;
        out.bytes_written += size;
        out.files.push((num, size, sm, lg));
        Ok(())
    };

    while it.valid() {
        let ik = it.key();
        let uk = user_key(ik);
        let new_key = current_user.as_deref() != Some(uk);
        if new_key {
            // Only cut output files at user-key boundaries so one user key
            // never spans two files in the same level.
            if let Some((_, tb)) = &builder
                && tb.estimated_file_size() >= opts.target_file_size
            {
                finish(builder.take().unwrap(), &mut out)?;
            }
            current_user = Some(uk.to_vec());
            last_seq_for_key = MAX_SEQ;
        }

        let s = seq(ik);
        let drop = if last_seq_for_key <= smallest_snapshot {
            // A newer version of this key is visible to every snapshot.
            true
        } else {
            kind(ik) == Kind::Delete && s <= smallest_snapshot && c.is_base_level_for_key(uk)
        };
        last_seq_for_key = s;

        if drop {
            out.dropped += 1;
        } else {
            if builder.is_none() {
                let num = alloc();
                builder = Some((
                    num,
                    TableBuilder::create(&table_path(dir, num), opts.table)?,
                ));
            }
            builder.as_mut().unwrap().1.add(ik, it.value())?;
        }
        it.next();
    }
    // Never install a compaction that silently stopped on a read error.
    it.status()?;
    if let Some(b) = builder.take() {
        finish(b, &mut out)?;
    }
    Ok(out)
}
