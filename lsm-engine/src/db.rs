//! The database: ties together the WAL, memtables, versions and the
//! background flush/compaction thread.
//!
//! Write path (group commit):
//!   1. A writer enqueues its batch and waits until it is at the front.
//!   2. The front writer becomes the *leader*: it makes room (maybe freezing
//!      the memtable), merges every queued batch into one, assigns sequence
//!      numbers, and releases the state lock.
//!   3. It appends one WAL record (one fsync for the whole group if `sync`),
//!      inserts into the memtable, then publishes the new visible sequence
//!      and wakes the followers whose batches it wrote.
//!
//! Read path: snapshot (memtable, immutable memtable, current Version,
//! sequence) under the lock, then search without holding it.

use crate::batch::WriteBatch;
use crate::cache::BlockCache;
use crate::compaction::{self, Compaction};
use crate::error::{Error, Result};
use crate::iterator::{DbIterator, InternalIterator, MergingIterator};
use crate::key::*;
use crate::log::{LogWriter, read_log};
use crate::memtable::{LookupResult, MemTable};
use crate::options::Options;
use crate::sstable::{TableBuilder, TableIterator};
use crate::version::*;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

struct Writer {
    batch: WriteBatch,
    sync: bool,
    done: bool,
    result: Option<std::result::Result<(), String>>,
}

struct State {
    mem: Arc<MemTable>,
    imm: Option<Arc<MemTable>>,
    wal_number: u64,
    versions: VersionSet,
    visible_seq: SeqNum,
    snapshots: BTreeMap<SeqNum, usize>,
    writers: VecDeque<Arc<Mutex<Writer>>>,
    bg_error: Option<String>,
    manual_compaction: bool,
    bg_busy: bool,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub user_bytes_written: AtomicU64,
    pub flush_bytes: AtomicU64,
    pub compaction_bytes_read: AtomicU64,
    pub compaction_bytes_written: AtomicU64,
    pub compactions: AtomicU64,
    pub trivial_moves: AtomicU64,
    pub flushes: AtomicU64,
    pub stall_micros: AtomicU64,
    pub write_groups: AtomicU64,
    pub writes: AtomicU64,
}

struct Inner {
    dir: PathBuf,
    opts: Options,
    cache: Arc<BlockCache>,
    state: Mutex<State>,
    wal: Mutex<LogWriter>,
    /// Wakes the background thread.
    bg_cv: Condvar,
    /// Wakes writers (queue progress, flush/compaction finished).
    cv: Condvar,
    shutting_down: AtomicBool,
    stats: Stats,
}

pub struct Db {
    inner: Arc<Inner>,
    bg: Option<JoinHandle<()>>,
}

/// A consistent point-in-time view; reads through it ignore later writes.
/// Old versions it can see are kept by compaction until it is dropped.
pub struct Snapshot {
    seq: SeqNum,
    inner: Arc<Inner>,
}

impl Snapshot {
    pub fn sequence(&self) -> SeqNum {
        self.seq
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let mut st = self.inner.state.lock().unwrap();
        if let Some(c) = st.snapshots.get_mut(&self.seq) {
            *c -= 1;
            if *c == 0 {
                st.snapshots.remove(&self.seq);
            }
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct ReadOptions<'a> {
    pub snapshot: Option<&'a Snapshot>,
}

fn parse_file_name(name: &str) -> Option<(u64, &str)> {
    let (num, ext) = name.split_once('.')?;
    Some((num.parse().ok()?, ext))
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let mut versions = VersionSet::new(&dir);
        let exists = dir.join("CURRENT").exists();
        if exists {
            versions.recover()?;
        } else if !opts.create_if_missing {
            return Err(Error::InvalidArgument(format!(
                "{} does not exist",
                dir.display()
            )));
        }
        let cache = Arc::new(BlockCache::new(opts.block_cache_size));

        // Replay WAL files that weren't flushed before the crash, in order.
        let mut logs: Vec<u64> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                let (n, ext) = parse_file_name(&name)?;
                (ext == "log" && n >= versions.log_number).then_some(n)
            })
            .collect();
        logs.sort();
        let mut edit = VersionEdit::default();
        for &n in &logs {
            versions.next_file = versions.next_file.max(n + 1);
            let (records, _clean) = read_log(&log_path(&dir, n))?;
            let mem = MemTable::new();
            for r in records {
                let (base, count) =
                    WriteBatch::iterate(&r, |s, k, key, val| mem.add(s, k, key, val))?;
                if count > 0 {
                    versions.last_seq = versions.last_seq.max(base + count as u64 - 1);
                }
            }
            if !mem.is_empty() {
                let num = versions.new_file_number();
                let (size, sm, lg) = write_level0(&dir, &opts, &mem, num)?;
                edit.added.push((0, num, size, sm, lg));
            }
        }

        let wal_number = versions.new_file_number();
        let wal = LogWriter::create(&log_path(&dir, wal_number))?;
        // Install recovered tables and the new log number in a fresh MANIFEST.
        versions.log_number = wal_number;
        if !edit.added.is_empty() {
            versions.apply_in_memory(&edit);
        }
        versions.write_new_manifest()?;

        let visible = versions.last_seq;
        let inner = Arc::new(Inner {
            dir,
            opts,
            cache,
            state: Mutex::new(State {
                mem: Arc::new(MemTable::new()),
                imm: None,
                wal_number,
                versions,
                visible_seq: visible,
                snapshots: BTreeMap::new(),
                writers: VecDeque::new(),
                bg_error: None,
                manual_compaction: false,
                bg_busy: false,
            }),
            wal: Mutex::new(wal),
            bg_cv: Condvar::new(),
            cv: Condvar::new(),
            shutting_down: AtomicBool::new(false),
            stats: Stats::default(),
        });
        inner.delete_obsolete_files();
        let bg_inner = inner.clone();
        let bg = std::thread::Builder::new()
            .name("lsm-bg".into())
            .spawn(move || bg_inner.background_loop())
            .expect("spawn background thread");
        inner.bg_cv.notify_one();
        Ok(Db {
            inner,
            bg: Some(bg),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut b = WriteBatch::new();
        b.put(key, value);
        self.write(b)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut b = WriteBatch::new();
        b.delete(key);
        self.write(b)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_opt(batch, self.inner.opts.sync)
    }

    /// Applies `batch` atomically. With `sync`, returns only after the WAL
    /// is fsynced.
    pub fn write_opt(&self, batch: WriteBatch, sync: bool) -> Result<()> {
        self.inner.write(batch, sync)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_opt(key, ReadOptions::default())
    }

    pub fn get_opt(&self, key: &[u8], ro: ReadOptions) -> Result<Option<Vec<u8>>> {
        let (mem, imm, version, seq) = {
            let st = self.inner.state.lock().unwrap();
            let seq = ro.snapshot.map(|s| s.seq).unwrap_or(st.visible_seq);
            (
                st.mem.clone(),
                st.imm.clone(),
                st.versions.current.clone(),
                seq,
            )
        };
        for m in std::iter::once(&mem).chain(imm.as_ref()) {
            match m.get(key, seq) {
                LookupResult::Found(v) => return Ok(Some(v)),
                LookupResult::Deleted => return Ok(None),
                LookupResult::NotFound => {}
            }
        }
        match version.get(key, seq, &self.inner.cache)? {
            GetResult::Found(v) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut st = self.inner.state.lock().unwrap();
        let seq = st.visible_seq;
        *st.snapshots.entry(seq).or_insert(0) += 1;
        Snapshot {
            seq,
            inner: self.inner.clone(),
        }
    }

    /// Iterator over all keys (in order) as of now or a snapshot,
    /// positioned at the first key.
    pub fn iter(&self, ro: ReadOptions) -> DbIterator {
        let (mem, imm, version, seq) = {
            let st = self.inner.state.lock().unwrap();
            let seq = ro.snapshot.map(|s| s.seq).unwrap_or(st.visible_seq);
            (
                st.mem.clone(),
                st.imm.clone(),
                st.versions.current.clone(),
                seq,
            )
        };
        let mut children: Vec<Box<dyn InternalIterator>> = vec![Box::new(mem.iter())];
        if let Some(imm) = &imm {
            children.push(Box::new(imm.iter()));
        }
        for f in version.levels[0].iter().rev() {
            if let Ok(t) = f.table(&self.inner.cache) {
                children.push(Box::new(t.iter()));
            }
        }
        for level in 1..NUM_LEVELS {
            if !version.levels[level].is_empty() {
                children.push(Box::new(LevelIterator::new(
                    version.levels[level].clone(),
                    self.inner.cache.clone(),
                )));
            }
        }
        // The pin keeps the version's files (and memtables) alive.
        let mut it = DbIterator::new(
            Box::new(MergingIterator::new(children)),
            seq,
            Box::new((version, mem, imm)),
        );
        it.seek_to_first();
        it
    }

    /// Keys in [start, end) in order. `None` bounds are open.
    pub fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>, ro: ReadOptions) -> DbIterator {
        let mut it = self.iter(ro);
        it.set_end(end.map(|e| e.to_vec()));
        match start {
            Some(s) => it.seek(s),
            None => it.seek_to_first(),
        }
        it
    }

    /// Flushes the memtable and compacts until no level is over budget,
    /// then pushes everything down as far as it goes. Mostly for tests and
    /// benchmarks.
    pub fn compact_all(&self) -> Result<()> {
        self.flush()?;
        let mut st = self.inner.state.lock().unwrap();
        st.manual_compaction = true;
        self.inner.bg_cv.notify_one();
        while st.manual_compaction || st.bg_busy {
            if let Some(e) = &st.bg_error {
                return Err(Error::Corruption(e.clone()));
            }
            st = self.inner.cv.wait(st).unwrap();
        }
        Ok(())
    }

    /// Forces the current memtable to an L0 table.
    pub fn flush(&self) -> Result<()> {
        let mut st = self.inner.state.lock().unwrap();
        if !st.mem.is_empty() {
            while st.imm.is_some() {
                st = self.inner.cv.wait(st).unwrap();
            }
            st = self.inner.switch_memtable(st)?;
        }
        while st.imm.is_some() {
            if let Some(e) = &st.bg_error {
                return Err(Error::Corruption(e.clone()));
            }
            st = self.inner.cv.wait(st).unwrap();
        }
        Ok(())
    }

    pub fn stats(&self) -> &Stats {
        &self.inner.stats
    }

    /// Files per level, e.g. "2,3,10,0,0,0,0".
    pub fn level_summary(&self) -> String {
        self.inner.state.lock().unwrap().versions.current.summary()
    }

    pub fn level_bytes(&self) -> Vec<u64> {
        let st = self.inner.state.lock().unwrap();
        (0..NUM_LEVELS)
            .map(|l| st.versions.current.level_bytes(l))
            .collect()
    }

    /// (compaction + flush bytes written) / user bytes written.
    pub fn write_amplification(&self) -> f64 {
        let s = &self.inner.stats;
        let user = s.user_bytes_written.load(Ordering::Relaxed).max(1) as f64;
        let disk = (s.flush_bytes.load(Ordering::Relaxed)
            + s.compaction_bytes_written.load(Ordering::Relaxed)) as f64;
        disk / user
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let h = self.inner.cache.hits.load(Ordering::Relaxed) as f64;
        let m = self.inner.cache.misses.load(Ordering::Relaxed) as f64;
        if h + m == 0.0 { 0.0 } else { h / (h + m) }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
        {
            let _st = self.inner.state.lock().unwrap();
            self.inner.bg_cv.notify_all();
        }
        if let Some(h) = self.bg.take() {
            let _ = h.join();
        }
        let _ = self.inner.wal.lock().unwrap().flush(true);
    }
}

fn write_level0(
    dir: &Path,
    opts: &Options,
    mem: &MemTable,
    num: u64,
) -> Result<(u64, Vec<u8>, Vec<u8>)> {
    let mut b = TableBuilder::create(&table_path(dir, num), opts.table)?;
    let mut it = mem.iter();
    it.seek_to_first();
    while it.valid() {
        b.add(it.key(), it.value())?;
        it.next();
    }
    let (sm, lg) = (b.smallest.clone(), b.largest.clone());
    Ok((b.finish()?, sm, lg))
}

type Guard<'a> = MutexGuard<'a, State>;

impl Inner {
    fn write(&self, batch: WriteBatch, sync: bool) -> Result<()> {
        let me = Arc::new(Mutex::new(Writer {
            batch,
            sync,
            done: false,
            result: None,
        }));
        let mut st = self.state.lock().unwrap();
        st.writers.push_back(me.clone());
        loop {
            {
                let mut w = me.lock().unwrap();
                if w.done {
                    return w.result.take().unwrap().map_err(Error::Corruption);
                }
            }
            if Arc::ptr_eq(st.writers.front().unwrap(), &me) {
                break;
            }
            st = self.cv.wait(st).unwrap();
        }

        // ---- We are the leader for a write group.
        let (mut st, room) = self.make_room_for_write(st);
        let group_result: std::result::Result<(), String> = match room {
            Err(e) => Err(e.to_string()),
            Ok(()) => {
                // Build the group: as many queued batches as fit in ~1 MiB.
                // A non-sync leader must not absorb a sync writer (it would
                // return before that writer's data is fsynced).
                let leader_sync = me.lock().unwrap().sync;
                let mut merged = WriteBatch::new();
                let mut group = 0;
                let mut bytes = 0;
                for w in st.writers.iter() {
                    let w = w.lock().unwrap();
                    if group > 0
                        && ((w.sync && !leader_sync)
                            || bytes + w.batch.approximate_size() > (1 << 20))
                    {
                        break;
                    }
                    bytes += w.batch.approximate_size();
                    merged.append(&w.batch);
                    group += 1;
                }
                let base = st.versions.last_seq + 1;
                merged.set_seq(base);
                st.versions.last_seq += merged.len() as u64;
                let mem = st.mem.clone();
                drop(st);

                // WAL append + memtable insert without the state lock: only
                // the leader gets here, so the WAL has a single writer.
                let r = (|| -> Result<()> {
                    let mut wal = self.wal.lock().unwrap();
                    wal.add_record(merged.encoded())?;
                    wal.flush(leader_sync)?;
                    drop(wal);
                    WriteBatch::iterate(merged.encoded(), |s, k, key, val| {
                        mem.add(s, k, key, val)
                    })?;
                    Ok(())
                })();
                self.stats
                    .user_bytes_written
                    .fetch_add(bytes as u64, Ordering::Relaxed);
                self.stats.write_groups.fetch_add(1, Ordering::Relaxed);
                self.stats.writes.fetch_add(group as u64, Ordering::Relaxed);

                st = self.state.lock().unwrap();
                match r {
                    Ok(()) => {
                        // Publish: readers may now see this group.
                        st.visible_seq = base + merged.len() as u64 - 1;
                        // Mark followers done (leader is popped below).
                        for w in st.writers.iter().take(group).skip(1) {
                            let mut w = w.lock().unwrap();
                            w.done = true;
                            w.result = Some(Ok(()));
                        }
                        for _ in 0..group {
                            st.writers.pop_front();
                        }
                        Ok(())
                    }
                    Err(e) => {
                        // A failed WAL write leaves the log in an unknown
                        // state; refuse further writes.
                        st.bg_error = Some(format!("wal write failed: {e}"));
                        let msg = e.to_string();
                        for w in st.writers.iter().take(group).skip(1) {
                            let mut w = w.lock().unwrap();
                            w.done = true;
                            w.result = Some(Err(msg.clone()));
                        }
                        for _ in 0..group {
                            st.writers.pop_front();
                        }
                        Err(msg)
                    }
                }
            }
        };
        if group_result.is_err() && st.writers.front().is_some_and(|w| Arc::ptr_eq(w, &me)) {
            st.writers.pop_front();
        }
        self.cv.notify_all();
        drop(st);
        group_result.map_err(Error::Corruption)
    }

    /// Ensures the memtable has room, freezing it and switching WALs if it
    /// is full. Applies backpressure when L0 is getting deep.
    fn make_room_for_write<'a>(&'a self, mut st: Guard<'a>) -> (Guard<'a>, Result<()>) {
        let mut delayed = false;
        loop {
            if let Some(e) = &st.bg_error {
                let e = Error::Corruption(e.clone());
                return (st, Err(e));
            }
            let l0 = st.versions.current.levels[0].len();
            if !delayed && l0 >= self.opts.l0_slowdown_trigger {
                // Soft limit: delay this write by 1ms so compaction can keep
                // up, rather than stalling hard later.
                drop(st);
                let t = Instant::now();
                std::thread::sleep(Duration::from_millis(1));
                self.stats
                    .stall_micros
                    .fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                delayed = true;
                st = self.state.lock().unwrap();
                continue;
            }
            if st.mem.approximate_size() < self.opts.write_buffer_size {
                return (st, Ok(()));
            }
            if st.imm.is_some() || l0 >= self.opts.l0_stop_trigger {
                // Previous memtable still flushing, or L0 is full: wait.
                let t = Instant::now();
                st = self
                    .cv
                    .wait_timeout(st, Duration::from_millis(100))
                    .unwrap()
                    .0;
                self.stats
                    .stall_micros
                    .fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                continue;
            }
            match self.switch_memtable(st) {
                Ok(g) => st = g,
                Err(e) => return (self.state.lock().unwrap(), Err(e)),
            }
        }
    }

    /// Freezes the active memtable and starts a new WAL file.
    fn switch_memtable<'a>(&'a self, mut st: Guard<'a>) -> Result<Guard<'a>> {
        let num = st.versions.new_file_number();
        let new_wal = LogWriter::create(&log_path(&self.dir, num))?;
        {
            let mut wal = self.wal.lock().unwrap();
            wal.flush(true)?; // old log fully durable before we stop writing it
            *wal = new_wal;
        }
        st.wal_number = num;
        let old = std::mem::replace(&mut st.mem, Arc::new(MemTable::new()));
        st.imm = Some(old);
        self.bg_cv.notify_one();
        Ok(st)
    }

    fn smallest_snapshot(st: &State) -> SeqNum {
        st.snapshots
            .keys()
            .next()
            .copied()
            .unwrap_or(st.versions.last_seq)
    }

    fn background_loop(self: Arc<Self>) {
        let mut st = self.state.lock().unwrap();
        loop {
            if self.shutting_down.load(Ordering::SeqCst) {
                break;
            }
            if st.bg_error.is_some() {
                st.manual_compaction = false;
                self.cv.notify_all();
                st = self.bg_cv.wait(st).unwrap();
                continue;
            }
            st.bg_busy = true;
            let r = if st.imm.is_some() {
                self.flush_imm(st)
            } else if let Some(c) = compaction::pick_compaction(&st.versions, &self.opts, None) {
                self.do_compaction(st, c)
            } else if st.manual_compaction {
                // Push data down: compact the shallowest non-empty level
                // that isn't the last populated one.
                let v = st.versions.current.clone();
                let last = (0..NUM_LEVELS)
                    .rev()
                    .find(|&l| !v.levels[l].is_empty())
                    .unwrap_or(0);
                match (0..last).find(|&l| !v.levels[l].is_empty()) {
                    Some(l) => {
                        let c =
                            compaction::pick_compaction(&st.versions, &self.opts, Some(l)).unwrap();
                        self.do_compaction(st, c)
                    }
                    None => {
                        st.manual_compaction = false;
                        Ok(st)
                    }
                }
            } else {
                st.bg_busy = false;
                self.cv.notify_all();
                st = self.bg_cv.wait(st).unwrap();
                continue;
            };
            st = match r {
                Ok(g) => g,
                Err((g, e)) => {
                    let mut g = g;
                    g.bg_error = Some(e.to_string());
                    g
                }
            };
            st.bg_busy = false;
            self.cv.notify_all();
        }
    }

    fn flush_imm<'a>(
        &'a self,
        mut st: Guard<'a>,
    ) -> std::result::Result<Guard<'a>, (Guard<'a>, Error)> {
        let imm = st.imm.clone().unwrap();
        let num = st.versions.new_file_number();
        let wal_number = st.wal_number;
        drop(st);

        let res = write_level0(&self.dir, &self.opts, &imm, num);
        let mut st = self.state.lock().unwrap();
        let (size, sm, lg) = match res {
            Ok(x) => x,
            Err(e) => return Err((st, e)),
        };
        let edit = VersionEdit {
            // Logs older than the current WAL are no longer needed.
            log_number: Some(wal_number),
            added: vec![(0, num, size, sm, lg)],
            ..Default::default()
        };
        if let Err(e) = st.versions.log_and_apply(edit) {
            return Err((st, e));
        }
        st.imm = None;
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        self.stats.flush_bytes.fetch_add(size, Ordering::Relaxed);
        self.delete_obsolete_logs(&st);
        Ok(st)
    }

    fn do_compaction<'a>(
        &'a self,
        mut st: Guard<'a>,
        c: Compaction,
    ) -> std::result::Result<Guard<'a>, (Guard<'a>, Error)> {
        let out_level = c.level + 1;
        if c.is_trivial_move() {
            // Nothing to merge with: just re-parent the file.
            let f = &c.inputs[0][0];
            let edit = VersionEdit {
                deleted: vec![(c.level, f.number)],
                added: vec![(
                    out_level,
                    f.number,
                    f.size,
                    f.smallest.clone(),
                    f.largest.clone(),
                )],
                compact_pointers: vec![(c.level, f.largest.clone())],
                ..Default::default()
            };
            // Version::apply moves the same FileMeta, so the file survives.
            if let Err(e) = st.versions.log_and_apply(edit) {
                return Err((st, e));
            }
            self.stats.trivial_moves.fetch_add(1, Ordering::Relaxed);
            return Ok(st);
        }

        let smallest_snapshot = Self::smallest_snapshot(&st);
        drop(st);

        let mut nums = Vec::new();
        let res = compaction::run_compaction(
            &c,
            &self.dir,
            &self.opts,
            &self.cache,
            smallest_snapshot,
            || {
                let n = self.state.lock().unwrap().versions.new_file_number();
                nums.push(n);
                n
            },
        );
        let mut st = self.state.lock().unwrap();
        let out = match res {
            Ok(o) => o,
            Err(e) => {
                for n in nums {
                    let _ = std::fs::remove_file(table_path(&self.dir, n));
                }
                return Err((st, e));
            }
        };
        let mut edit = VersionEdit::default();
        for (i, files) in c.inputs.iter().enumerate() {
            for f in files {
                edit.deleted.push((c.level + i, f.number));
            }
        }
        for (num, size, sm, lg) in out.files {
            edit.added.push((out_level, num, size, sm, lg));
        }
        if c.level > 0 {
            let largest = c.inputs[0]
                .iter()
                .map(|f| f.largest.clone())
                .max_by(|a, b| cmp_internal(a, b))
                .unwrap();
            edit.compact_pointers.push((c.level, largest));
        }
        if let Err(e) = st.versions.log_and_apply(edit) {
            return Err((st, e));
        }
        self.stats.compactions.fetch_add(1, Ordering::Relaxed);
        self.stats
            .compaction_bytes_read
            .fetch_add(c.input_bytes(), Ordering::Relaxed);
        self.stats
            .compaction_bytes_written
            .fetch_add(out.bytes_written, Ordering::Relaxed);
        Ok(st)
    }

    fn delete_obsolete_logs(&self, st: &State) {
        let keep_from = st.versions.log_number;
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                if let Some(name) = e.file_name().to_str()
                    && let Some((n, "log")) = parse_file_name(name)
                    && n < keep_from
                {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    }

    /// On open: remove tables not referenced by the current version (e.g.
    /// outputs of a compaction that crashed), old logs and old manifests.
    fn delete_obsolete_files(&self) {
        let st = self.state.lock().unwrap();
        let live: std::collections::HashSet<u64> = st
            .versions
            .current
            .levels
            .iter()
            .flatten()
            .map(|f| f.number)
            .collect();
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let Some(name) = e.file_name().to_str().map(|s| s.to_string()) else {
                    continue;
                };
                let remove = if let Some((n, ext)) = parse_file_name(&name) {
                    match ext {
                        "sst" => !live.contains(&n),
                        "log" => n < st.versions.log_number,
                        _ => false,
                    }
                } else if let Some(n) = name
                    .strip_prefix("MANIFEST-")
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    n != st.versions.manifest_number
                } else {
                    false
                };
                if remove {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    }
}

/// Concatenating iterator over one sorted, non-overlapping level. Opens
/// tables lazily, so a seek touches only the one file that can match.
struct LevelIterator {
    files: Vec<Arc<FileMeta>>,
    cache: Arc<BlockCache>,
    idx: usize,
    cur: Option<TableIterator>,
    err: Option<String>,
}

impl LevelIterator {
    fn new(files: Vec<Arc<FileMeta>>, cache: Arc<BlockCache>) -> Self {
        LevelIterator {
            idx: files.len(),
            files,
            cache,
            cur: None,
            err: None,
        }
    }

    fn open(&mut self, idx: usize) {
        self.idx = idx;
        self.cur = None;
        if idx < self.files.len() {
            match self.files[idx].table(&self.cache) {
                Ok(t) => self.cur = Some(t.iter()),
                Err(e) => self.err = Some(e.to_string()),
            }
        }
    }

    fn skip_forward(&mut self) {
        while self.cur.as_ref().is_some_and(|c| !c.valid()) {
            if let Some(c) = &self.cur
                && let Err(e) = c.status()
            {
                self.err = Some(e.to_string());
                self.cur = None;
                return;
            }
            let next = self.idx + 1;
            self.open(next);
            if let Some(c) = &mut self.cur {
                c.seek_to_first();
            }
        }
    }
}

impl InternalIterator for LevelIterator {
    fn valid(&self) -> bool {
        self.cur.as_ref().is_some_and(|c| c.valid())
    }
    fn seek_to_first(&mut self) {
        self.open(0);
        if let Some(c) = &mut self.cur {
            c.seek_to_first();
        }
        self.skip_forward();
    }
    fn seek(&mut self, target: &[u8]) {
        let i = self
            .files
            .partition_point(|f| cmp_internal(&f.largest, target).is_lt());
        self.open(i);
        if let Some(c) = &mut self.cur {
            c.seek(target);
        }
        self.skip_forward();
    }
    fn next(&mut self) {
        if let Some(c) = &mut self.cur {
            c.next();
        }
        self.skip_forward();
    }
    fn key(&self) -> &[u8] {
        self.cur.as_ref().unwrap().key()
    }
    fn value(&self) -> &[u8] {
        self.cur.as_ref().unwrap().value()
    }
    fn status(&self) -> Result<()> {
        if let Some(e) = &self.err {
            return Err(Error::Corruption(e.clone()));
        }
        self.cur.as_ref().map_or(Ok(()), |c| c.status())
    }
}
