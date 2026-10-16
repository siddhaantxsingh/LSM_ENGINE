//! Versions: the immutable set of live SSTables per level, and the
//! MANIFEST that durably records every change to that set.
//!
//! A `Version` is never mutated. Flushes and compactions produce a
//! `VersionEdit` (files added/removed), which is appended to the MANIFEST
//! and applied to produce a new `Version`. Readers grab an `Arc<Version>`
//! and keep using it even if compaction replaces it, so reads never block
//! on compaction. An SSTable is deleted from disk only when the last
//! version referencing it is dropped (see `FileMeta::drop`).

use crate::cache::BlockCache;
use crate::coding::*;
use crate::error::{Result, corruption};
use crate::fs::FileSystem;
use crate::key::*;
use crate::log::{LAST_RECORD_CHECKSUM, LogEnd, LogWriter, read_log};
use crate::sstable::Table;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub const NUM_LEVELS: usize = 7;

pub fn table_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("{n:06}.sst"))
}
pub fn log_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("{n:06}.log"))
}
pub fn manifest_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{n:06}"))
}

pub struct FileMeta {
    pub number: u64,
    pub size: u64,
    pub smallest: Vec<u8>, // internal keys
    pub largest: Vec<u8>,
    path: PathBuf,
    fs: Arc<dyn FileSystem>,
    table: Mutex<Option<Arc<Table>>>,
    obsolete: AtomicBool,
}

impl FileMeta {
    pub fn new(
        fs: &Arc<dyn FileSystem>,
        dir: &Path,
        number: u64,
        size: u64,
        smallest: Vec<u8>,
        largest: Vec<u8>,
    ) -> Self {
        FileMeta {
            number,
            size,
            smallest,
            largest,
            path: table_path(dir, number),
            fs: fs.clone(),
            table: Mutex::new(None),
            obsolete: AtomicBool::new(false),
        }
    }

    /// Lazily opens the table. The open Table lives as long as this meta.
    pub fn table(&self, cache: &Arc<BlockCache>) -> Result<Arc<Table>> {
        let mut t = self.table.lock().unwrap();
        if let Some(t) = &*t {
            return Ok(t.clone());
        }
        let table = Arc::new(Table::open(
            &*self.fs,
            &self.path,
            self.number,
            Some(cache.clone()),
        )?);
        *t = Some(table.clone());
        Ok(table)
    }

    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::SeqCst);
    }

    pub fn smallest_user(&self) -> &[u8] {
        user_key(&self.smallest)
    }
    pub fn largest_user(&self) -> &[u8] {
        user_key(&self.largest)
    }
}

impl Drop for FileMeta {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::SeqCst) {
            let _ = self.fs.remove_file(&self.path);
        }
    }
}

/// (level, file number, file size, smallest internal key, largest internal key)
pub type NewFile = (usize, u64, u64, Vec<u8>, Vec<u8>);

#[derive(Default, Debug, Clone)]
pub struct VersionEdit {
    pub log_number: Option<u64>,
    pub next_file: Option<u64>,
    pub last_seq: Option<SeqNum>,
    pub deleted: Vec<(usize, u64)>,
    pub added: Vec<NewFile>,
    pub compact_pointers: Vec<(usize, Vec<u8>)>,
}

impl VersionEdit {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        if let Some(v) = self.log_number {
            put_varint(&mut b, 1);
            put_varint(&mut b, v);
        }
        if let Some(v) = self.next_file {
            put_varint(&mut b, 2);
            put_varint(&mut b, v);
        }
        if let Some(v) = self.last_seq {
            put_varint(&mut b, 3);
            put_varint(&mut b, v);
        }
        for (l, n) in &self.deleted {
            put_varint(&mut b, 4);
            put_varint(&mut b, *l as u64);
            put_varint(&mut b, *n);
        }
        for (l, n, s, sm, lg) in &self.added {
            put_varint(&mut b, 5);
            put_varint(&mut b, *l as u64);
            put_varint(&mut b, *n);
            put_varint(&mut b, *s);
            put_length_prefixed(&mut b, sm);
            put_length_prefixed(&mut b, lg);
        }
        for (l, k) in &self.compact_pointers {
            put_varint(&mut b, 6);
            put_varint(&mut b, *l as u64);
            put_length_prefixed(&mut b, k);
        }
        b
    }

    pub fn decode(mut b: &[u8]) -> Result<VersionEdit> {
        let mut e = VersionEdit::default();
        let level = |l: u64| -> Result<usize> {
            if l < NUM_LEVELS as u64 {
                Ok(l as usize)
            } else {
                corruption(format!("level {l} out of range"))
            }
        };
        let var = |b: &mut &[u8]| -> Result<u64> {
            let (v, n) = get_varint(b)?;
            *b = &b[n..];
            Ok(v)
        };
        let lp = |b: &mut &[u8]| -> Result<Vec<u8>> {
            let (s, n) = get_length_prefixed(b)?;
            let v = s.to_vec();
            *b = &b[n..];
            Ok(v)
        };
        while !b.is_empty() {
            match var(&mut b)? {
                1 => e.log_number = Some(var(&mut b)?),
                2 => e.next_file = Some(var(&mut b)?),
                3 => e.last_seq = Some(var(&mut b)?),
                4 => {
                    let l = level(var(&mut b)?)?;
                    e.deleted.push((l, var(&mut b)?));
                }
                5 => {
                    let l = level(var(&mut b)?)?;
                    let n = var(&mut b)?;
                    let s = var(&mut b)?;
                    let sm = lp(&mut b)?;
                    let lg = lp(&mut b)?;
                    if sm.len() < 8 || lg.len() < 8 || cmp_internal(&sm, &lg).is_gt() {
                        return corruption(format!("bad key range for table {n}"));
                    }
                    e.added.push((l, n, s, sm, lg));
                }
                6 => {
                    let l = level(var(&mut b)?)?;
                    e.compact_pointers.push((l, lp(&mut b)?));
                }
                t => return corruption(format!("unknown version edit tag {t}")),
            }
        }
        Ok(e)
    }
}

#[derive(Clone, Default)]
pub struct Version {
    /// L0: sorted by file number (oldest first); files may overlap.
    /// L1+: sorted by smallest key; files never overlap.
    pub levels: [Vec<Arc<FileMeta>>; NUM_LEVELS],
}

pub enum GetResult {
    Found(Vec<u8>),
    Deleted,
    NotFound,
}

impl Version {
    pub fn get(&self, key: &[u8], snapshot: SeqNum, cache: &Arc<BlockCache>) -> Result<GetResult> {
        let ikey = make_internal_key(key, snapshot, SEEK_KIND);
        let check = |f: &FileMeta| -> Result<Option<GetResult>> {
            if let Some((k, v)) = f.table(cache)?.get(&ikey)? {
                return Ok(Some(match kind(&k) {
                    Kind::Put => GetResult::Found(v),
                    Kind::Delete => GetResult::Deleted,
                }));
            }
            Ok(None)
        };
        // L0: newest file first; any may contain the key.
        for f in self.levels[0].iter().rev() {
            if key >= f.smallest_user()
                && key <= f.largest_user()
                && let Some(r) = check(f)?
            {
                return Ok(r);
            }
        }
        // L1+: at most one candidate file per level (binary search).
        for files in &self.levels[1..] {
            let i = files.partition_point(|f| cmp_internal(&f.largest, &ikey).is_lt());
            if i < files.len()
                && key >= files[i].smallest_user()
                && let Some(r) = check(&files[i])?
            {
                return Ok(r);
            }
        }
        Ok(GetResult::NotFound)
    }

    pub fn level_bytes(&self, level: usize) -> u64 {
        self.levels[level].iter().map(|f| f.size).sum()
    }

    /// Files in `level` whose user-key range overlaps [lo, hi].
    pub fn overlapping(&self, level: usize, lo: &[u8], hi: &[u8]) -> Vec<Arc<FileMeta>> {
        self.levels[level]
            .iter()
            .filter(|f| f.largest_user() >= lo && f.smallest_user() <= hi)
            .cloned()
            .collect()
    }

    /// Returns the new version plus the files it drops. The caller marks
    /// those obsolete (deleted from disk once unreferenced) only after the
    /// edit is durable.
    fn apply(
        &self,
        edit: &VersionEdit,
        fs: &Arc<dyn FileSystem>,
        dir: &Path,
    ) -> Result<(Version, Vec<Arc<FileMeta>>)> {
        // Check coherence first: a history that deletes a file it never
        // added, or adds one twice, means the MANIFEST cannot be trusted.
        for &(level, num) in &edit.deleted {
            if !self.levels[level].iter().any(|f| f.number == num) {
                return corruption(format!("edit deletes table {num} not in level {level}"));
            }
        }
        for (level, num, ..) in &edit.added {
            let moved = edit.deleted.iter().any(|d| d.1 == *num);
            if !moved && self.levels.iter().flatten().any(|f| f.number == *num) {
                return corruption(format!("edit adds table {num} twice (level {level})"));
            }
        }
        let mut v = self.clone();
        // A file that is both deleted and added (trivial move to the next
        // level) is moved, not recreated: marking it obsolete would delete
        // a live file once the old version is dropped.
        let added: std::collections::HashSet<u64> = edit.added.iter().map(|a| a.1).collect();
        let mut moved: std::collections::HashMap<u64, Arc<FileMeta>> = Default::default();
        let mut dropped = Vec::new();
        for &(level, num) in &edit.deleted {
            v.levels[level].retain(|f| {
                if f.number != num {
                    return true;
                }
                if added.contains(&num) {
                    moved.insert(num, f.clone());
                } else {
                    dropped.push(f.clone());
                }
                false
            });
        }
        for (level, num, size, sm, lg) in &edit.added {
            let meta = moved.remove(num).unwrap_or_else(|| {
                Arc::new(FileMeta::new(fs, dir, *num, *size, sm.clone(), lg.clone()))
            });
            v.levels[*level].push(meta);
        }
        v.levels[0].sort_by_key(|f| f.number);
        for files in &mut v.levels[1..] {
            files.sort_by(|a, b| cmp_internal(&a.smallest, &b.smallest));
        }
        Ok((v, dropped))
    }

    pub fn summary(&self) -> String {
        self.levels
            .iter()
            .map(|l| l.len().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Holds the current Version plus the MANIFEST writer and counters.
pub struct VersionSet {
    dir: PathBuf,
    fs: Arc<dyn FileSystem>,
    pub current: Arc<Version>,
    pub next_file: u64,
    pub last_seq: SeqNum,
    pub log_number: u64,
    pub manifest_number: u64,
    /// Recovery dropped a torn last MANIFEST record. Tables that only it
    /// referenced are kept aside instead of deleted.
    pub manifest_tail_dropped: bool,
    manifest: Option<LogWriter>,
    pub compact_pointers: [Vec<u8>; NUM_LEVELS],
}

impl VersionSet {
    pub fn new(fs: Arc<dyn FileSystem>, dir: &Path) -> Self {
        VersionSet {
            dir: dir.to_path_buf(),
            fs,
            current: Arc::new(Version::default()),
            next_file: 2,
            last_seq: 0,
            log_number: 0,
            manifest_number: 1,
            manifest_tail_dropped: false,
            manifest: None,
            compact_pointers: Default::default(),
        }
    }

    pub fn new_file_number(&mut self) -> u64 {
        let n = self.next_file;
        self.next_file += 1;
        n
    }

    /// Replays the MANIFEST named by CURRENT. Fails (and changes nothing
    /// on disk) unless the history is complete and coherent and every live
    /// table exists with its recorded size: when metadata is in doubt,
    /// refusing to open is safer than deleting tables.
    pub fn recover(&mut self) -> Result<()> {
        let cur = self.fs.read(&self.dir.join("CURRENT"))?;
        let cur = String::from_utf8_lossy(&cur);
        let name = cur.trim_end_matches('\n');
        let Some(num) = name
            .strip_prefix("MANIFEST-")
            .and_then(|n| n.parse::<u64>().ok())
        else {
            return corruption(format!("CURRENT does not name a MANIFEST: {name:?}"));
        };
        let contents = read_log(&*self.fs, &self.dir.join(name))?;
        match contents.end {
            LogEnd::Clean => {}
            // The last edit was being appended when the process died. It
            // was never synced, so nothing depended on it.
            // Every MANIFEST record is fsynced on its own, so a complete
            // last record with a bad checksum is damage, not a torn write.
            LogEnd::TornTail { offset, reason } if reason == LAST_RECORD_CHECKSUM => {
                return corruption(format!("{name} corrupt at offset {offset}: {reason}"));
            }
            LogEnd::TornTail { .. } => self.manifest_tail_dropped = true,
            LogEnd::Corrupt { offset, reason } => {
                return corruption(format!("{name} corrupt at offset {offset}: {reason}"));
            }
        }
        if contents.records.is_empty() {
            return corruption(format!("{name} has no records"));
        }
        let mut v = Version::default();
        for r in contents.records {
            let e = VersionEdit::decode(&r)?;
            if let Some(n) = e.log_number {
                self.log_number = n;
            }
            if let Some(n) = e.next_file {
                self.next_file = n;
            }
            if let Some(s) = e.last_seq {
                self.last_seq = s;
            }
            for (l, k) in &e.compact_pointers {
                self.compact_pointers[*l] = k.clone();
            }
            // Nothing is marked obsolete during replay: recovery must not
            // delete anything until the whole history checks out. Unused
            // tables are removed by the caller after a successful open.
            v = v.apply(&e, &self.fs, &self.dir)?.0;
        }
        for f in v.levels.iter().flatten() {
            match self.fs.file_size(&f.path) {
                Ok(n) if n == f.size => {}
                Ok(n) => {
                    return corruption(format!(
                        "table {} is {n} bytes, MANIFEST says {}",
                        f.number, f.size
                    ));
                }
                Err(e) => return corruption(format!("table {} missing: {e}", f.number)),
            }
            self.next_file = self.next_file.max(f.number + 1);
        }
        self.next_file = self.next_file.max(num + 1);
        self.manifest_number = num;
        self.current = Arc::new(v);
        Ok(())
    }

    /// Starts a fresh MANIFEST containing a full snapshot of the current
    /// state, then atomically points CURRENT at it.
    pub fn write_new_manifest(&mut self) -> Result<()> {
        // Tables and WALs created before this point must be durable in the
        // directory before a MANIFEST names them.
        self.fs.sync_dir(&self.dir)?;
        let num = self.new_file_number();
        let mut w = LogWriter::create(&*self.fs, &manifest_path(&self.dir, num))?;
        let mut snap = VersionEdit {
            log_number: Some(self.log_number),
            next_file: Some(self.next_file),
            last_seq: Some(self.last_seq),
            ..Default::default()
        };
        for (level, files) in self.current.levels.iter().enumerate() {
            for f in files {
                snap.added.push((
                    level,
                    f.number,
                    f.size,
                    f.smallest.clone(),
                    f.largest.clone(),
                ));
            }
        }
        for (l, k) in self.compact_pointers.iter().enumerate() {
            if !k.is_empty() {
                snap.compact_pointers.push((l, k.clone()));
            }
        }
        w.add_record(&snap.encode())?;
        w.flush(true)?;
        self.fs.sync_dir(&self.dir)?;
        let tmp = self.dir.join("CURRENT.tmp");
        let mut f = self.fs.create(&tmp)?;
        f.append(format!("MANIFEST-{num:06}\n").as_bytes())?;
        f.sync()?;
        drop(f);
        self.fs.rename(&tmp, &self.dir.join("CURRENT"))?;
        self.fs.sync_dir(&self.dir)?;
        self.manifest = Some(w);
        self.manifest_number = num;
        Ok(())
    }

    /// Applies an edit in memory only (used during recovery, before the
    /// fresh MANIFEST that will contain it is written).
    pub fn apply_in_memory(&mut self, edit: &VersionEdit) -> Result<()> {
        let (v, dropped) = self.current.apply(edit, &self.fs, &self.dir)?;
        dropped.iter().for_each(|f| f.mark_obsolete());
        self.current = Arc::new(v);
        Ok(())
    }

    /// Durably records `edit` and installs the resulting version.
    pub fn log_and_apply(&mut self, mut edit: VersionEdit) -> Result<()> {
        if edit.log_number.is_none() {
            edit.log_number = Some(self.log_number);
        }
        edit.next_file = Some(self.next_file);
        edit.last_seq = Some(self.last_seq);
        // Validate before writing so a bad edit never reaches the MANIFEST.
        let (next, dropped) = self.current.apply(&edit, &self.fs, &self.dir)?;
        if !edit.added.is_empty() {
            // New tables must be durable in the directory before the
            // MANIFEST references them (audit defect D7).
            self.fs.sync_dir(&self.dir)?;
        }
        let w = self.manifest.as_mut().expect("manifest open");
        w.add_record(&edit.encode())?;
        w.flush(true)?;
        self.log_number = edit.log_number.unwrap();
        for (l, k) in &edit.compact_pointers {
            self.compact_pointers[*l] = k.clone();
        }
        dropped.iter().for_each(|f| f.mark_obsolete());
        self.current = Arc::new(next);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_roundtrip() {
        let e = VersionEdit {
            log_number: Some(7),
            next_file: Some(9),
            last_seq: Some(1234),
            deleted: vec![(1, 4)],
            added: vec![(
                2,
                8,
                4096,
                b"a\0\0\0\0\0\0\0\0".to_vec(),
                b"z\0\0\0\0\0\0\0\0".to_vec(),
            )],
            compact_pointers: vec![(1, b"m".to_vec())],
        };
        let d = VersionEdit::decode(&e.encode()).unwrap();
        assert_eq!(d.log_number, Some(7));
        assert_eq!(d.added[0].1, 8);
        assert_eq!(d.deleted, vec![(1, 4)]);
        assert_eq!(d.compact_pointers[0].1, b"m");
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;

    /// D8 (medium): out-of-range level numbers in a MANIFEST record were
    /// accepted by decode and then panicked in Version::apply.
    #[test]
    fn d8_decode_rejects_bad_level() {
        let mut b = Vec::new();
        put_varint(&mut b, 4); // deleted file
        put_varint(&mut b, 200); // level
        put_varint(&mut b, 5); // number
        assert!(VersionEdit::decode(&b).is_err());
        let mut b = Vec::new();
        put_varint(&mut b, 5); // added file with a 2-byte internal key
        put_varint(&mut b, 1);
        put_varint(&mut b, 9);
        put_varint(&mut b, 100);
        put_length_prefixed(&mut b, b"ab");
        put_length_prefixed(&mut b, b"cd");
        assert!(VersionEdit::decode(&b).is_err());
    }
}
