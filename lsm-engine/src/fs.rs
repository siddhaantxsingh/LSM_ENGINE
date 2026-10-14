//! File system abstraction.
//!
//! Every file the engine touches goes through [`FileSystem`]. The default
//! is [`StdFs`] (the real OS). [`SimFs`] is an in-memory file system that
//! models what survives a power loss, and can "crash" after any number of
//! mutating operations. Tests use it to crash the engine at every write,
//! fsync, rename and delete, then recover from the surviving bytes.
//!
//! Durability model used by `SimFs` (strict POSIX):
//! * File data is durable only up to the last `sync` of that file.
//! * A directory entry (create, rename, remove) is durable only after
//!   `sync_dir` on its directory.
//! * Files are append-only after `create` (which always makes a new, empty
//!   file), which is how the engine uses them.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A file opened for appending.
pub trait WritableFile: Send {
    /// Appends bytes. They reach the OS but are not durable until `sync`.
    fn append(&mut self, data: &[u8]) -> io::Result<()>;
    /// Makes every appended byte durable (fsync / fdatasync).
    fn sync(&mut self) -> io::Result<()>;
}

/// A file opened for positional reads.
pub trait RandomAccessFile: Send + Sync {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    /// File length when it was opened.
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait FileSystem: Send + Sync + fmt::Debug {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Creates (or truncates) a file for appending.
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>>;
    fn open(&self, path: &Path) -> io::Result<Arc<dyn RandomAccessFile>>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn exists(&self, path: &Path) -> bool;
    fn file_size(&self, path: &Path) -> io::Result<u64>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    /// File names (not paths) directly inside `dir`.
    fn list_dir(&self, dir: &Path) -> io::Result<Vec<String>>;
    /// Makes creates/renames/removes in `dir` durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

// ------------------------------------------------------------------ StdFs

/// The real file system.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdFs;

struct StdWritable(std::fs::File);

impl WritableFile for StdWritable {
    fn append(&mut self, data: &[u8]) -> io::Result<()> {
        use std::io::Write;
        self.0.write_all(data)
    }
    fn sync(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }
}

struct StdRandom {
    file: std::fs::File,
    len: u64,
}

impl RandomAccessFile for StdRandom {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(buf, offset)
    }
    fn len(&self) -> u64 {
        self.len
    }
}

impl FileSystem for StdFs {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        Ok(Box::new(StdWritable(std::fs::File::create(path)?)))
    }
    fn open(&self, path: &Path) -> io::Result<Arc<dyn RandomAccessFile>> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        Ok(Arc::new(StdRandom { file, len }))
    }
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }
    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn file_size(&self, path: &Path) -> io::Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
    fn list_dir(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir)? {
            if let Ok(name) = e?.file_name().into_string() {
                out.push(name);
            }
        }
        Ok(out)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::File::open(dir)?.sync_all()
    }
}

// ------------------------------------------------------------------ SimFs

/// What a simulated power loss does to bytes that were appended but not
/// yet synced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsyncedData {
    /// All unsynced bytes are lost.
    Drop,
    /// A random-length prefix of the unsynced bytes survives (torn write).
    TornPrefix,
    /// A random-length prefix survives and the rest of the unsynced range
    /// reads back as zeros (file size updated, data blocks never written).
    ZeroTail,
    /// Every unsynced byte survives (the OS happened to write it back).
    KeepAll,
}

/// What a simulated power loss does to directory operations (create,
/// rename, remove) that were not followed by `sync_dir`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsyncedDirOps {
    /// Lost: the directory looks as it did at the last `sync_dir`.
    Drop,
    /// Kept (common on journaling file systems).
    Keep,
}

#[derive(Clone, Copy, Debug)]
pub struct CrashMode {
    pub data: UnsyncedData,
    pub dir_ops: UnsyncedDirOps,
    /// Seed for the random torn-prefix lengths.
    pub seed: u64,
}

/// Kinds of mutating operations, for coverage reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OpKind {
    Create,
    Append,
    Sync,
    Rename,
    Remove,
    SyncDir,
}

#[derive(Default)]
struct Inode {
    data: Vec<u8>,
    synced_len: usize,
}

type InodeRef = Arc<Mutex<Inode>>;

#[derive(Default)]
struct SimState {
    /// Directory as the running process sees it.
    live: BTreeMap<PathBuf, InodeRef>,
    /// Directory entries as of the last sync_dir.
    durable: BTreeMap<PathBuf, InodeRef>,
    /// Durable image captured at the moment of the simulated crash.
    crashed: Option<Box<CrashImage>>,
    op_log: Vec<OpKind>,
}

/// Snapshot of everything needed to build the post-crash file system.
struct CrashImage {
    live: Vec<(PathBuf, Vec<u8>, usize)>,
    durable: Vec<(PathBuf, Vec<u8>, usize)>,
}

/// In-memory file system with power-loss simulation and crash injection.
#[derive(Clone)]
pub struct SimFs {
    state: Arc<Mutex<SimState>>,
    ops: Arc<AtomicU64>,
    crash_at: Arc<AtomicU64>,
}

impl fmt::Debug for SimFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimFs(ops={})", self.ops.load(Ordering::SeqCst))
    }
}

impl Default for SimFs {
    fn default() -> Self {
        Self::new()
    }
}

fn crashed_err() -> io::Error {
    io::Error::other("simulated crash: file system is gone")
}

fn not_found(p: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("{} not found", p.display()),
    )
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

impl SimFs {
    pub fn new() -> Self {
        SimFs {
            state: Arc::new(Mutex::new(SimState::default())),
            ops: Arc::new(AtomicU64::new(0)),
            crash_at: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    /// The file system "loses power" just before the `n`-th mutating
    /// operation (1-based), which therefore never happens; every later
    /// operation fails.
    pub fn crash_before_op(&self, n: u64) {
        self.crash_at.store(n, Ordering::SeqCst);
    }

    /// Number of mutating operations performed so far.
    pub fn op_count(&self) -> u64 {
        self.ops.load(Ordering::SeqCst)
    }

    /// Kinds of the mutating operations performed so far, in order.
    pub fn op_log(&self) -> Vec<OpKind> {
        self.state.lock().unwrap().op_log.clone()
    }

    pub fn has_crashed(&self) -> bool {
        self.state.lock().unwrap().crashed.is_some()
    }

    /// Counts the op and decides whether the crash happens now. Called with
    /// the state lock held so the image matches the op order exactly.
    fn begin_op(&self, st: &mut SimState, kind: OpKind) -> io::Result<()> {
        if st.crashed.is_some() {
            return Err(crashed_err());
        }
        let n = self.ops.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= self.crash_at.load(Ordering::SeqCst) {
            st.crashed = Some(Box::new(Self::capture(st)));
            return Err(crashed_err());
        }
        st.op_log.push(kind);
        Ok(())
    }

    fn capture(st: &SimState) -> CrashImage {
        let snap = |m: &BTreeMap<PathBuf, InodeRef>| {
            m.iter()
                .map(|(p, i)| {
                    let i = i.lock().unwrap();
                    (p.clone(), i.data.clone(), i.synced_len)
                })
                .collect()
        };
        CrashImage {
            live: snap(&st.live),
            durable: snap(&st.durable),
        }
    }

    /// Builds the file system that a restarted process would see after a
    /// power loss at this moment (or at the injected crash point, if one
    /// happened). Everything in the result is durable.
    pub fn crash_image(&self, mode: CrashMode) -> SimFs {
        let st = self.state.lock().unwrap();
        let img = match &st.crashed {
            Some(i) => CrashImage {
                live: i.live.clone(),
                durable: i.durable.clone(),
            },
            None => Self::capture(&st),
        };
        drop(st);
        let entries = match mode.dir_ops {
            UnsyncedDirOps::Drop => img.durable,
            UnsyncedDirOps::Keep => img.live,
        };
        let mut rng = mode.seed | 1;
        let out = SimFs::new();
        {
            let mut ost = out.state.lock().unwrap();
            for (path, data, synced) in entries {
                let unsynced = data.len() - synced;
                let mut bytes = data[..synced].to_vec();
                match mode.data {
                    UnsyncedData::Drop => {}
                    UnsyncedData::KeepAll => bytes.extend_from_slice(&data[synced..]),
                    UnsyncedData::TornPrefix => {
                        let keep = (xorshift(&mut rng) % (unsynced as u64 + 1)) as usize;
                        bytes.extend_from_slice(&data[synced..synced + keep]);
                    }
                    UnsyncedData::ZeroTail => {
                        let keep = (xorshift(&mut rng) % (unsynced as u64 + 1)) as usize;
                        bytes.extend_from_slice(&data[synced..synced + keep]);
                        bytes.resize(data.len(), 0);
                    }
                }
                let len = bytes.len();
                let inode = Arc::new(Mutex::new(Inode {
                    data: bytes,
                    synced_len: len,
                }));
                ost.live.insert(path.clone(), inode.clone());
                ost.durable.insert(path, inode);
            }
        }
        out
    }

    /// Raw contents of a file as the running process sees it (for tests).
    pub fn file_contents(&self, path: &Path) -> Option<Vec<u8>> {
        let st = self.state.lock().unwrap();
        st.live.get(path).map(|i| i.lock().unwrap().data.clone())
    }

    /// Replaces a file's contents in place, durably (for corruption tests).
    pub fn set_file_contents(&self, path: &Path, data: Vec<u8>) {
        let st = self.state.lock().unwrap();
        if let Some(i) = st.live.get(path) {
            let mut i = i.lock().unwrap();
            i.synced_len = data.len();
            i.data = data;
        }
    }
}

struct SimWritable {
    fs: SimFs,
    inode: InodeRef,
}

impl WritableFile for SimWritable {
    fn append(&mut self, data: &[u8]) -> io::Result<()> {
        let mut st = self.fs.state.lock().unwrap();
        self.fs.begin_op(&mut st, OpKind::Append)?;
        self.inode.lock().unwrap().data.extend_from_slice(data);
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        let mut st = self.fs.state.lock().unwrap();
        self.fs.begin_op(&mut st, OpKind::Sync)?;
        let mut i = self.inode.lock().unwrap();
        i.synced_len = i.data.len();
        Ok(())
    }
}

struct SimRandom {
    data: Vec<u8>,
}

impl RandomAccessFile for SimRandom {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let start = usize::try_from(offset).map_err(|_| io::ErrorKind::UnexpectedEof)?;
        let end = start
            .checked_add(buf.len())
            .filter(|&e| e <= self.data.len())
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        buf.copy_from_slice(&self.data[start..end]);
        Ok(())
    }
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl FileSystem for SimFs {
    fn create_dir_all(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
    fn create(&self, path: &Path) -> io::Result<Box<dyn WritableFile>> {
        let mut st = self.state.lock().unwrap();
        self.begin_op(&mut st, OpKind::Create)?;
        let inode: InodeRef = Arc::default();
        st.live.insert(path.to_path_buf(), inode.clone());
        Ok(Box::new(SimWritable {
            fs: self.clone(),
            inode,
        }))
    }
    fn open(&self, path: &Path) -> io::Result<Arc<dyn RandomAccessFile>> {
        let data = self.read(path)?;
        Ok(Arc::new(SimRandom { data }))
    }
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let st = self.state.lock().unwrap();
        if st.crashed.is_some() {
            return Err(crashed_err());
        }
        st.live
            .get(path)
            .map(|i| i.lock().unwrap().data.clone())
            .ok_or_else(|| not_found(path))
    }
    fn exists(&self, path: &Path) -> bool {
        self.state.lock().unwrap().live.contains_key(path)
    }
    fn file_size(&self, path: &Path) -> io::Result<u64> {
        let st = self.state.lock().unwrap();
        st.live
            .get(path)
            .map(|i| i.lock().unwrap().data.len() as u64)
            .ok_or_else(|| not_found(path))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        if !st.live.contains_key(from) {
            return Err(not_found(from));
        }
        self.begin_op(&mut st, OpKind::Rename)?;
        let inode = st.live.remove(from).unwrap();
        st.live.insert(to.to_path_buf(), inode);
        Ok(())
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        if !st.live.contains_key(path) {
            return Err(not_found(path));
        }
        self.begin_op(&mut st, OpKind::Remove)?;
        st.live.remove(path);
        Ok(())
    }
    fn list_dir(&self, dir: &Path) -> io::Result<Vec<String>> {
        let st = self.state.lock().unwrap();
        if st.crashed.is_some() {
            return Err(crashed_err());
        }
        Ok(st
            .live
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .filter_map(|p| p.file_name()?.to_str().map(str::to_string))
            .collect())
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        self.begin_op(&mut st, OpKind::SyncDir)?;
        let st = &mut *st;
        st.durable.retain(|p, _| p.parent() != Some(dir));
        for (p, i) in st.live.iter() {
            if p.parent() == Some(dir) {
                st.durable.insert(p.clone(), i.clone());
            }
        }
        Ok(())
    }
}

/// Counts of each operation kind (for test coverage reports).
pub fn op_histogram(ops: &[OpKind]) -> HashMap<OpKind, usize> {
    let mut h = HashMap::new();
    for o in ops {
        *h.entry(*o).or_insert(0) += 1;
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simfs_durability_model() {
        let fs = SimFs::new();
        let d = Path::new("/db");
        let a = d.join("a");
        let mut f = fs.create(&a).unwrap();
        f.append(b"hello").unwrap();
        f.sync().unwrap();
        f.append(b" world").unwrap();
        // File created but directory never synced: gone after power loss.
        let img = fs.crash_image(CrashMode {
            data: UnsyncedData::KeepAll,
            dir_ops: UnsyncedDirOps::Drop,
            seed: 1,
        });
        assert!(!img.exists(&a));
        fs.sync_dir(d).unwrap();
        let img = fs.crash_image(CrashMode {
            data: UnsyncedData::Drop,
            dir_ops: UnsyncedDirOps::Drop,
            seed: 1,
        });
        assert_eq!(img.read(&a).unwrap(), b"hello");
        let img = fs.crash_image(CrashMode {
            data: UnsyncedData::ZeroTail,
            dir_ops: UnsyncedDirOps::Drop,
            seed: 7,
        });
        let got = img.read(&a).unwrap();
        assert_eq!(got.len(), 11);
        assert_eq!(&got[..5], b"hello");
    }

    #[test]
    fn simfs_crash_point_stops_everything() {
        let fs = SimFs::new();
        fs.crash_before_op(3);
        let p = Path::new("/db/x");
        let mut f = fs.create(p).unwrap(); // op 1
        f.append(b"1").unwrap(); // op 2
        assert!(f.append(b"2").is_err()); // op 3: crash
        assert!(f.sync().is_err());
        assert!(fs.has_crashed());
        let img = fs.crash_image(CrashMode {
            data: UnsyncedData::KeepAll,
            dir_ops: UnsyncedDirOps::Keep,
            seed: 1,
        });
        assert_eq!(img.read(p).unwrap(), b"1");
    }
}
