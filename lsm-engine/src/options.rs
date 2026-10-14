use crate::fs::{FileSystem, StdFs};
use crate::sstable::TableOptions;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Options {
    /// Create the database if it doesn't exist.
    pub create_if_missing: bool,
    /// Memtable size before it is frozen and flushed to an L0 table.
    pub write_buffer_size: usize,
    /// Target size of SSTables produced by compaction.
    pub target_file_size: u64,
    /// Number of L0 files that triggers a compaction into L1.
    pub l0_compaction_trigger: usize,
    /// Writes are delayed ~1ms each once L0 reaches this many files.
    pub l0_slowdown_trigger: usize,
    /// Writes stop until compaction catches up at this many L0 files.
    pub l0_stop_trigger: usize,
    /// Max bytes in L1; each deeper level is `level_multiplier` times larger.
    pub max_bytes_level1: u64,
    pub level_multiplier: u64,
    /// Block cache capacity in bytes.
    pub block_cache_size: usize,
    /// fsync the WAL on every write (durable across power loss).
    pub sync: bool,
    pub table: TableOptions,
    /// File system used for every file. Defaults to the real one.
    pub fs: Arc<dyn FileSystem>,
    /// What to do with damaged WAL files on open.
    pub wal_recovery_mode: WalRecoveryMode,
}

/// How `Db::open` treats WAL files that do not end cleanly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WalRecoveryMode {
    /// Default. A torn tail (what a crash during an append leaves) is
    /// dropped, but only in the newest WAL that has records. Corruption
    /// followed by more data, or a torn tail in an older WAL, is an error.
    /// Limitation: a bit flip inside the payload of the very last record
    /// looks the same as a torn write and is dropped.
    #[default]
    TolerateCorruptedTail,
    /// Any WAL that does not end cleanly is an error.
    AbsoluteConsistency,
    /// Replay up to the first damaged record and ignore everything after
    /// it, including later WAL files. Opt-in data loss for recovering a
    /// damaged database.
    PointInTime,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            write_buffer_size: 4 << 20,
            target_file_size: 2 << 20,
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            max_bytes_level1: 10 << 20,
            level_multiplier: 10,
            block_cache_size: 8 << 20,
            sync: false,
            table: TableOptions::default(),
            fs: Arc::new(StdFs),
            wal_recovery_mode: WalRecoveryMode::default(),
        }
    }
}

impl Options {
    pub fn max_bytes_for_level(&self, level: usize) -> u64 {
        let mut b = self.max_bytes_level1;
        for _ in 1..level {
            b *= self.level_multiplier;
        }
        b
    }
}
