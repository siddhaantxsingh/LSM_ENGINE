//! Regression tests for defects found in the 2026-10-14 audit. Each test
//! was written before its fix and failed on the unfixed code (see
//! docs/evidence/2026-10-14/failing_first.log).

use lsm_engine::fs::{CrashMode, SimFs, UnsyncedData, UnsyncedDirOps};
use lsm_engine::{Db, Options, ReadOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn k(i: usize) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == ext))
        .collect();
    v.sort();
    v
}

fn manifest(dir: &Path) -> PathBuf {
    let cur = std::fs::read_to_string(dir.join("CURRENT")).unwrap();
    dir.join(cur.trim())
}

fn flip(path: &Path, offset: usize) {
    let mut d = std::fs::read(path).unwrap();
    d[offset] ^= 0x5a;
    std::fs::write(path, d).unwrap();
}

/// No background compaction: every flush stays an L0 file.
fn l0_only() -> Options {
    Options {
        l0_compaction_trigger: 1000,
        l0_slowdown_trigger: 1000,
        l0_stop_trigger: 1000,
        ..Default::default()
    }
}

/// D1 (critical): Db::flush() froze the memtable while a write leader was
/// still inserting into it outside the lock. The flushed table missed the
/// write, the WAL holding it was deleted, and the memtable was dropped:
/// an acknowledged write vanished even without a crash.
#[test]
fn d1_flush_does_not_lose_concurrent_writes() {
    // SimFs makes flushes cheap (no real fsync), so many flushes overlap
    // with writes.
    let fs = SimFs::new();
    let opts = Options {
        fs: Arc::new(fs),
        ..l0_only()
    };
    let db = Arc::new(Db::open("/db", opts).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let flusher = {
        let (db, stop) = (db.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                db.flush().unwrap();
            }
        })
    };
    // Large batches keep the leader inserting into the memtable (outside
    // the state lock) for a long time, which widens the race window.
    const BATCHES: usize = 40;
    const PER_BATCH: usize = 500;
    std::thread::scope(|s| {
        for t in 0..4usize {
            let db = db.clone();
            s.spawn(move || {
                for b in 0..BATCHES {
                    let mut wb = lsm_engine::WriteBatch::new();
                    for i in 0..PER_BATCH {
                        wb.put(&k(t * 1_000_000 + b * PER_BATCH + i), b"v");
                    }
                    db.write(wb).unwrap();
                }
            });
        }
    });
    stop.store(true, Ordering::Relaxed);
    flusher.join().unwrap();
    let mut missing = 0;
    for t in 0..4usize {
        for i in 0..BATCHES * PER_BATCH {
            if db.get(&k(t * 1_000_000 + i)).unwrap().is_none() {
                missing += 1;
            }
        }
    }
    eprintln!(
        "flushes={} groups={}",
        db.stats().flushes.load(Ordering::Relaxed),
        db.stats().write_groups.load(Ordering::Relaxed)
    );
    assert_eq!(missing, 0, "acknowledged writes lost by concurrent flush");
}

fn db_with_flushes(dir: &Path, flushes: usize) {
    let db = Db::open(dir, l0_only()).unwrap();
    for f in 0..flushes {
        for i in 0..200 {
            db.put(&k(f * 1000 + i), b"value").unwrap();
        }
        db.flush().unwrap();
    }
}

/// D2 (critical): a corrupted record in the middle of the MANIFEST ended
/// replay silently. The engine then opened with an older file set and
/// deleted the "unreferenced" (live) tables.
#[test]
fn d2_mid_manifest_corruption_refuses_open_and_keeps_tables() {
    let dir = tempfile::tempdir().unwrap();
    db_with_flushes(dir.path(), 4);
    let ssts = files_with_ext(dir.path(), "sst");
    assert_eq!(ssts.len(), 4);
    // Corrupt the first record (the full snapshot written at open).
    flip(&manifest(dir.path()), 20);
    assert!(
        Db::open(dir.path(), l0_only()).is_err(),
        "opened with a corrupted MANIFEST"
    );
    assert_eq!(
        files_with_ext(dir.path(), "sst"),
        ssts,
        "tables were deleted"
    );
}

/// D2b (critical): an empty MANIFEST was treated as an empty database and
/// every table was deleted.
#[test]
fn d2b_empty_manifest_refuses_open() {
    let dir = tempfile::tempdir().unwrap();
    db_with_flushes(dir.path(), 2);
    let ssts = files_with_ext(dir.path(), "sst");
    std::fs::write(manifest(dir.path()), b"").unwrap();
    assert!(Db::open(dir.path(), l0_only()).is_err());
    assert_eq!(files_with_ext(dir.path(), "sst"), ssts);
}

/// D3 (high): a corrupted record in the middle of the WAL ended replay
/// silently, dropping every later (synced, acknowledged) write.
#[test]
fn d3_mid_wal_corruption_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(
            dir.path(),
            Options {
                sync: true,
                ..l0_only()
            },
        )
        .unwrap();
        for i in 0..100 {
            db.put(&k(i), b"value").unwrap();
        }
    }
    let wal = files_with_ext(dir.path(), "log").pop().unwrap();
    flip(&wal, 40); // inside an early record
    match Db::open(dir.path(), l0_only()) {
        Err(_) => {}
        Ok(db) => {
            let present = (0..100)
                .filter(|&i| db.get(&k(i)).unwrap().is_some())
                .count();
            panic!("opened despite mid-WAL corruption; {present}/100 synced writes visible");
        }
    }
}

/// D4 (high): SST footer handles were trusted. A huge size overflowed
/// `size + 4` (panic) or asked for an enormous allocation.
#[test]
fn d4_bad_footer_size_is_corruption_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    db_with_flushes(dir.path(), 1);
    let sst = files_with_ext(dir.path(), "sst").pop().unwrap();
    let mut d = std::fs::read(&sst).unwrap();
    let n = d.len();
    // index_size field is bytes [24..32) of the 40-byte footer.
    d[n - 40 + 24..n - 40 + 32].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(&sst, d).unwrap();
    let r = std::panic::catch_unwind(|| match Db::open(dir.path(), l0_only()) {
        Err(e) => format!("open error: {e}"),
        Ok(db) => match db.get(&k(0)) {
            Err(e) => format!("get error: {e}"),
            Ok(v) => format!("get ok: {v:?}"),
        },
    });
    let msg = r.expect("panicked on corrupt footer");
    assert!(msg.contains("error"), "{msg}");
}

/// D5 (high): Db::iter() skipped L0 tables that failed to open, so a scan
/// silently returned fewer keys and status() said Ok.
#[test]
fn d5_iter_reports_unopenable_table() {
    let dir = tempfile::tempdir().unwrap();
    db_with_flushes(dir.path(), 2);
    let sst = files_with_ext(dir.path(), "sst").pop().unwrap();
    let n = std::fs::metadata(&sst).unwrap().len() as usize;
    flip(&sst, n - 1); // footer magic
    // Opening may refuse (also acceptable); if it opens, iteration must
    // not look complete.
    if let Ok(db) = Db::open(dir.path(), l0_only()) {
        let mut it = db.iter(ReadOptions::default());
        let seen = it.by_ref().count();
        assert!(
            it.status().is_err(),
            "scan silently returned {seen} of 400 keys"
        );
    }
}

fn sim_opts(fs: &SimFs) -> Options {
    Options {
        fs: Arc::new(fs.clone()),
        sync: true,
        write_buffer_size: 4 << 10,
        ..l0_only()
    }
}

/// D7 (high): new WAL and SST files were never made durable in their
/// directory (no directory fsync), so after a power loss the MANIFEST
/// could reference a missing table and synced writes in a new WAL could
/// vanish.
#[test]
fn d7_new_files_survive_power_loss() {
    let fs = SimFs::new();
    let dir = Path::new("/db");
    {
        let db = Db::open(dir, sim_opts(&fs)).unwrap();
        for i in 0..300 {
            db.put(&k(i), &[1u8; 100]).unwrap();
        }
        db.flush().unwrap();
        for i in 300..400 {
            db.put(&k(i), &[1u8; 100]).unwrap();
        }
        // Power loss now: no clean shutdown.
        let img = fs.crash_image(CrashMode {
            data: UnsyncedData::Drop,
            dir_ops: UnsyncedDirOps::Drop,
            seed: 1,
        });
        std::mem::forget(db);
        let db = Db::open(dir, sim_opts(&img)).expect("reopen after power loss");
        for i in 0..400 {
            assert!(
                db.get(&k(i)).expect("read after power loss").is_some(),
                "acknowledged synced write {i} lost"
            );
        }
    }
}

/// D9 (medium): a table listed in the MANIFEST but missing on disk was
/// only noticed when a read happened to touch it (and scans skipped it).
#[test]
fn d9_missing_table_refuses_open() {
    let dir = tempfile::tempdir().unwrap();
    db_with_flushes(dir.path(), 3);
    let sst = files_with_ext(dir.path(), "sst").remove(1);
    std::fs::remove_file(sst).unwrap();
    assert!(Db::open(dir.path(), l0_only()).is_err());
}

/// D10 (medium): a WAL whose tail was zero-filled by the file system after
/// a crash (size updated, data not written) was rejected: an all-zero
/// header parsed as a valid empty record and the batch decoder failed.
#[test]
fn d10_zero_filled_wal_tail_is_a_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(
            dir.path(),
            Options {
                sync: true,
                ..l0_only()
            },
        )
        .unwrap();
        for i in 0..50 {
            db.put(&k(i), b"v").unwrap();
        }
    }
    let wal = files_with_ext(dir.path(), "log").pop().unwrap();
    let mut d = std::fs::read(&wal).unwrap();
    d.extend_from_slice(&[0u8; 4096]);
    std::fs::write(&wal, d).unwrap();
    let db = Db::open(dir.path(), l0_only()).expect("zero tail must be tolerated");
    for i in 0..50 {
        assert!(db.get(&k(i)).unwrap().is_some());
    }
}

/// Follow-up to D2: a torn last MANIFEST record (crash while appending an
/// edit) is tolerated, and a table only that record referenced is kept
/// aside as `*.sst.lost` instead of deleted.
#[test]
fn torn_manifest_tail_keeps_orphan_tables_aside() {
    let dir = tempfile::tempdir().unwrap();
    db_with_flushes(dir.path(), 3);
    let m = manifest(dir.path());
    let len = std::fs::metadata(&m).unwrap().len();
    let f = std::fs::OpenOptions::new().write(true).open(&m).unwrap();
    f.set_len(len - 3).unwrap(); // tear the last edit (the 3rd flush)
    drop(f);
    // The 3rd flush's WAL was already deleted, so its keys are only in the
    // orphaned table: the open succeeds but must not delete that table.
    let db = Db::open(dir.path(), l0_only()).unwrap();
    assert_eq!(files_with_ext(dir.path(), "sst").len(), 2);
    assert_eq!(files_with_ext(dir.path(), "lost").len(), 1);
    assert!(db.get(&k(0)).unwrap().is_some());
}
