//! Corruption sweep: flip one bit at a time in every kind of file (SST,
//! MANIFEST, CURRENT, WAL), reopen and read everything back. The only
//! acceptable outcomes are an error (at open or on that read) or the
//! correct data. A wrong value, a missing key reported as "not found", an
//! extra key, or a scan that ends early without an error fails the test.
//!
//! One documented exception: a flip inside the payload of the very last
//! WAL record looks exactly like a torn write and that record is dropped
//! in the default `TolerateCorruptedTail` mode. The test checks that the
//! result is then exactly the state before that write, and that
//! `AbsoluteConsistency` mode reports an error instead.

use lsm_engine::fs::{CrashMode, FileSystem, SimFs, UnsyncedData, UnsyncedDirOps};
use lsm_engine::{Db, Options, ReadOptions, WalRecoveryMode};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const LAST_KEY: &[u8] = b"zzlast";
const LAST_VAL: &[u8] = b"0123456789";
/// WAL record for a one-put batch: 12-byte record header, 12-byte batch
/// header, kind, varint+key, varint+value, 1 trailer byte.
const LAST_RECORD_LEN: usize = 12 + 12 + 1 + 1 + 6 + 1 + 10 + 1;

fn opts(fs: &SimFs, mode: WalRecoveryMode) -> Options {
    Options {
        fs: Arc::new(fs.clone()),
        write_buffer_size: 8 << 10,
        target_file_size: 8 << 10,
        max_bytes_level1: 32 << 10,
        l0_compaction_trigger: 4,
        wal_recovery_mode: mode,
        table: lsm_engine::TableOptions {
            block_size: 512,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn clone_fs(fs: &SimFs) -> SimFs {
    fs.crash_image(CrashMode {
        data: UnsyncedData::KeepAll,
        dir_ops: UnsyncedDirOps::Keep,
        seed: 1,
    })
}

/// Builds a database with tables in several levels, an L0 table, a WAL
/// with unflushed writes, a MANIFEST and CURRENT. Returns the file system
/// and the model before and after the final write.
fn build() -> (SimFs, Model, Model) {
    let fs = SimFs::new();
    let dir = Path::new("/db");
    let mut model = Model::new();
    let db = Db::open(dir, opts(&fs, WalRecoveryMode::default())).unwrap();
    let k = |i: u32| format!("key{i:06}").into_bytes();
    for i in 0..3000 {
        let v = format!("value-{i}-{}", "x".repeat((i % 30) as usize)).into_bytes();
        db.put(&k(i), &v).unwrap();
        model.insert(k(i), v);
    }
    for i in (0..3000).step_by(3) {
        db.delete(&k(i)).unwrap();
        model.remove(&k(i));
    }
    db.compact_all().unwrap();
    for i in (0..3000).step_by(5) {
        db.put(&k(i), b"second").unwrap();
        model.insert(k(i), b"second".to_vec());
    }
    db.flush().unwrap();
    for i in (1..3000).step_by(7) {
        db.put(&k(i), b"in-wal").unwrap();
        model.insert(k(i), b"in-wal".to_vec());
    }
    let before_last = model.clone();
    db.put(LAST_KEY, LAST_VAL).unwrap();
    model.insert(LAST_KEY.to_vec(), LAST_VAL.to_vec());
    eprintln!("built db: levels {}", db.level_summary());
    drop(db);
    (clone_fs(&fs), before_last, model)
}

/// Ok(()) if every read was either correct for `model` or an error.
fn check(db: &Db, model: &Model) -> Result<usize, String> {
    let mut errors = 0;
    for (k, v) in model {
        match db.get(k) {
            Ok(Some(got)) if &got == v => {}
            Ok(got) => return Err(format!("get({k:?}) = {got:?}, want {v:?}")),
            Err(_) => errors += 1,
        }
    }
    for absent in [&b"key000000"[..], b"key000003", b"nope", b"key999999"] {
        if !model.contains_key(absent)
            && let Ok(Some(v)) = db.get(absent)
        {
            return Err(format!("deleted/absent key {absent:?} returned {v:?}"));
        }
    }
    let mut want = model.iter();
    for item in db.iter(ReadOptions::default()) {
        match item {
            Ok((k, v)) => match want.next() {
                Some((wk, wv)) if *wk == k && *wv == v => {}
                other => return Err(format!("scan yielded {k:?}, want {other:?}")),
            },
            Err(_) => return Ok(errors + 1),
        }
    }
    if let Some((k, _)) = want.next() {
        return Err(format!("scan ended without error before {k:?}"));
    }
    Ok(errors)
}

#[derive(Default, Debug)]
struct Tally {
    flips: usize,
    open_error: usize,
    read_error: usize,
    no_effect: usize,
    torn_last_wal_record: usize,
}

fn sweep_file(base: &SimFs, path: &Path, before_last: &Model, model: &Model, tally: &mut Tally) {
    let data = base.read(path).unwrap();
    let n = data.len();
    // Every byte of small files and of the last 128 bytes (footers, last
    // records); a stride through the rest of large files.
    let stride = (n / 400).max(1);
    let offsets: Vec<usize> = (0..n)
        .filter(|&o| n <= 512 || o % stride == 0 || o + 128 >= n)
        .collect();
    let is_wal = path.extension().is_some_and(|e| e == "log");
    for &off in &offsets {
        let bit = (off * 7 + 3) % 8;
        let fs = clone_fs(base);
        let mut d = data.clone();
        d[off] ^= 1 << bit;
        fs.set_file_contents(path, d);
        tally.flips += 1;
        let ctx = format!("{} byte {off} bit {bit}", path.display());
        let in_last_wal_record = is_wal && off >= n - LAST_RECORD_LEN;
        match Db::open("/db", opts(&fs, WalRecoveryMode::default())) {
            Err(_) => tally.open_error += 1,
            Ok(db) => {
                let r = check(&db, model);
                match r {
                    Ok(0) => tally.no_effect += 1,
                    Ok(_) => tally.read_error += 1,
                    Err(e) if in_last_wal_record => {
                        // Must be exactly "the last write was dropped".
                        let r2 = check(&db, before_last);
                        assert!(
                            matches!(r2, Ok(0)),
                            "{ctx}: neither full state ({e}) nor state before last write ({r2:?})"
                        );
                        assert_eq!(db.get(LAST_KEY).unwrap(), None, "{ctx}");
                        tally.torn_last_wal_record += 1;
                        // AbsoluteConsistency must refuse instead.
                        drop(db);
                        let fs2 = clone_fs(base);
                        let mut d = data.clone();
                        d[off] ^= 1 << bit;
                        fs2.set_file_contents(path, d);
                        assert!(
                            Db::open("/db", opts(&fs2, WalRecoveryMode::AbsoluteConsistency))
                                .is_err(),
                            "{ctx}: AbsoluteConsistency accepted a damaged WAL"
                        );
                    }
                    Err(e) => panic!("{ctx}: silent corruption: {e}"),
                }
            }
        }
    }
}

#[test]
fn bit_flips_in_every_file_type_are_detected() {
    let (base, before_last, model) = build();
    let dir = Path::new("/db");
    let mut names = base.list_dir(dir).unwrap();
    names.sort();
    eprintln!("files: {names:?}");
    let mut by_type: BTreeMap<&str, Tally> = BTreeMap::new();
    for name in &names {
        let kind = if name.ends_with(".sst") {
            "sst"
        } else if name.ends_with(".log") {
            "wal"
        } else if name.starts_with("MANIFEST") {
            "manifest"
        } else if name == "CURRENT" {
            "current"
        } else {
            continue;
        };
        let path: PathBuf = dir.join(name);
        let t = by_type.entry(kind).or_default();
        sweep_file(&base, &path, &before_last, &model, t);
    }
    for (k, t) in &by_type {
        eprintln!("{k:>8}: {t:?}");
    }
    for k in ["sst", "wal", "manifest", "current"] {
        assert!(
            by_type.get(k).is_some_and(|t| t.flips > 0),
            "no {k} files tested"
        );
    }
    // The model is non-trivial.
    assert!(model.len() > 1000);
}
