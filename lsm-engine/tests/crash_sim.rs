//! Crash-point sweep. A fixed workload (puts, deletes, batches, flushes,
//! compactions) runs on `SimFs`. For every mutating file-system operation
//! N (create, append, fsync, rename, remove, directory fsync) we rerun the
//! workload with a power loss just before op N, build the post-crash disk
//! under several durability modes, reopen, and compare against a model:
//!
//! * `sync = true`: the recovered state must equal the model after k
//!   writes, where every acknowledged write is included (k >= acked) and
//!   nothing beyond the write in flight appears (k <= attempted).
//! * `sync = false`: the recovered state must equal the model after some
//!   prefix of the writes that includes everything before the last
//!   completed `flush()`.
//!
//! Then the recovered database is written to and reopened again, and a
//! second crash is injected into recovery itself.
//!
//! `LSM_CRASH_STRIDE=n` tests every n-th crash point (default 1 = all).

use lsm_engine::fs::{CrashMode, OpKind, SimFs, UnsyncedData, UnsyncedDirOps, op_histogram};
use lsm_engine::{Db, Options, ReadOptions, WriteBatch};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Debug)]
enum Step {
    Put(u32, u32),
    Delete(u32),
    Batch(Vec<(u32, Option<u32>)>),
    Flush,
    CompactAll,
}

fn key(k: u32) -> Vec<u8> {
    format!("key{k:05}").into_bytes()
}

fn val(k: u32, v: u32) -> Vec<u8> {
    let mut s = format!("v{k}-{v}-").into_bytes();
    s.resize(40 + (v as usize % 60), b'x');
    s
}

fn workload() -> Vec<Step> {
    let mut steps = Vec::new();
    let mut r = 0x2545F4914F6CDD1Du64;
    let mut rnd = |n: u64| {
        r ^= r << 13;
        r ^= r >> 7;
        r ^= r << 17;
        (r % n) as u32
    };
    for i in 0..260u32 {
        let s = match rnd(20) {
            0..=10 => Step::Put(rnd(120), i),
            11..=14 => Step::Delete(rnd(120)),
            15..=17 => Step::Batch(
                (0..1 + rnd(5))
                    .map(|_| (rnd(120), (rnd(3) > 0).then_some(i)))
                    .collect(),
            ),
            18 => Step::Flush,
            _ => {
                if i % 3 == 0 {
                    Step::CompactAll
                } else {
                    Step::Put(rnd(120), i)
                }
            }
        };
        steps.push(s);
    }
    steps
}

fn opts(fs: &SimFs, sync: bool) -> Options {
    Options {
        fs: Arc::new(fs.clone()),
        sync,
        write_buffer_size: 3 << 10,
        target_file_size: 4 << 10,
        max_bytes_level1: 8 << 10,
        l0_compaction_trigger: 2,
        table: lsm_engine::TableOptions {
            block_size: 256,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Model states: states[k] = contents after the first k write steps.
fn model_states(steps: &[Step]) -> Vec<Model> {
    let mut m = Model::new();
    let mut out = vec![m.clone()];
    for s in steps {
        match s {
            Step::Put(k, v) => {
                m.insert(key(*k), val(*k, *v));
            }
            Step::Delete(k) => {
                m.remove(&key(*k));
            }
            Step::Batch(ops) => {
                for (k, v) in ops {
                    match v {
                        Some(v) => m.insert(key(*k), val(*k, *v)),
                        None => m.remove(&key(*k)),
                    };
                }
            }
            Step::Flush | Step::CompactAll => continue,
        }
        out.push(m.clone());
    }
    out
}

struct RunResult {
    acked: usize,
    attempted: usize,
    /// Writes covered by the last flush() that returned Ok.
    flushed: usize,
}

fn run(db: &Db, steps: &[Step]) -> RunResult {
    let mut res = RunResult {
        acked: 0,
        attempted: 0,
        flushed: 0,
    };
    for s in steps {
        let r = match s {
            Step::Put(k, v) => {
                res.attempted += 1;
                db.put(&key(*k), &val(*k, *v))
            }
            Step::Delete(k) => {
                res.attempted += 1;
                db.delete(&key(*k))
            }
            Step::Batch(ops) => {
                res.attempted += 1;
                let mut b = WriteBatch::new();
                for (k, v) in ops {
                    match v {
                        Some(v) => b.put(&key(*k), &val(*k, *v)),
                        None => b.delete(&key(*k)),
                    };
                }
                db.write(b)
            }
            Step::Flush => db.flush().map(|_| res.flushed = res.acked),
            Step::CompactAll => db.compact_all().map(|_| res.flushed = res.acked),
        };
        match r {
            Ok(()) => {
                if !matches!(s, Step::Flush | Step::CompactAll) {
                    res.acked += 1;
                }
            }
            Err(_) => break,
        }
    }
    res
}

fn contents(db: &Db) -> Result<Model, String> {
    let mut m = Model::new();
    for item in db.iter(ReadOptions::default()) {
        let (k, v) = item.map_err(|e| e.to_string())?;
        m.insert(k, v);
    }
    // Point reads must agree with the scan.
    for (k, v) in &m {
        let got = db.get(k).map_err(|e| e.to_string())?;
        if got.as_ref() != Some(v) {
            return Err(format!("get({k:?}) disagrees with scan"));
        }
    }
    Ok(m)
}

const MODES: [(UnsyncedData, UnsyncedDirOps); 5] = [
    (UnsyncedData::Drop, UnsyncedDirOps::Drop),
    (UnsyncedData::TornPrefix, UnsyncedDirOps::Drop),
    (UnsyncedData::ZeroTail, UnsyncedDirOps::Keep),
    (UnsyncedData::KeepAll, UnsyncedDirOps::Keep),
    (UnsyncedData::TornPrefix, UnsyncedDirOps::Keep),
];

fn sweep(sync: bool) {
    let dir = Path::new("/db");
    let steps = workload();
    let states = model_states(&steps);
    let stride: u64 = std::env::var("LSM_CRASH_STRIDE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    // Reference run without a crash, to learn how many ops there are.
    let fs = SimFs::new();
    let total = {
        let db = Db::open(dir, opts(&fs, sync)).unwrap();
        let r = run(&db, &steps);
        assert_eq!(r.acked, states.len() - 1);
        assert_eq!(contents(&db).unwrap(), states[r.acked]);
        let st = db.stats();
        let compactions = st.compactions.load(std::sync::atomic::Ordering::Relaxed)
            + st.trivial_moves.load(std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "sync={sync}: reference run: {} flushes, {compactions} compactions/moves, levels {}",
            st.flushes.load(std::sync::atomic::Ordering::Relaxed),
            db.level_summary()
        );
        assert!(compactions > 0, "workload must exercise compaction");
        drop(db);
        fs.op_count()
    };
    let hist = op_histogram(&fs.op_log());
    eprintln!("sync={sync}: reference run has {total} fs ops: {hist:?}");

    let mut checked = 0usize;
    let mut crashed_kinds: BTreeMap<String, usize> = BTreeMap::new();
    let mut n = 1;
    // Background work makes the op count vary a little between runs, so go
    // a bit past the reference total.
    while n <= total + 20 {
        let fs = SimFs::new();
        fs.crash_before_op(n);
        // None: crashed during the very first open.
        let db = Db::open(dir, opts(&fs, sync)).ok();
        let res = match &db {
            Some(db) => run(db, &steps),
            None => RunResult {
                acked: 0,
                attempted: 0,
                flushed: 0,
            },
        };
        if fs.has_crashed() {
            let log = fs.op_log();
            *crashed_kinds
                .entry(format!(
                    "{:?}",
                    log.last().copied().unwrap_or(OpKind::Create)
                ))
                .or_default() += 1;
        }
        for (mi, (data, dir_ops)) in MODES.iter().enumerate() {
            let img = fs.crash_image(CrashMode {
                data: *data,
                dir_ops: *dir_ops,
                seed: n * 31 + mi as u64,
            });
            let ctx = format!(
                "sync={sync} crash before op {n} mode {data:?}/{dir_ops:?} acked={} attempted={} flushed={}",
                res.acked, res.attempted, res.flushed
            );
            let db2 = Db::open(dir, opts(&img, sync))
                .unwrap_or_else(|e| panic!("{ctx}: reopen failed: {e}"));
            let got = contents(&db2).unwrap_or_else(|e| panic!("{ctx}: read failed: {e}"));
            let (lo, hi) = if sync {
                (res.acked, res.attempted)
            } else {
                (res.flushed, res.attempted)
            };
            let k = (lo..=hi.min(states.len() - 1)).find(|&k| states[k] == got);
            assert!(
                k.is_some(),
                "{ctx}: recovered state ({} keys) matches no model state in [{lo}, {hi}]",
                got.len()
            );
            // The recovered database must keep working and survive another
            // reopen, and a crash during its own recovery.
            db2.put(b"zz-after", b"1").unwrap();
            drop(db2);
            let fs3 = img.crash_image(CrashMode {
                data: UnsyncedData::KeepAll,
                dir_ops: UnsyncedDirOps::Keep,
                seed: 1,
            });
            let recovery_ops = {
                let probe = fs3.crash_image(CrashMode {
                    data: UnsyncedData::KeepAll,
                    dir_ops: UnsyncedDirOps::Keep,
                    seed: 1,
                });
                drop(Db::open(dir, opts(&probe, sync)).unwrap());
                probe.op_count()
            };
            let crash_in_recovery = 1 + (n * 7 + mi as u64) % recovery_ops.max(1);
            fs3.crash_before_op(crash_in_recovery);
            let _ = Db::open(dir, opts(&fs3, sync)).map(drop);
            let fs4 = fs3.crash_image(CrashMode {
                data: *data,
                dir_ops: *dir_ops,
                seed: n,
            });
            let db4 = Db::open(dir, opts(&fs4, sync)).unwrap_or_else(|e| {
                panic!("{ctx}: reopen after crash in recovery (op {crash_in_recovery}) failed: {e}")
            });
            let mut got4 = contents(&db4).unwrap();
            if sync {
                assert_eq!(got4.remove(&b"zz-after"[..]), Some(b"1".to_vec()), "{ctx}");
            } else {
                got4.remove(&b"zz-after"[..]);
            }
            assert_eq!(got4, got, "{ctx}: state changed across crash in recovery");
            checked += 1;
        }
        drop(db);
        n += stride;
    }
    eprintln!(
        "sync={sync}: verified {checked} crash images; crash points by preceding op kind: {crashed_kinds:?}"
    );
}

#[test]
fn crash_at_every_fs_op_sync() {
    sweep(true);
}

#[test]
fn crash_at_every_fs_op_nosync() {
    sweep(false);
}
