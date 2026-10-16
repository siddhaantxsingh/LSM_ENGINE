//! Model-based property test: random operation sequences, including
//! snapshots (MVCC reads), batches, range scans, flushes, compactions and
//! reopens, must behave exactly like a BTreeMap model. Each snapshot is
//! checked against a frozen copy of the model taken when it was created.

use lsm_engine::{Db, Options, ReadOptions, Snapshot, WriteBatch};
use proptest::prelude::*;
use std::collections::BTreeMap;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Debug, Clone)]
enum Op {
    Put(u16, u8),
    Delete(u16),
    Batch(Vec<(u16, Option<u8>)>),
    Get(u16),
    Scan(u16, u16),
    TakeSnapshot,
    ReleaseSnapshot(usize),
    SnapGet(usize, u16),
    SnapScan(usize, u16, u16),
    Flush,
    Compact,
    Reopen,
}

const KEYS: u16 = 300;

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        30 => (0..KEYS, any::<u8>()).prop_map(|(k, v)| Op::Put(k, v)),
        10 => (0..KEYS).prop_map(Op::Delete),
        6 => prop::collection::vec((0..KEYS, prop::option::of(any::<u8>())), 1..8).prop_map(Op::Batch),
        15 => (0..KEYS).prop_map(Op::Get),
        5 => (0..KEYS, 0..KEYS).prop_map(|(a, b)| Op::Scan(a.min(b), a.max(b))),
        4 => Just(Op::TakeSnapshot),
        2 => any::<usize>().prop_map(Op::ReleaseSnapshot),
        8 => (any::<usize>(), 0..KEYS).prop_map(|(s, k)| Op::SnapGet(s, k)),
        3 => (any::<usize>(), 0..KEYS, 0..KEYS).prop_map(|(s, a, b)| Op::SnapScan(s, a.min(b), a.max(b))),
        3 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => Just(Op::Reopen),
    ]
}

fn opts() -> Options {
    Options {
        write_buffer_size: 4 << 10,
        target_file_size: 4 << 10,
        max_bytes_level1: 16 << 10,
        l0_compaction_trigger: 2,
        table: lsm_engine::TableOptions {
            block_size: 256,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn key(k: u16) -> Vec<u8> {
    format!("k{k:05}").into_bytes()
}

fn value(v: u8) -> Vec<u8> {
    vec![v; (v as usize % 50) + 1]
}

fn scan(db: &Db, a: u16, b: u16, ro: ReadOptions) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.scan(Some(&key(a)), Some(&key(b)), ro)
        .map(|r| r.unwrap())
        .collect()
}

fn range(m: &Model, a: u16, b: u16) -> Vec<(Vec<u8>, Vec<u8>)> {
    m.range(key(a)..key(b))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("LSM_MODEL_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(64)
    ))]
    #[test]
    fn engine_matches_btreemap_model_with_snapshots(ops in prop::collection::vec(op(), 1..300)) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path(), opts()).unwrap();
        let mut model = Model::new();
        let mut snaps: Vec<(Snapshot, Model)> = Vec::new();
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    db.put(&key(k), &value(v)).unwrap();
                    model.insert(key(k), value(v));
                }
                Op::Delete(k) => {
                    db.delete(&key(k)).unwrap();
                    model.remove(&key(k));
                }
                Op::Batch(ops) => {
                    let mut b = WriteBatch::new();
                    for (k, v) in ops {
                        match v {
                            Some(v) => {
                                b.put(&key(k), &value(v));
                                model.insert(key(k), value(v));
                            }
                            None => {
                                b.delete(&key(k));
                                model.remove(&key(k));
                            }
                        }
                    }
                    db.write(b).unwrap();
                }
                Op::Get(k) => {
                    prop_assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned());
                }
                Op::Scan(a, b) => {
                    prop_assert_eq!(scan(&db, a, b, ReadOptions::default()), range(&model, a, b));
                }
                Op::TakeSnapshot => snaps.push((db.snapshot(), model.clone())),
                Op::ReleaseSnapshot(i) => {
                    if !snaps.is_empty() {
                        let i = i % snaps.len();
                        snaps.remove(i);
                    }
                }
                Op::SnapGet(i, k) => {
                    if !snaps.is_empty() {
                        let (s, m) = &snaps[i % snaps.len()];
                        let ro = ReadOptions { snapshot: Some(s) };
                        prop_assert_eq!(db.get_opt(&key(k), ro).unwrap(), m.get(&key(k)).cloned());
                    }
                }
                Op::SnapScan(i, a, b) => {
                    if !snaps.is_empty() {
                        let (s, m) = &snaps[i % snaps.len()];
                        let ro = ReadOptions { snapshot: Some(s) };
                        prop_assert_eq!(scan(&db, a, b, ro), range(m, a, b));
                    }
                }
                Op::Flush => db.flush().unwrap(),
                Op::Compact => db.compact_all().unwrap(),
                Op::Reopen => {
                    // Snapshots do not survive a restart.
                    snaps.clear();
                    drop(db);
                    db = Db::open(dir.path(), opts()).unwrap();
                }
            }
        }
        // Every live snapshot still sees exactly its point in time after
        // all the flushes and compactions.
        for (s, m) in &snaps {
            let ro = ReadOptions { snapshot: Some(s) };
            let all: Model = db.iter(ro).map(|r| r.unwrap()).collect();
            prop_assert_eq!(&all, m);
        }
        let all: Model = db.iter(ReadOptions::default()).map(|r| r.unwrap()).collect();
        prop_assert_eq!(all, model);
    }
}
