use lsm_engine::{Db, Options, ReadOptions, WriteBatch};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Tiny buffers so a few thousand keys exercise flushes and multi-level
/// compaction.
fn small_opts() -> Options {
    Options {
        write_buffer_size: 16 << 10,
        target_file_size: 16 << 10,
        max_bytes_level1: 64 << 10,
        l0_compaction_trigger: 2,
        table: lsm_engine::TableOptions {
            block_size: 512,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn k(i: usize) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

#[test]
fn basic_ops() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"a", b"3").unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"3"[..]));
    db.delete(b"a").unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    // Empty key and value are allowed.
    db.put(b"", b"").unwrap();
    assert_eq!(db.get(b"").unwrap().as_deref(), Some(&b""[..]));
}

#[test]
fn recovers_from_wal_without_flush() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..1000 {
            db.put(&k(i), format!("v{i}").as_bytes()).unwrap();
        }
        db.delete(&k(5)).unwrap();
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(&k(999)).unwrap().as_deref(), Some(&b"v999"[..]));
    assert_eq!(db.get(&k(5)).unwrap(), None);
    // Sequence numbers must continue past recovered ones.
    db.put(&k(5), b"again").unwrap();
    drop(db);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(&k(5)).unwrap().as_deref(), Some(&b"again"[..]));
}

#[test]
fn many_keys_across_levels_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let n = 20_000;
    {
        let db = Db::open(dir.path(), small_opts()).unwrap();
        for i in 0..n {
            db.put(&k(i), format!("value-{i}").as_bytes()).unwrap();
        }
        for i in (0..n).step_by(3) {
            db.delete(&k(i)).unwrap();
        }
        for i in (0..n).step_by(5) {
            db.put(&k(i), b"overwritten").unwrap();
        }
        db.compact_all().unwrap();
        let summary = db.level_summary();
        assert!(
            summary.split(',').filter(|c| *c != "0").count() >= 1,
            "{summary}"
        );
    }
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..n {
        let want: Option<Vec<u8>> = if i % 5 == 0 {
            Some(b"overwritten".to_vec())
        } else if i % 3 == 0 {
            None
        } else {
            Some(format!("value-{i}").into_bytes())
        };
        assert_eq!(db.get(&k(i)).unwrap(), want, "key {i}");
    }
    let count = db.iter(ReadOptions::default()).count();
    let expected = (0..n).filter(|i| i % 5 == 0 || i % 3 != 0).count();
    assert_eq!(count, expected);
}

#[test]
fn scan_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..5000 {
        db.put(&k(i), b"x").unwrap();
    }
    db.flush().unwrap();
    for i in 0..5000 {
        if i % 2 == 0 {
            db.delete(&k(i)).unwrap();
        }
    }
    let got: Vec<_> = db
        .scan(Some(&k(100)), Some(&k(110)), ReadOptions::default())
        .map(|(k, _)| k)
        .collect();
    let want: Vec<_> = (100..110).filter(|i| i % 2 == 1).map(k).collect();
    assert_eq!(got, want);
    assert_eq!(
        db.scan(Some(&k(4990)), None, ReadOptions::default())
            .count(),
        5
    );
    assert_eq!(
        db.scan(None, Some(&k(10)), ReadOptions::default()).count(),
        5
    );
}

#[test]
fn snapshots_survive_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..3000 {
        db.put(&k(i), b"v1").unwrap();
    }
    let snap = db.snapshot();
    for i in 0..3000 {
        if i % 2 == 0 {
            db.put(&k(i), b"v2").unwrap();
        } else {
            db.delete(&k(i)).unwrap();
        }
    }
    db.compact_all().unwrap();
    let ro = ReadOptions {
        snapshot: Some(&snap),
    };
    for i in (0..3000).step_by(7) {
        assert_eq!(
            db.get_opt(&k(i), ro).unwrap().as_deref(),
            Some(&b"v1"[..]),
            "snapshot key {i}"
        );
    }
    assert_eq!(db.iter(ro).count(), 3000);
    assert_eq!(db.iter(ReadOptions::default()).count(), 1500);
    drop(snap);
    db.compact_all().unwrap();
    assert_eq!(db.get(&k(1)).unwrap(), None);
    assert_eq!(db.get(&k(2)).unwrap().as_deref(), Some(&b"v2"[..]));
}

#[test]
fn tombstones_reclaim_space() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small_opts()).unwrap();
    for i in 0..10_000 {
        db.put(&k(i), &[7u8; 100]).unwrap();
    }
    db.compact_all().unwrap();
    let before: u64 = db.level_bytes().iter().sum();
    for i in 0..10_000 {
        db.delete(&k(i)).unwrap();
    }
    db.compact_all().unwrap();
    let after: u64 = db.level_bytes().iter().sum();
    assert!(
        after < before / 20,
        "space not reclaimed: {before} -> {after}"
    );
    assert_eq!(db.iter(ReadOptions::default()).count(), 0);
}

#[test]
fn write_batch_is_atomic_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let mut b = WriteBatch::new();
    b.put(b"x", b"1")
        .put(b"x", b"2")
        .delete(b"y")
        .put(b"z", b"3");
    db.put(b"y", b"old").unwrap();
    db.write(b).unwrap();
    assert_eq!(
        db.get(b"x").unwrap().as_deref(),
        Some(&b"2"[..]),
        "later op in batch wins"
    );
    assert_eq!(db.get(b"y").unwrap(), None);
    assert_eq!(db.get(b"z").unwrap().as_deref(), Some(&b"3"[..]));
}

#[test]
fn concurrent_readers_and_writers() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), small_opts()).unwrap());
    std::thread::scope(|s| {
        for t in 0..4 {
            let db = db.clone();
            s.spawn(move || {
                for i in 0..3000 {
                    let key = format!("t{t}-{i:06}");
                    db.put(key.as_bytes(), key.as_bytes()).unwrap();
                    // Read-your-writes from the same thread.
                    assert_eq!(
                        db.get(key.as_bytes()).unwrap().as_deref(),
                        Some(key.as_bytes())
                    );
                }
            });
        }
        for _ in 0..2 {
            let db = db.clone();
            s.spawn(move || {
                for _ in 0..50 {
                    // Iterators must always see sorted, unique keys.
                    let keys: Vec<_> = db.iter(ReadOptions::default()).map(|(k, _)| k).collect();
                    assert!(keys.windows(2).all(|w| w[0] < w[1]));
                }
            });
        }
    });
    assert_eq!(db.iter(ReadOptions::default()).count(), 12_000);
    assert!(
        db.stats()
            .write_groups
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
}

#[test]
fn detects_corrupted_table_on_read() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..2000 {
            db.put(&k(i), &[1u8; 50]).unwrap();
        }
        db.flush().unwrap();
    }
    let sst = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .find(|e| e.path().extension().is_some_and(|x| x == "sst"))
        .unwrap()
        .path();
    let mut data = std::fs::read(&sst).unwrap();
    data[100] ^= 0xff;
    std::fs::write(&sst, &data).unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let errors = (0..2000).filter(|&i| db.get(&k(i)).is_err()).count();
    assert!(errors > 0, "corruption went unnoticed");
}

// ---------------------------------------------------------------- model test

#[derive(Debug, Clone)]
enum Op {
    Put(u16, u8),
    Delete(u16),
    Get(u16),
    Scan(u16, u16),
    Flush,
    Compact,
    Reopen,
}

fn op_strategy() -> impl proptest::strategy::Strategy<Value = Op> {
    use proptest::prelude::*;
    prop_oneof![
        30 => (0u16..500, any::<u8>()).prop_map(|(k, v)| Op::Put(k, v)),
        10 => (0u16..500).prop_map(Op::Delete),
        20 => (0u16..500).prop_map(Op::Get),
        5 => (0u16..500, 0u16..500).prop_map(|(a, b)| Op::Scan(a.min(b), a.max(b))),
        2 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => Just(Op::Reopen),
    ]
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(48))]
    /// Random operation sequences must behave exactly like a BTreeMap,
    /// including across flushes, compactions and restarts.
    #[test]
    fn behaves_like_btreemap(ops in proptest::collection::vec(op_strategy(), 1..400)) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path(), small_opts()).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let key = |k: u16| format!("k{k:05}").into_bytes();
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    let val = vec![v; (v as usize % 40) + 1];
                    db.put(&key(k), &val).unwrap();
                    model.insert(key(k), val);
                }
                Op::Delete(k) => {
                    db.delete(&key(k)).unwrap();
                    model.remove(&key(k));
                }
                Op::Get(k) => {
                    proptest::prop_assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned());
                }
                Op::Scan(a, b) => {
                    let got: Vec<_> = db.scan(Some(&key(a)), Some(&key(b)), ReadOptions::default()).collect();
                    let want: Vec<_> = model.range(key(a)..key(b)).map(|(k, v)| (k.clone(), v.clone())).collect();
                    proptest::prop_assert_eq!(got, want);
                }
                Op::Flush => db.flush().unwrap(),
                Op::Compact => db.compact_all().unwrap(),
                Op::Reopen => {
                    drop(db);
                    db = Db::open(dir.path(), small_opts()).unwrap();
                }
            }
        }
        let all: Vec<_> = db.iter(ReadOptions::default()).collect();
        let want: Vec<_> = model.into_iter().collect();
        proptest::prop_assert_eq!(all, want);
    }
}
