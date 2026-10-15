//! Same-machine comparison of lsm-engine with RocksDB (via the `rocksdb`
//! crate, bundled RocksDB 10.4.2), sled 0.34 and an in-memory BTreeMap.
//!
//! Usage: bench-compare <engine> <dir> [num_keys]
//!   engine: lsm | rocksdb | sled | btreemap
//!
//! Workload (single thread, 16-byte keys, 100-byte values, no compression):
//!   fillrandom   N random-order puts (sync off)
//!   flush+compact (engines that support it), then:
//!   readrandom   N gets of existing keys
//!   readmissing  N gets of absent keys
//!   scan100      N/100 seeks each followed by 100 next()
//!   fillsync     2000 puts with fsync per write
//! Disk bytes written are taken from /proc/self/io (write_bytes), so write
//! amplification is measured the same way for every engine.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

trait Engine {
    fn put(&self, k: &[u8], v: &[u8]);
    fn get(&self, k: &[u8]) -> Option<Vec<u8>>;
    /// Seeks to k and reads up to n entries; returns how many it read.
    fn scan(&self, k: &[u8], n: usize) -> usize;
    fn compact(&self) {}
}

struct Lsm(lsm_engine::Db);
impl Engine for Lsm {
    fn put(&self, k: &[u8], v: &[u8]) {
        self.0.put(k, v).unwrap()
    }
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.0.get(k).unwrap()
    }
    fn scan(&self, k: &[u8], n: usize) -> usize {
        self.0
            .scan(Some(k), None, lsm_engine::ReadOptions::default())
            .take(n)
            .map(|r| r.unwrap())
            .count()
    }
    fn compact(&self) {
        self.0.compact_all().unwrap()
    }
}

struct Rocks(rocksdb::DB, bool);
impl Engine for Rocks {
    fn put(&self, k: &[u8], v: &[u8]) {
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(self.1);
        self.0.put_opt(k, v, &wo).unwrap()
    }
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.0.get(k).unwrap()
    }
    fn scan(&self, k: &[u8], n: usize) -> usize {
        let mut it = self.0.raw_iterator();
        it.seek(k);
        let mut c = 0;
        while it.valid() && c < n {
            c += 1;
            it.next();
        }
        it.status().unwrap();
        c
    }
    fn compact(&self) {
        self.0.flush().unwrap();
        self.0.compact_range::<&[u8], &[u8]>(None, None);
    }
}

struct Sled(sled::Db, bool);
impl Engine for Sled {
    fn put(&self, k: &[u8], v: &[u8]) {
        self.0.insert(k, v).unwrap();
        if self.1 {
            self.0.flush().unwrap();
        }
    }
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.0.get(k).unwrap().map(|v| v.to_vec())
    }
    fn scan(&self, k: &[u8], n: usize) -> usize {
        self.0.range(k..).take(n).map(|r| r.unwrap()).count()
    }
    fn compact(&self) {
        self.0.flush().unwrap();
    }
}

struct Map(Mutex<BTreeMap<Vec<u8>, Vec<u8>>>);
impl Engine for Map {
    fn put(&self, k: &[u8], v: &[u8]) {
        self.0.lock().unwrap().insert(k.to_vec(), v.to_vec());
    }
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.0.lock().unwrap().get(k).cloned()
    }
    fn scan(&self, k: &[u8], n: usize) -> usize {
        self.0.lock().unwrap().range(k.to_vec()..).take(n).count()
    }
}

fn open(engine: &str, dir: &Path, sync: bool) -> Box<dyn Engine> {
    match engine {
        "lsm" => Box::new(Lsm(lsm_engine::Db::open(
            dir,
            lsm_engine::Options {
                sync,
                ..Default::default()
            },
        )
        .unwrap())),
        "rocksdb" => {
            let mut o = rocksdb::Options::default();
            o.create_if_missing(true);
            o.set_compression_type(rocksdb::DBCompressionType::None);
            o.set_write_buffer_size(4 << 20); // same as lsm-engine
            o.set_target_file_size_base(2 << 20);
            o.set_max_bytes_for_level_base(10 << 20);
            let mut bo = rocksdb::BlockBasedOptions::default();
            bo.set_bloom_filter(10.0, false);
            bo.set_block_cache(&rocksdb::Cache::new_lru_cache(8 << 20));
            o.set_block_based_table_factory(&bo);
            Box::new(Rocks(rocksdb::DB::open(&o, dir).unwrap(), sync))
        }
        "sled" => Box::new(Sled(sled::open(dir).unwrap(), sync)),
        "btreemap" => Box::new(Map(Mutex::new(BTreeMap::new()))),
        _ => panic!("unknown engine {engine}"),
    }
}

fn disk_write_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/io").unwrap_or_default();
    s.lines()
        .find_map(|l| l.strip_prefix("write_bytes: "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

fn dir_size(p: &Path) -> u64 {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let m = e.metadata().unwrap();
            n += if m.is_dir() { dir_size(&e.path()) } else { m.len() };
        }
    }
    n
}

fn rng(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let engine = args.get(1).expect("engine").clone();
    let dir = Path::new(args.get(2).expect("dir")).to_path_buf();
    let n: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(500_000);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = |i: u64| format!("{i:016}");
    let value = vec![b'v'; 100];
    let report = |name: &str, ops: u64, t: Instant| {
        let s = t.elapsed().as_secs_f64();
        println!("{engine:<9} {name:<12} {:>10.0} ops/s", ops as f64 / s);
    };

    let db = open(&engine, &dir.join("main"), false);
    let w0 = disk_write_bytes();
    let t = Instant::now();
    let mut r = 0x9E3779B97F4A7C15u64;
    for _ in 0..n {
        db.put(key(rng(&mut r) % n).as_bytes(), &value);
    }
    report("fillrandom", n, t);
    let t = Instant::now();
    db.compact();
    println!("{engine:<9} {:<12} {:>10.2} s", "compact", t.elapsed().as_secs_f64());
    let written = disk_write_bytes() - w0;
    let user = n * (16 + 100);
    println!(
        "{engine:<9} {:<12} {:>10.2} x  ({} MB written to disk for {} MB of puts)",
        "write-amp",
        written as f64 / user as f64,
        written >> 20,
        user >> 20
    );
    println!(
        "{engine:<9} {:<12} {:>10} MB on disk after compaction",
        "space",
        dir_size(&dir.join("main")) >> 20
    );

    let t = Instant::now();
    let mut found = 0;
    let mut r = 0xD1B54A32D192ED03u64;
    for _ in 0..n {
        found += db.get(key(rng(&mut r) % n).as_bytes()).is_some() as u64;
    }
    report("readrandom", n, t);
    assert!(found > n / 2, "{found}");
    let t = Instant::now();
    for i in 0..n {
        assert!(db.get(format!("{i:016}x").as_bytes()).is_none());
    }
    report("readmissing", n, t);
    let t = Instant::now();
    let scans = n / 100;
    let mut r = 0x1234567u64;
    for _ in 0..scans {
        db.scan(key(rng(&mut r) % n).as_bytes(), 100);
    }
    report("scan100", scans, t);
    drop(db);

    if engine != "btreemap" {
        let db = open(&engine, &dir.join("sync"), true);
        let t = Instant::now();
        for i in 0..2000 {
            db.put(key(i).as_bytes(), &value);
        }
        report("fillsync", 2000, t);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
