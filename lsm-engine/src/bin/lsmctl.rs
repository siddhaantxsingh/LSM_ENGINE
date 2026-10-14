//! lsmctl: CLI, benchmark and crash-test driver for lsm-engine.
//!
//!   lsmctl <dir> put <key> <value>
//!   lsmctl <dir> get <key>
//!   lsmctl <dir> delete <key>
//!   lsmctl <dir> scan [start] [end]
//!   lsmctl <dir> stats
//!   lsmctl <dir> bench [--num N] [--value-size B] [--threads T]
//!   lsmctl <dir> crash-writer          (writes forever with sync, prints acked keys)

use lsm_engine::{Db, Options, ReadOptions, WriteBatch};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

fn usage() -> ! {
    eprintln!("usage: lsmctl <dir> put|get|delete|scan|stats|bench|crash-writer ...");
    std::process::exit(2)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        usage();
    }
    let dir = &args[1];
    let cmd = args[2].as_str();
    let rest = &args[3..];
    match cmd {
        "bench" => return bench(dir, rest),
        "crash-writer" => return crash_writer(dir),
        _ => {}
    }
    let db = Db::open(dir, Options::default()).unwrap_or_else(|e| {
        eprintln!("open: {e}");
        std::process::exit(1)
    });
    match (cmd, rest) {
        ("put", [k, v]) => db.put(k.as_bytes(), v.as_bytes()).unwrap(),
        ("get", [k]) => match db.get(k.as_bytes()).unwrap() {
            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
            None => {
                eprintln!("(not found)");
                std::process::exit(1)
            }
        },
        ("delete", [k]) => db.delete(k.as_bytes()).unwrap(),
        ("scan", r) => {
            let start = r.first().map(|s| s.as_bytes());
            let end = r.get(1).map(|s| s.as_bytes());
            for item in db.scan(start, end, ReadOptions::default()) {
                let (k, v) = item.unwrap_or_else(|e| {
                    eprintln!("scan: {e}");
                    std::process::exit(1)
                });
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
        }
        ("stats", []) => {
            println!("files per level: {}", db.level_summary());
            println!("bytes per level: {:?}", db.level_bytes());
        }
        _ => usage(),
    }
}

/// Used by the crash test: writes synced batches forever and prints each
/// key only after write() returned (i.e. it is acknowledged & durable).
fn crash_writer(dir: &str) {
    let opts = Options {
        write_buffer_size: 64 << 10,
        target_file_size: 64 << 10,
        max_bytes_level1: 256 << 10,
        sync: true,
        ..Default::default()
    };
    let db = Db::open(dir, opts).unwrap();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for i in 0u64..u64::MAX {
        let mut b = WriteBatch::new();
        let k = format!("key{i:010}");
        b.put(
            k.as_bytes(),
            format!("value-{i}-{}", "x".repeat((i % 100) as usize)).as_bytes(),
        );
        if i % 7 == 0 && i > 0 {
            b.delete(format!("key{:010}", i - 1).as_bytes());
        }
        db.write(b).unwrap();
        writeln!(out, "{i}").unwrap();
        out.flush().unwrap();
    }
}

fn arg(rest: &[String], name: &str, default: usize) -> usize {
    rest.iter()
        .position(|a| a == name)
        .and_then(|i| rest.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn bench(dir: &str, rest: &[String]) {
    let num = arg(rest, "--num", 1_000_000);
    let vsize = arg(rest, "--value-size", 100);
    let threads = arg(rest, "--threads", 1);
    let bloom = arg(rest, "--bloom-bits", 10);
    let mut opts = Options::default();
    opts.table.bloom_bits_per_key = bloom;
    let opts2 = opts.clone();
    let open = move || Arc::new(Db::open(dir, opts2.clone()).unwrap());
    if rest.iter().any(|a| a == "--sync-only") {
        return fillsync(dir, threads, vsize);
    }
    let _ = std::fs::remove_dir_all(dir);
    let db = open();
    let value = vec![b'v'; vsize];
    let key = |i: u64| format!("{i:016}");
    let report = |name: &str, n: usize, t: Instant| {
        let s = t.elapsed().as_secs_f64();
        println!(
            "{name:<14} {:>10.0} ops/s  {:>8.2} µs/op  {:>7.1} MB/s",
            n as f64 / s,
            s * 1e6 / n as f64,
            (n * (16 + vsize)) as f64 / s / 1e6
        );
    };
    println!(
        "lsm-engine bench: {num} keys, {vsize}B values, {threads} thread(s), bloom {bloom} bits/key"
    );

    let t = Instant::now();
    for i in 0..num as u64 {
        db.put(key(i).as_bytes(), &value).unwrap();
    }
    report("fillseq", num, t);

    drop(db);
    let _ = std::fs::remove_dir_all(dir);
    let db = open();
    let t = Instant::now();
    let per = num / threads;
    std::thread::scope(|s| {
        for th in 0..threads {
            let db = db.clone();
            let value = value.clone();
            s.spawn(move || {
                let mut r = 0x9E3779B97F4A7C15u64 ^ (th as u64 + 1);
                for _ in 0..per {
                    db.put(key(xorshift(&mut r) % num as u64).as_bytes(), &value)
                        .unwrap();
                }
            });
        }
    });
    report("fillrandom", per * threads, t);
    let t = Instant::now();
    db.compact_all().unwrap();
    println!(
        "{:<14} {:>10.2} s   (levels: {})",
        "compact",
        t.elapsed().as_secs_f64(),
        db.level_summary()
    );
    println!("write-amp      {:>10.2}x", db.write_amplification());

    let t = Instant::now();
    let mut found = 0;
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|th| {
                let db = db.clone();
                s.spawn(move || {
                    let mut r = 0xD1B54A32D192ED03u64 ^ (th as u64 + 1);
                    let mut f = 0;
                    for _ in 0..per {
                        if db
                            .get(key(xorshift(&mut r) % num as u64).as_bytes())
                            .unwrap()
                            .is_some()
                        {
                            f += 1;
                        }
                    }
                    f
                })
            })
            .collect();
        found = hs.into_iter().map(|h| h.join().unwrap()).sum::<usize>();
    });
    report("readrandom", per * threads, t);
    println!(
        "               {found} of {} found, block cache hit rate {:.1}%",
        per * threads,
        db.cache_hit_rate() * 100.0
    );

    let t = Instant::now();
    let mut r = 0x1234567u64;
    for _ in 0..per {
        db.get(format!("{:016}x", xorshift(&mut r) % num as u64).as_bytes())
            .unwrap();
    }
    report("readmissing", per, t);

    let t = Instant::now();
    let n = db
        .iter(ReadOptions::default())
        .try_fold(0usize, |n, r| r.map(|_| n + 1))
        .unwrap();
    report("readseq", n, t);
}

/// Synced writes: each write() returns only after fsync. With T threads,
/// group commit lets one fsync cover many writers.
fn fillsync(dir: &str, threads: usize, vsize: usize) {
    let _ = std::fs::remove_dir_all(dir);
    let db = Arc::new(
        Db::open(
            dir,
            Options {
                sync: true,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let per = 20_000 / threads;
    let value = vec![b'v'; vsize];
    let t = Instant::now();
    std::thread::scope(|s| {
        for th in 0..threads {
            let db = db.clone();
            let value = value.clone();
            s.spawn(move || {
                for i in 0..per {
                    db.put(format!("{th:02}-{i:010}").as_bytes(), &value)
                        .unwrap();
                }
            });
        }
    });
    let n = per * threads;
    let st = db.stats();
    let groups = st.write_groups.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "fillsync  threads={threads:<3} {:>8.0} ops/s  avg group size {:.1}",
        n as f64 / t.elapsed().as_secs_f64(),
        n as f64 / groups.max(1) as f64
    );
}
