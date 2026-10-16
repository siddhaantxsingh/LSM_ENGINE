# LSM Engine

An LSM-tree key-value storage engine written from scratch in Rust, with a
C API. WAL -> memtable -> L0 SSTables -> leveled compaction, MVCC
snapshots, group commit, bloom filters, a sharded block cache.

Not production-ready. This is a learning and portfolio project with a
serious test suite. The 2026-10-14 audit is in
[`docs/AUDIT_2026-10-14.md`](docs/AUDIT_2026-10-14.md); all numbers below
have logs in [`docs/evidence/2026-10-14/`](docs/evidence/2026-10-14/).

## Layout

```
lsm-engine/      the engine (Rust crate) and lsmctl CLI
  src/fs.rs      FileSystem trait; StdFs (real) and SimFs (power-loss simulator)
  tests/         regressions, crash_sim, corruption, model, db, crash (SIGKILL)
capi/            C API: include/lsm.h, Rust cdylib/staticlib, C test program
bench-compare/   benchmark vs RocksDB, sled and a BTreeMap (not built by CI)
scripts/         mutation_check.sh
docs/            audit, requirements matrix, owner actions, evidence logs
```

Design notes: [`lsm-engine/DESIGN.md`](lsm-engine/DESIGN.md).

## Quick start

```bash
cd lsm-engine
cargo test --release
cargo run --release --bin lsmctl -- /tmp/db put hello world
cargo run --release --bin lsmctl -- /tmp/db get hello
cargo run --release --bin lsmctl -- /tmp/db scan
cd ../capi && ./run_c_tests.sh     # C API: plain, ASan+UBSan, valgrind
```

Scans return `Result` items. If a read error stops a scan early, the last
item is an `Err`, so an incomplete scan is never mistaken for a complete
one:

```rust
for item in db.scan(Some(b"a"), Some(b"z"), ReadOptions::default()) {
    let (k, v) = item?;
}
```

## What is tested

| Test | What it checks |
|---|---|
| `tests/crash_sim.rs` | Power loss before every file-system operation (create, append, fsync, rename, remove, directory fsync) of a workload with flushes and compactions, under 5 durability modes (unsynced data dropped / torn / zero-filled / kept; directory entries dropped / kept). Recovery must equal the model after k writes, with every acknowledged synced write present. A second crash is injected into recovery itself. 6,055 crash images. |
| `tests/corruption.rs` | 9,247 single-bit flips across SST, WAL, MANIFEST and CURRENT files. Every outcome is an error or correct data; never a wrong value, a silently missing key, or a silently short scan. |
| `tests/model.rs` | proptest against a `BTreeMap` model: puts, deletes, batches, gets, range scans, snapshot reads and scans, flush, compaction, reopen. |
| `tests/regressions.rs` | One failing-first test per audit defect (D1-D10). |
| `tests/crash.rs` | SIGKILL of a real process mid-write/flush/compaction, 5 rounds. |
| `capi/` | NULL/length checks, ownership, panics caught at the boundary; Rust tests also run under Miri; the C program runs under ASan+UBSan and valgrind (0 errors, 0 leaks). |
| `scripts/mutation_check.sh` | Injects 14 known bugs; 11 were caught in the recorded run (see audit for the 3 survivors). |

`SimFs` models strict POSIX durability in memory. It is not a real power
cut; see [`docs/OWNER_ACTIONS.md`](docs/OWNER_ACTIONS.md).

## Recovery behaviour

- MANIFEST: a torn last record (crash during an append) is dropped. Any
  other damage, an empty MANIFEST, an incoherent history, or a live table
  that is missing or has the wrong size makes `Db::open` fail. Nothing is
  deleted before the MANIFEST checks out. Tables orphaned by a dropped
  torn record are renamed `*.sst.lost`, not deleted.
- WAL, `WalRecoveryMode`:
  - `TolerateCorruptedTail` (default): drop a torn tail of the newest WAL
    that has data; anything else is an error. Limitation: a bit flip in
    the payload of the very last record looks like a torn write and that
    record is dropped.
  - `AbsoluteConsistency`: any damage is an error.
  - `PointInTime`: replay up to the first damage, ignore the rest.
- Log record format: `len | crc(len) | crc(payload) | payload | 0xA5`,
  files start with the magic `LSMLOG\0\2`. This changed in the audit;
  older databases cannot be opened.

## Benchmarks

`bench-compare`, 300,000 random 16-byte keys, 100-byte values, single
thread, no compression, bloom 10 bits/key. Median of 3 runs on a shared
4-vCPU container (load average 8-11 from other jobs). Treat these as
rough same-machine comparisons; they vary up to ~2x between runs. Full
log: `docs/evidence/2026-10-14/bench_compare.log`.

| | lsm-engine | RocksDB 10.4.2 | sled 0.34 | BTreeMap (memory) |
|---|---|---|---|---|
| fillrandom (ops/s) | 141k | 145k | 120k | 820k |
| readrandom (ops/s) | 361k | 350k | 442k | 1.34M |
| readmissing (ops/s) | 2.19M | 1.98M | 1.16M | 4.16M |
| seek + 100 next (scans/s) | 37k | 52k | 18k | 433k |
| fsync per write (ops/s) | 278 | 302 | see note | - |
| disk bytes written / user bytes | 4.0x | 3.9-4.1x | 2.9-3.3x | - |
| disk size after compaction | 21 MB | 21 MB | 62 MB | - |

The data set is small (35 MB) and mostly cached, so this does not show
how either engine behaves on large data. The sled synced-write numbers
(298-9,669 ops/s) suggest `flush()` did not always reach the disk and
are not comparable. RocksDB ran with near-default options matched to
this engine's sizes.

## Limitations

- Not tested on real power loss or real disk faults.
- No compression, one background thread, no bounded table cache, no
  range deletes, no coverage-guided fuzzing.
- Memtable iterators copy the memtable.
