# Requirements matrix

Statuses: VERIFIED / IMPLEMENTED BUT UNVERIFIED / PARTIAL / FAILED /
BLOCKED / NOT IMPLEMENTED. Evidence paths are under
`docs/evidence/2026-10-14/` unless they are test names.

| # | Requirement | Baseline | Final | Evidence | Notes |
|---|---|---|---|---|---|
| 1 | Build, fmt, clippy `-D warnings`, tests | PARTIAL (clippy failed) | VERIFIED | `baseline.md`, `final_tests.log` | 21 tests at baseline; 13 test targets now |
| 2 | CI workflow at repo root | NOT IMPLEMENTED | IMPLEMENTED BUT UNVERIFIED | `.github/workflows/ci.yml` | lint, test, capi (C + ASan + valgrind), miri jobs; see final report for run status |
| 3 | WAL crash consistency (crash at every write/fsync) | PARTIAL (SIGKILL test only) | VERIFIED | `crash_sim.rs`, `crash_sweep.log` | power loss before each of 711 (sync) / 460 (no sync) fs ops x 5 modes, compared to a model; 6,055 crash images |
| 4 | SSTable and MANIFEST crash consistency | PARTIAL | VERIFIED | same sweep, `d7_*`, `d2_*` | sweep covers flush, compaction, MANIFEST and CURRENT writes, plus a crash during recovery |
| 5 | Compaction crash consistency | NOT IMPLEMENTED (no test) | VERIFIED | `crash_sim.rs` reference run: 13 flushes, 6 compactions | small workload; L0-L2 only |
| 6 | Model-based tests incl. snapshots, deletes, scans, reopen | PARTIAL (no snapshots) | VERIFIED | `model.rs` (64 cases x up to 300 ops) | |
| 7 | Corruption: bit flips in every file type are detected | PARTIAL (one SST test) | VERIFIED | `corruption.rs`, `corruption_sweep.log` | 9,247 flips: SST, WAL, MANIFEST, CURRENT; 0 silent; last-WAL-record limitation documented and tested |
| 8 | MVCC snapshot isolation | PARTIAL | VERIFIED | `model.rs` snapshot ops, `snapshots_survive_compaction` | |
| 9 | Compaction correctness | PARTIAL | VERIFIED | `model.rs`, `tombstones_reclaim_space`, mutation check (2 compaction mutants caught) | |
| 10 | Write/space amplification measured | PARTIAL (lsm only) | VERIFIED | `bench_compare.log` | same /proc/self/io method for all engines |
| 11 | Recovery modes | NOT IMPLEMENTED (claimed) | VERIFIED | `WalRecoveryMode`, `corruption.rs`, `d3_*` | PointInTime only lightly tested |
| 12 | C ABI: null/length checks, ownership, no panics across FFI | NOT IMPLEMENTED (claimed) | VERIFIED | `capi/`, `capi_tests.log`, `miri.log` | C program plain, ASan+UBSan, valgrind (0 errors, 0 leaks); Rust tests under Miri. ASan instruments the C side only |
| 13 | Benchmark vs RocksDB | NOT IMPLEMENTED | VERIFIED (shared machine) | `bench_compare.log` | rocksdb crate 0.24 / RocksDB 10.4.2, built here with libclang symlink |
| 14 | Benchmark vs sled and BTreeMap | NOT IMPLEMENTED | VERIFIED (shared machine) | `bench_compare.log` | |
| 15 | Mutation testing of the suite | NOT IMPLEMENTED (claimed) | PARTIAL | `mutation_check.log`, `scripts/mutation_check.sh` | 11/14 killed; see audit |
| 16 | Real power-loss test on hardware | NOT IMPLEMENTED | BLOCKED | - | needs hardware or a resettable VM disk; see OWNER_ACTIONS |
| 17 | Coverage-guided fuzzing | NOT IMPLEMENTED | NOT IMPLEMENTED | - | out of time budget |
