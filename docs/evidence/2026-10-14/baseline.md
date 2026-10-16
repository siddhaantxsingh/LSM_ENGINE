# Baseline (before any change), 2026-10-14

Commit: `15d55a5` (main). Machine: shared 4-vCPU cloud container, load
average 7-8 during the runs (other jobs were running). Toolchain: rustc
1.99.0, clang 18.1.3, valgrind 3.22. Logs are in this directory.

| Check | Command | Result | Log |
|---|---|---|---|
| Build | `cargo build --release --all-targets` | PASS (53 s) | `baseline_build.log` |
| Format | `cargo fmt --check` | PASS | `baseline_fmt.log` |
| Lint | `cargo clippy --all-targets -- -D warnings` | FAIL: 1 error (`for_unbounded_range` in `lsmctl.rs`) | `baseline_clippy.log` |
| Tests | `cargo test --release` | PASS: 21 tests (9 unit, 10 integration, 1 SIGKILL crash, 1 doctest) | `baseline_tests.log` |
| Bench | `lsmctl bench --num 1000000 --threads 2` x3 | ran; numbers vary 2-3x between runs because of load | `baseline_bench.log` |

Baseline benchmark ranges (3 runs, 1M keys, 2 threads): fillseq 95k-270k
ops/s, fillrandom 45k-121k ops/s, readrandom 146k-252k ops/s,
readmissing 0.93M-1.82M ops/s, readseq 3.2M-3.5M ops/s, write-amp
3.99x-4.40x.

## README claims that did not hold at baseline

- "Rust tests 47 / 47 PASS": the repository had 21 tests.
- "`cargo clippy -D warnings` PASS": failed with 1 error on this toolchain.
- C ABI, `capi/include/lsm.h`, `capi/src/lib.rs`: did not exist.
- `tests/corruption.rs`, `tests/model.rs`, `docs/` (durability,
  performance, security, ADRs, audits): did not exist.
- Recovery modes `TolerateCorruptedTail` / `AbsoluteConsistency` /
  `PointInTime`: did not exist in the code.
- "Mid-WAL damage ... fixed": not fixed (defect D3).
- "Corrupted MANIFEST ... fail-safe recovery": not fixed (defect D2).
- "Corrupt SST footer ... 1 TiB allocation fixed": not fixed (defect D4).
- "Directory fsyncs around creation of new WAL/SST files": not present (D7).
- "Flush/write race fixed": the race was still there (defect D1).
- "Mutation testing PASS", "cargo audit 0 vulnerabilities": no script or
  evidence in the repository; not reproduced.
