# Owner actions

Things only the repository owner can do. Each item says what to run and
what success looks like.

## 1. Review and merge the audit branch

The work is on branch `ccr-a6e6c567-y4w7k8`. No pull request was opened.

```bash
git fetch origin
git log --oneline origin/main..origin/ccr-a6e6c567-y4w7k8
# open a PR on GitHub from ccr-a6e6c567-y4w7k8 into main, review, merge
```

Success: the `CI` workflow runs on the PR and all jobs are green.

## 2. Check that GitHub Actions is enabled

The workflow lives in `.github/workflows/ci.yml`. If the Actions tab shows
no runs after a push, enable Actions in Settings -> Actions -> General.

Success: a run named `CI` appears for each push, with jobs `lint`, `test`,
`capi` and `miri`.

## 3. Existing databases need to be rebuilt (on-disk format change)

The WAL/MANIFEST record format changed (file magic `LSMLOG\0\2`, a
checksum on the record length). The new code refuses to open a database
written by the old code ("bad log magic"), and the old code cannot read
new files. If you have data in the old format (for example in the
companion Raft KV project), export it with the old binary first:

```bash
git checkout 15d55a5 -- lsm-engine   # old code
cargo run --release --bin lsmctl -- /path/to/db scan > dump.tsv
git checkout ccr-a6e6c567-y4w7k8 -- lsm-engine
# re-insert dump.tsv with the new lsmctl (put per line)
```

Success: `lsmctl <dir> scan` on the new database prints the same rows.

## 4. C API consumers

The C API (`capi/`, header `capi/include/lsm.h`) is new in this branch.
The old README described a C API and a `capi/` directory that did not
exist in the repository. If the companion Go project links against some
other C API, its bindings must be updated to this header.

Success: the Go project builds against `capi/target/release/liblsm.a` and
its tests pass.

## 5. Benchmarks on dedicated hardware (optional)

All numbers in `docs/evidence/2026-10-14/` come from a shared 4-vCPU cloud
container with other jobs running (load averages are recorded next to
each run). For numbers you can quote, run on an idle machine:

```bash
cd bench-compare
LIBCLANG_PATH=/usr/lib/llvm-18/lib cargo build --release   # needs libclang.so
for e in lsm rocksdb sled btreemap; do
  for r in 1 2 3; do ./target/release/bench-compare $e /tmp/bc 500000; done
done
```

If the build fails with "couldn't find any valid shared libraries matching
libclang.so", point `LIBCLANG_PATH` at a directory containing
`libclang.so` (a symlink to `libclang-18.so.1` is enough).

Success: three runs per engine within about 10% of each other.

## 6. Real power-loss testing (optional)

Crash consistency is verified with a simulated file system (`SimFs`) and
SIGKILL tests. Neither cuts power to a real disk. A real test needs
hardware (or a VM with a virtual disk you can hard-reset, plus
`dm-log-writes` or similar). This cannot be done from this container.
