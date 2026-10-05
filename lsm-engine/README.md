<div align="center">
<h1>LSM Engine</h1>

<h3>An embeddable LSM-tree key-value storage engine written from scratch in Rust.</h3>

<p>
<img src="https://img.shields.io/badge/Rust-DEA584?style=for-the-badge&logo=rust&logoColor=black">
<img src="https://img.shields.io/badge/LSM-Storage%20Engine-6A5ACD?style=for-the-badge">
<img src="https://img.shields.io/badge/WAL-CRC32%20%2B%20Recovery-FF8C00?style=for-the-badge">
<img src="https://img.shields.io/badge/MVCC-Snapshots-2E8B57?style=for-the-badge">
<img src="https://img.shields.io/badge/C%20ABI-Embeddable-1E90FF?style=for-the-badge">
</p>
**WAL → MemTable → Immutable MemTable → SSTables → Leveled Compaction**
</div>

------------------------------------------------------------------------

## ✦ Why build another storage engine?

Because storage engines hide an enormous amount of systems engineering
behind a tiny API:

``` rust
db.put(key, value)?;
db.get(key)?;
db.scan(...)?;
```

Underneath that API are questions about:

-   crash consistency
-   file formats
-   corruption
-   write ordering
-   compaction
-   read amplification
-   metadata recovery
-   concurrent writers
-   snapshots
-   cache behavior
-   resource limits

`lsm-engine` implements those mechanisms from scratch in Rust and
documents the bugs discovered while hardening them.

------------------------------------------------------------------------

## 🧭 Architecture

``` mermaid
flowchart TB
    W["Put / WriteBatch"] --> Q["Writer / Group Commit"]
    Q --> WAL[("WAL")]
    Q --> M["MemTable"]

    M -->|"full"| I["Immutable MemTable"]
    I --> L0[("L0 SSTables")]

    L0 --> L1[("L1")]
    L1 --> L2[("L2")]
    L2 --> L3["..."]
    L3 --> L6[("L6")]

    R["Get / Scan"] --> M
    R --> I
    R --> L0
    R --> L1
    R --> L2

    MAN[("MANIFEST")] -. "live file set" .-> L0
    MAN -. "version edits" .-> L1
    MAN -. "version edits" .-> L2
```

### Write lifecycle

``` mermaid
sequenceDiagram
    participant C as Caller
    participant W as Writer
    participant M as MemTable
    participant D as Disk

    C->>W: Put / WriteBatch
    W->>D: Append WAL record
    W->>D: fsync
    W->>M: Apply batch
    W-->>C: Success
    M->>D: Flush to SSTable
```

The WAL is the durability boundary for acknowledged synchronous writes.

------------------------------------------------------------------------

## 🔎 Read path

``` mermaid
flowchart LR
    A["Get / Scan"] --> B{"Active MemTable"}
    B -->|hit| R["Return"]
    B -->|miss| C{"Immutable MemTable"}
    C -->|hit| R
    C -->|miss| D{"Bloom filter"}
    D -->|negative| E["Not found"]
    D -->|possible| F["SSTable block/index"]
    F --> R
```

The read path combines:

-   ordered MemTable lookup
-   immutable MemTable lookup
-   Bloom filters
-   block indexes
-   prefix-compressed blocks
-   sharded LRU block cache
-   merged iteration
-   MVCC filtering

------------------------------------------------------------------------

## 🧱 SSTable format

``` mermaid
flowchart TB
    H["SSTable"] --> D["Data blocks"]
    H --> I["Index"]
    H --> F["Bloom filter"]
    H --> T["Footer / metadata"]

    D --> P["Prefix compression"]
    D --> RP["Restart points"]
    D --> CRC["Per-block CRC"]
```

Every on-disk structure is bounds-checked before use.

Malformed metadata is expected to become an explicit error, not an
allocator catastrophe or silent wrong answer.

------------------------------------------------------------------------

## 🧬 MVCC snapshots

``` text
user key
   ↓
internal key = user key + sequence + type
   ↓
versioned ordering
   ↓
snapshot sequence
   ↓
visible value
```

Example:

``` rust
let db = Db::open("/tmp/db", Options::default())?;

db.put(b"user:42", b"Ada")?;

let snap = db.snapshot();

db.delete(b"user:42")?;

assert_eq!(
    db.get_opt(
        b"user:42",
        ReadOptions { snapshot: Some(&snap) }
    )?,
    Some(b"Ada".to_vec())
);
```

Snapshots provide point-in-time reads without copying the entire
database.

------------------------------------------------------------------------

## 🔥 Compaction

``` mermaid
flowchart LR
    L0["L0<br/>overlapping"] --> C1["Pick compaction"]
    C1 --> M["Merge sorted streams"]
    M --> G["Apply MVCC / tombstone rules"]
    G --> O["Write new SSTables"]
    O --> F["Fsync + metadata update"]
    F --> N["Install new version"]
    N --> X["Delete obsolete files safely"]
```

Leveled compaction uses approximately 10× level-size targets, trivial
moves, compact pointers, and tombstone garbage collection.

The MANIFEST is treated as critical metadata rather than a best-effort
log.

------------------------------------------------------------------------

## 🧨 Crash consistency

The engine explicitly distinguishes:

``` text
Crash-torn tail
       vs.
Mid-log corruption
       vs.
Valid log
```

Recovery modes include:

-   `TolerateCorruptedTail`
-   `AbsoluteConsistency`
-   `PointInTime`

The final hardening pass fixed a bug where mid-WAL damage could silently
drop synced writes.

It also added directory fsyncs around creation of new WAL/SST files.

------------------------------------------------------------------------

## 🛡️ Metadata safety

One of the hardest audit findings involved the MANIFEST.

A corrupted or truncated MANIFEST could make newer SSTables appear
obsolete.

The hardened engine now refuses unsafe recovery instead of deleting
tables based on incomplete metadata.

``` mermaid
flowchart TD
    A["Open DB"] --> B["Read CURRENT"]
    B --> C["Replay MANIFEST"]
    C --> D{"Version history coherent?"}
    D -->|Yes| E["Install Version"]
    D -->|No| F["FAIL SAFE"]
    F --> G["Keep existing SSTables"]
```

This is a deliberate design choice:

> **When metadata is ambiguous, refusing to open is safer than deleting
> data.**

------------------------------------------------------------------------

## 🧪 Correctness testing

The final verification includes:

  Check                                                  Result
  ------------------------------ ------------------------------
  Rust tests                                   **47 / 47 PASS**
  Model-based tests                                    **PASS**
  Snapshot + batch model tests                         **PASS**
  Mutation testing                                     **PASS**
  SIGKILL crash test               **PASS, 3 consecutive runs**
  Decoder robustness                                   **PASS**
  C ABI round-trip                                     **PASS**
  `cargo audit`                           **0 vulnerabilities**
  `cargo fmt --check`                                  **PASS**
  `cargo clippy -D warnings`                           **PASS**

### Model-based testing

Random sequences of:

``` text
put
delete
get
scan
flush
compact
reopen
snapshot
WriteBatch
```

are compared against a reference `BTreeMap`.

### Mutation testing

Real SST and MANIFEST bytes are flipped/truncated.

The required outcome is:

``` text
correct value
      OR
explicit error

NEVER:
wrong value
silent missing data
silent incomplete scan
```

------------------------------------------------------------------------

## 🐛 Bugs found during hardening

The repository preserves the engineering history.

### Critical: flush/write race

A concurrent `flush()` could race the write leader and lose an
acknowledged write.

**Fixed + regression tested.**

### High: corrupt SST footer

Malformed metadata could trigger an attempted **1 TiB allocation**.

**Fixed + regression tested.**

### High: mid-WAL damage

Synced writes after corruption could be silently dropped.

**Fixed + recovery tests added.**

### Critical: corrupted MANIFEST

An empty/truncated MANIFEST could make the engine delete valid SSTables.

**Fixed + fail-safe recovery tests added.**

This is the kind of debugging work that makes the project more than a
data-structure exercise.

------------------------------------------------------------------------

## 📈 Performance

Recorded on a noisy **2-vCPU cloud VM**, 1M keys, 16-byte keys, 100-byte
values, `sync=false`, 2 threads.

  Benchmark                               Throughput Latency
  ----------------------- -------------------------- ------------------------
  `fillseq`                     **240k--248k ops/s** ---
  `fillrandom`                  **108k--126k ops/s** p50 3.9 µs · p99 71 µs
  `readrandom`                  **320k--423k ops/s** p50 5.3 µs · p99 22 µs
  `readmissing` + Bloom       **1.70M--1.83M ops/s** ---
  `readseq`                     **3.1M--3.7M ops/s** ---
  seek + 100-row scan       **20.8k--23.7k scans/s** p50 48 µs
  Write amplification                      **4.62×** ---

### Synchronous group commit

``` text
1 writer   → ~4.3k ops/s
32 writers → ~29.4k ops/s
```

The 32-writer workload achieved roughly **19 writes per fsync**.

These are same-machine engineering measurements, not production capacity
claims.

------------------------------------------------------------------------

## 🔌 C ABI

The engine exposes an embeddable C ABI:

``` text
Rust storage engine
        │
        ├── native Rust API
        │
        └── C ABI
              │
              ▼
          Go systems
```

The C ABI is used by the companion Raft KV project as its persistent
state-store backend.

------------------------------------------------------------------------

## 🚀 Quick start

``` bash
cargo test --release
```

CLI:

``` bash
cargo run --release --bin lsmctl -- /tmp/db put hello world
cargo run --release --bin lsmctl -- /tmp/db get hello
cargo run --release --bin lsmctl -- /tmp/db scan
```

Benchmark:

``` bash
cargo run --release --bin lsmctl -- \
  /tmp/bench bench --num 1000000 --threads 2

cargo run --release --bin lsmctl -- \
  /tmp/bench bench --sync-only --threads 32
```

------------------------------------------------------------------------

## 📁 Repository map

``` text
src/
├── db.rs
├── memtable.rs
├── log.rs
├── sstable.rs
├── block.rs
├── bloom.rs
├── cache.rs
├── version.rs
├── compaction.rs
├── iterator.rs
├── key.rs
└── options.rs

tests/
├── crash.rs
├── corruption.rs
├── model.rs
└── db.rs

capi/
├── include/lsm.h
└── src/lib.rs

docs/
├── durability
├── performance
├── security
├── ADRs
└── audits
```

\~5.7k lines of Rust including tests.

------------------------------------------------------------------------

## 📚 Documentation

  ------------------------------------------------------------------------------------------------------------------
  Area                                Link
  ----------------------------------- ------------------------------------------------------------------------------
  Design                              [`DESIGN.md`](DESIGN.md)

  Durability                          [`docs/LSM_DURABILITY_AND_RECOVERY.md`](docs/LSM_DURABILITY_AND_RECOVERY.md)

  Performance                         [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)

  Security                            [`docs/SECURITY.md`](docs/SECURITY.md)

  Interview guide                     [`docs/INTERVIEW.md`](docs/INTERVIEW.md)

  Final audit                         [`docs/FINAL_AUDIT.md`](docs/FINAL_AUDIT.md)

  Architecture audit                  [`docs/COMBINED_ARCHITECTURE_AUDIT.md`](docs/COMBINED_ARCHITECTURE_AUDIT.md)

  ADRs                                [`docs/adr/`](docs/adr/)
  ------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 🚧 Honest limitations

Not production-ready.

Known gaps:

-   no power-loss filesystem fault injection
-   no coverage-guided fuzzing campaign
-   limited compaction parallelism
-   no compression codec
-   limited metrics/tracing
-   no backup/repair subsystem
-   one background compaction worker

The project is intended as a **carefully tested storage-engine
implementation**, not a replacement for RocksDB.

------------------------------------------------------------------------

<div align="center">
### A storage engine is a promise about data.

### The interesting engineering starts when the disk, metadata, and process stop behaving nicely.
</div>
