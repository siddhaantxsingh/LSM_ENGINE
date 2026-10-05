# Design notes

How the engine works, why each piece exists, and the trade-offs I made. File names point to the code.

## 1. Why an LSM tree?

A B-tree updates pages **in place**: a random write means read page, modify, write page, which is random I/O. An LSM tree (Log-Structured Merge tree) turns all writes into **sequential** I/O: append to a log, buffer in memory, and write out big sorted files. Old data is reorganized later in the background (compaction).

The trade-off is captured by three amplification factors:

| | Meaning | LSM vs B-tree |
|---|---|---|
| Write amplification | bytes written to disk / bytes written by user | LSM better for random writes, though compaction rewrites data several times |
| Read amplification | disk reads per lookup | LSM worse (several levels to check); bloom filters and caching compensate |
| Space amplification | disk bytes / live data bytes | LSM holds stale versions until compaction |

This is why RocksDB (LSM) powers write-heavy systems (Kafka Streams state, CockroachDB, TiKV) while B-trees (InnoDB, Postgres) dominate read-heavy OLTP.

## 2. Data model: internal keys and sequence numbers (`key.rs`)

Every write gets a monotonically increasing **sequence number**. Stored keys are *internal keys*:

```
user_key ++ 8-byte trailer = (seq << 8) | kind      kind: Put=1, Delete=0
```

Ordering: user key ascending, then **seq descending**. All versions of a key sit next to each other, newest first. To read key `k` at snapshot `s`, seek to `(k, s)`: the first entry at or after that position with user key `k` is the newest version visible at `s`.

Gotcha: you can't compare internal keys bytewise. `"a"+trailer` vs `"ab"+trailer` would compare a trailer byte against `'b'`. So `cmp_internal` splits off the trailer first.

**Deletes are writes.** A delete writes a *tombstone*. You can't remove the key from older files (they're immutable), so the tombstone shadows them until compaction drops both.

## 3. Write path (`db.rs: Inner::write`)

1. **WAL first.** The batch is appended to the write-ahead log *before* the memtable, so a crash can replay it. With `sync=true` the WAL is fsynced before `write()` returns.
2. **Memtable.** An ordered in-memory map. When it exceeds `write_buffer_size` (4 MiB) it is frozen as the *immutable memtable*, a new WAL file starts, and the background thread flushes it to an L0 table.

### Group commit

fsync takes milliseconds; doing one per write caps a single disk at a few thousand writes/s. Writers join a queue. The writer at the front becomes the **leader**: it merges every queued batch (up to 1 MiB) into one, assigns a contiguous range of sequence numbers, writes **one** WAL record and does **one** fsync, then wakes the followers it covered.

- The state lock is **released during the WAL write and fsync**. Only the leader writes the WAL, so no lock is needed there, and readers aren't blocked behind disk I/O.
- A non-sync leader never absorbs a sync writer, which would return before that writer's data was fsynced.
- **Visibility:** `visible_seq` is bumped only *after* the memtable insert, so readers never see half of a batch. That makes batches atomic for readers, not just on disk.

Result: 1 → 32 threads gives 4.7k → 32.9k synced writes/s, with ~19 writers per fsync.

### Backpressure

If compaction falls behind, L0 piles up and reads slow down, since every L0 file may need checking. At 8 L0 files each write is delayed 1 ms (a soft limit that smooths latency); at 12 writes stop until compaction catches up. Same idea as RocksDB's write stalls.

## 4. SSTable format (`sstable.rs`, `block.rs`)

```
[data block | crc] ... [filter block | crc] [index block | crc] [footer: 40 bytes]
```

- **Data blocks (~4 KiB):** keys are *prefix-compressed*, storing only the bytes that differ from the previous key, which is great for sorted keys like `user:000123`. Every 16 entries a **restart point** stores the full key; a lookup binary-searches the restart points, then scans ≤ 16 entries.
- **Index block:** the last key of each data block plus its offset/size. A lookup costs one index search and one block read.
- **Per-block CRC32:** detects bit rot and torn writes on read (`detects_corrupted_table_on_read`).
- **Footer:** fixed size at the end of the file with a magic number, so a reader opens a table from just its length.
- **Immutable:** tables are written once and never modified. That makes caching, concurrent reads and crash safety simple: a table either exists completely (it is fsynced before the MANIFEST references it) or not at all.

### Bloom filters (`bloom.rs`)

Each table carries a bloom filter over its user keys. A `get` checks it first, and a negative answer is definitive, so the table read is skipped. At 10 bits/key and k=7 hash functions the false-positive rate is ~0.8% (unit test checks < 2%). The k hashes come from **double hashing** (`h1 + i·h2`, Kirsch–Mitzenmacher), so one hash computation gives all seven.

The bench shows the effect: **missing-key reads 7.7× faster**, since each lookup would otherwise read a block in every level.

### Block cache (`cache.rs`)

Sharded LRU keyed by `(file number, block offset)`, bounded in bytes. 16 shards reduce lock contention between reader threads. Eviction uses a `BTreeMap<tick, key>`, which makes finding the least-recently-used entry O(log n).

## 5. Levels and compaction (`compaction.rs`)

- **L0:** flushed memtables. Files **overlap** (each covers whatever keys were written in that window), so a read checks every L0 file, newest first.
- **L1..L6:** each level is one **sorted run** of non-overlapping files, so a read binary-searches for the single candidate file per level. Level `n` may hold `10 MiB × 10^(n−1)`.

**Picking:** each level gets a score (L0: files / 4; Ln: bytes / budget). The highest score ≥ 1 is compacted. In Ln we pick one file, rotating through the key space with a **compact pointer** so every region eventually gets compacted, and add every overlapping file from Ln+1. If the Ln inputs can be widened without pulling in more Ln+1 files, we widen (fewer, larger compactions).

**Merging:** a merging iterator over the inputs yields entries in internal-key order. For each user key:
- Keep the newest version. Drop an older version **only if** a newer version exists that every live snapshot can see (`last_seq_for_key ≤ smallest_snapshot`); otherwise a snapshot reader would lose its data.
- Drop a tombstone if it is visible to all snapshots **and** no deeper level could hold the key (`is_base_level_for_key`). Otherwise the tombstone must survive to keep shadowing the older value below.
- Output files are cut at ~2 MiB, but **only at user-key boundaries**. If one user key's versions spanned two files in a level, the "one candidate file per level" lookup would break.

**Trivial move:** if an Ln file overlaps nothing in Ln+1, just re-parent it in the MANIFEST with no I/O. *Bug avoided:* the edit "deletes" and "adds" the same file number, and deletion marks a file obsolete (deleted from disk when unreferenced). `Version::apply` detects this and moves the existing `FileMeta` instead; otherwise trivial moves would delete live data.

**Write amplification:** measured at 4.3× for fillrandom plus full compaction (flush + compaction bytes / user bytes). Leveled compaction trades higher write amp for low read/space amp; tiered/universal compaction (Cassandra, RocksDB universal) makes the opposite choice.

## 6. Metadata and crash safety (`version.rs`)

The set of live files per level is a **Version**. Versions are **immutable**: a flush or compaction produces a **VersionEdit** ("add file 12 to L1, delete files 7, 9"), appends it to the **MANIFEST** log with fsync, and installs a new Version.

Crash-ordering rules:
1. New tables are fully written **and fsynced** before the edit that references them.
2. The edit is fsynced before the old files are deleted.
3. `CURRENT` (the name of the active MANIFEST) is replaced atomically: write temp → fsync → rename → fsync directory.

So after a crash the MANIFEST describes a consistent set of complete files. Leftovers (outputs of a compaction that crashed before its edit) aren't referenced and are removed on open (`delete_obsolete_files`).

**File lifetime:** readers and iterators hold an `Arc<Version>`. A compaction can replace the current version while a long scan is still reading the old files. Files are deleted in `FileMeta::drop` only when marked obsolete **and** no version references them. Reference counting gives safe concurrent deletion without any explicit reader tracking.

**Recovery (`Db::open`):** replay the MANIFEST → replay WAL files newer than `log_number` into memtables → flush them to L0 → write a fresh MANIFEST snapshot → delete obsolete files. `log_number` advances when a flush completes, so WAL files whose data is safely in tables are deleted.

## 7. Reads (`Db::get`, `Db::iter`)

`get`: take references to (memtable, immutable memtable, current version, sequence) under the lock (microseconds), then search with no lock: memtable → immutable → L0 newest-first → one file per deeper level. The first hit wins, because newer data always sits above older data. A tombstone hit means "not found" immediately.

`iter`/`scan`: a **MergingIterator** over the memtables, each L0 table, and one **LevelIterator** per level (it opens files lazily, so a short range scan touches only the files it needs). On top sits `DbIterator`, which applies MVCC: skip entries newer than the snapshot, emit only the newest version of each key, and hide keys whose newest visible entry is a tombstone.

**Snapshots** are just a sequence number plus a registration (so compaction knows the oldest one still in use). They cost almost nothing to create.

## 8. Testing strategy

| Test | What it proves |
|---|---|
| `crash.rs` (SIGKILL) | Durability: every acked synced write survives a kill at any point, including mid-compaction and during recovery. A mutation check confirms the test fails if the WAL flush is removed. |
| `behaves_like_btreemap` (proptest) | Functional correctness against a trivially correct model, across flush/compact/reopen, with automatic shrinking to a minimal failing case. |
| `snapshots_survive_compaction` | Compaction GC respects snapshots. |
| `tombstones_reclaim_space` | Deleted data is actually removed (>95% space reclaimed). |
| `detects_corrupted_table_on_read` | Bit flips are reported as errors, not returned as data. |
| `concurrent_readers_and_writers` | Thread safety; iterators always see sorted, unique keys. |

Compaction checks iterator `status()` before installing its output. A read error mid-merge would otherwise look like "end of data" and **silently drop the rest of the inputs**, which is a classic and catastrophic bug.

## 9. Limitations and next steps

- Memtable is a `BTreeMap` behind an `RwLock`, not a lock-free skiplist (concurrent inserts would need one).
- One background thread: flushes wait behind long compactions. RocksDB separates flush and compaction pools.
- No compression (Snappy/LZ4 per block would cut space 2–4× for text data).
- One open file handle per live table (no bounded table cache).
- No range deletes, column families, or prefix bloom filters.
- No subcompactions (splitting one big compaction across threads).

## 10. Interview questions to be ready for

1. *LSM vs B-tree: when would you choose each?* §1; talk about the three amplifications.
2. *Walk through a write and a read.* §3, §7.
3. *How are deletes handled, and when is the space reclaimed?* Tombstones; dropped at the base level when no snapshot needs them (§5).
4. *Why does L0 allow overlapping files, but L1+ doesn't?* L0 is a direct memtable dump; L1+ is merged into sorted runs so a read checks only one file per level.
5. *What's a bloom filter's false-positive rate, and why are there no false negatives?* ~0.6185^(bits/key); a set bit is never cleared.
6. *How do you guarantee crash consistency?* fsync order: table → MANIFEST edit → delete old files; atomic CURRENT; CRC'd logs with torn-tail truncation (§6).
7. *How do snapshots work with compaction?* Sequence numbers; compaction keeps the newest version ≤ each live snapshot (§5).
8. *What is group commit and why does it help?* §3.
9. *What is write amplification for leveled compaction, roughly?* ~10× per level (fan-out) in the worst case; measured 4.3× here because the dataset is small.
10. *How did you test durability?* SIGKILL test plus the mutation check (§8).
