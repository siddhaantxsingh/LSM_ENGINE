#!/usr/bin/env bash
# Injects known bugs one at a time and checks that the test suite fails.
# Each mutation is a (file, exact old text, new text) replacement.
# Usage: scripts/mutation_check.sh   (from the repo root; restores files)
set -u
cd "$(dirname "$0")/../lsm-engine"
export LSM_CRASH_STRIDE=${LSM_CRASH_STRIDE:-2}

mutate() { # name file old new
  local name="$1" file="$2" old="$3" new="$4"
  cp "$file" "$file.orig"
  python3 - "$file" "$old" "$new" <<'PY'
import sys
p, old, new = sys.argv[1:]
s = open(p).read()
if s.count(old) != 1:
    sys.exit(f"mutation text found {s.count(old)} times in {p}")
open(p, "w").write(s.replace(old, new))
PY
  if [ $? -ne 0 ]; then echo "SKIP  $name (pattern not found)"; mv "$file.orig" "$file"; return; fi
  if timeout 900 cargo test --release -q --test crash_sim --test regressions --test db --test model > /tmp/mut_$$.log 2>&1; then
    echo "SURVIVED $name"
  else
    echo "KILLED   $name ($(grep -m1 -oE 'test [a-z0-9_]+ \.\.\. FAILED|error\[E[0-9]+\]' /tmp/mut_$$.log))"
  fi
  mv "$file.orig" "$file"; touch "$file"  # newer mtime so cargo rebuilds
}

mutate "no dir fsync before MANIFEST edit" src/version.rs \
  "            self.fs.sync_dir(&self.dir)?;
        }
        let w = self.manifest" \
  "        }
        let w = self.manifest"
mutate "no dir fsync for new WAL" src/db.rs \
  "        self.opts.fs.sync_dir(&self.dir)?;
        {" "        {"
mutate "sync writes not fsynced" src/db.rs \
  "wal.flush(leader_sync)?;" "wal.flush(false)?;"
mutate "old WAL not synced on switch" src/db.rs \
  "wal.flush(true)?; // old log" "wal.flush(false)?; // old log"
mutate "SST not fsynced" src/sstable.rs \
  "        self.buf.clear();
        self.file.sync()?;" "        self.buf.clear();"
mutate "deletes current WAL" src/db.rs \
  "                    && n < keep_from" "                    && n <= keep_from"
mutate "mid-log corruption treated as torn tail" src/log.rs \
  "            break corrupt(p, \"record checksum mismatch\");" \
  "            break torn(p, \"record checksum mismatch\");"
mutate "tombstone dropped without base-level check" src/compaction.rs \
  "kind(ik) == Kind::Delete && s <= smallest_snapshot && c.is_base_level_for_key(uk)" \
  "kind(ik) == Kind::Delete && s <= smallest_snapshot"
mutate "old versions dropped ignoring snapshots" src/compaction.rs \
  "let drop = if last_seq_for_key <= smallest_snapshot {" \
  "let drop = if last_seq_for_key != MAX_SEQ {"
mutate "flush ignores in-flight write (D1)" src/db.rs \
  "            while st.imm.is_some() || st.write_in_progress {" \
  "            while st.imm.is_some() {"
mutate "snapshot reads see newer data" src/memtable.rs \
  "let target = InternalKey(make_internal_key(key, snapshot, SEEK_KIND));" \
  "let target = InternalKey(make_internal_key(key, MAX_SEQ, SEEK_KIND));"
mutate "bloom filter false negatives" src/bloom.rs \
  "        if filter[pos / 8] & (1 << (pos % 8)) == 0 {" \
  "        if filter[pos / 8] & (1 << (pos % 8)) == 0 || user_key.len() % 7 == 3 {"
mutate "block CRC not checked" src/sstable.rs \
  "    if crc32fast::hash(&buf) != crc {" "    if crc32fast::hash(&buf) != crc && false {"
mutate "MANIFEST mid-log corruption tolerated" src/version.rs \
  "            LogEnd::TornTail { .. } => self.manifest_tail_dropped = true," \
  "            LogEnd::TornTail { .. } | LogEnd::Corrupt { .. } => self.manifest_tail_dropped = true,"
rm -f /tmp/mut_$$.log
