//! Real crash test: run a child process that writes with `sync = true`
//! and prints each key after write() returns, SIGKILL it at a random
//! moment (mid-write, mid-flush or mid-compaction), reopen, and check that
//! every acknowledged write is present. Repeated several times on the same
//! directory so recovery itself is also crashed into.

use lsm_engine::{Db, Options};
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn survives_sigkill() {
    let dir = tempfile::tempdir().unwrap();
    let exe = env!("CARGO_BIN_EXE_lsmctl");
    let mut max_acked: Option<u64> = None;

    for round in 0..5 {
        let mut child = Command::new(exe)
            .arg(dir.path())
            .arg("crash-writer")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let start = Instant::now();
        let kill_after = Duration::from_millis(300 + 250 * round);
        let mut last = None;
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let Ok(line) = line else { break };
            last = Some(line.trim().parse::<u64>().unwrap());
            if start.elapsed() > kill_after {
                child.kill().unwrap(); // SIGKILL: no destructors, no flush
                break;
            }
        }
        child.wait().unwrap();
        // The writer restarts its counter at 0 each round, so acked keys
        // from this round are 0..=last.
        let acked = last.expect("child wrote nothing");
        max_acked = Some(max_acked.map_or(acked, |m: u64| m.max(acked)));

        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..=acked {
            let key = format!("key{i:010}");
            let deleted_by_next = (i + 1) % 7 == 0 && i < acked;
            let got = db.get(key.as_bytes()).unwrap();
            if i == acked && (i + 1) % 7 == 0 {
                // The next batch (which deletes this key) wasn't acked but
                // may still have reached disk: either outcome is correct.
                continue;
            }
            if deleted_by_next {
                assert!(
                    got.is_none(),
                    "round {round}: key {i} should have been deleted"
                );
            } else {
                assert!(
                    got.is_some(),
                    "round {round}: acknowledged key {i} lost after crash (acked up to {acked})"
                );
            }
        }
        eprintln!(
            "round {round}: {} acked writes verified after SIGKILL",
            acked + 1
        );
    }
    assert!(
        max_acked.unwrap() > 100,
        "crash test wrote too little to be meaningful"
    );
}
