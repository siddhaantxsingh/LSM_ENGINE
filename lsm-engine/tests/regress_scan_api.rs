//! D6 regression (separate file because the fix changed the iterator's
//! item type, so this test does not compile against the old API).

use lsm_engine::{Db, Options, ReadOptions};
use std::path::{Path, PathBuf};

fn k(i: usize) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == ext))
        .collect();
    v.sort();
    v
}

fn flip(path: &Path, offset: usize) {
    let mut d = std::fs::read(path).unwrap();
    d[offset] ^= 0x5a;
    std::fs::write(path, d).unwrap();
}

/// D6 (high): DbIterator implemented Iterator<Item = (key, value)>, so a
/// read error part-way through a scan looked like the end of the data.
#[test]
fn d6_scan_iterator_yields_errors() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..5000 {
            db.put(&k(i), &[3u8; 64]).unwrap();
        }
        db.flush().unwrap();
    }
    let sst = files_with_ext(dir.path(), "sst").pop().unwrap();
    let n = std::fs::metadata(&sst).unwrap().len() as usize;
    flip(&sst, n / 2); // some data block in the middle
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let items: Vec<lsm_engine::Result<(Vec<u8>, Vec<u8>)>> =
        db.scan(None, None, ReadOptions::default()).collect();
    let ok = items.iter().filter(|r| r.is_ok()).count();
    assert!(ok < 5000);
    assert!(
        items.last().is_some_and(|r| r.is_err()),
        "scan ended after {ok} keys without reporting an error"
    );
}
