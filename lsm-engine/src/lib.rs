//! An LSM-tree key/value storage engine.
//!
//! ```no_run
//! use lsm_engine::{Db, Options, ReadOptions, WriteBatch};
//! let db = Db::open("/tmp/mydb", Options::default()).unwrap();
//! db.put(b"hello", b"world").unwrap();
//! assert_eq!(db.get(b"hello").unwrap().as_deref(), Some(&b"world"[..]));
//!
//! let mut batch = WriteBatch::new();
//! batch.put(b"a", b"1").delete(b"hello");
//! db.write(batch).unwrap(); // atomic
//!
//! let snap = db.snapshot();
//! db.put(b"a", b"2").unwrap();
//! assert_eq!(db.get_opt(b"a", ReadOptions { snapshot: Some(&snap) }).unwrap().as_deref(), Some(&b"1"[..]));
//!
//! for (k, v) in db.scan(Some(b"a"), Some(b"z"), ReadOptions::default()) {
//!     println!("{:?} = {:?}", k, v);
//! }
//! ```

mod batch;
mod block;
mod bloom;
mod cache;
mod coding;
mod compaction;
mod db;
mod error;
mod iterator;
mod key;
mod log;
mod memtable;
mod options;
mod sstable;
mod version;

pub use batch::WriteBatch;
pub use db::{Db, ReadOptions, Snapshot, Stats};
pub use error::{Error, Result};
pub use iterator::DbIterator;
pub use options::Options;
pub use sstable::TableOptions;
