//! Append-only record log used for both the WAL and the MANIFEST.
//!
//! Record: `crc32(payload): u32 | len: u32 | payload`. A torn or corrupt
//! record at the tail (crash mid-write) ends recovery; everything before it
//! is returned. Writes were never acknowledged past an unsynced/torn record,
//! so dropping it is safe.

use crate::coding::*;
use crate::fs::{FileSystem, WritableFile};
use std::io;
use std::path::Path;

pub struct LogWriter {
    file: Box<dyn WritableFile>,
    buf: Vec<u8>,
}

impl LogWriter {
    pub fn create(fs: &dyn FileSystem, path: &Path) -> io::Result<Self> {
        Ok(LogWriter {
            file: fs.create(path)?,
            buf: Vec::with_capacity(64 << 10),
        })
    }

    pub fn add_record(&mut self, payload: &[u8]) -> io::Result<()> {
        put_u32(&mut self.buf, crc32fast::hash(payload));
        put_u32(&mut self.buf, payload.len() as u32);
        self.buf.extend_from_slice(payload);
        Ok(())
    }

    /// Hands buffered bytes to the OS, and fsyncs if `sync`.
    pub fn flush(&mut self, sync: bool) -> io::Result<()> {
        if !self.buf.is_empty() {
            let r = self.file.append(&self.buf);
            self.buf.clear();
            r?;
        }
        if sync {
            self.file.sync()?;
        }
        Ok(())
    }
}

/// Reads all intact records from a log file.
pub fn read_log(fs: &dyn FileSystem, path: &Path) -> io::Result<(Vec<Vec<u8>>, bool)> {
    let data = fs.read(path)?;
    let mut out = Vec::new();
    let mut p = 0;
    let mut clean = true;
    while p < data.len() {
        if p + 8 > data.len() {
            clean = false;
            break;
        }
        let crc = get_u32(&data[p..]);
        let len = get_u32(&data[p + 4..]) as usize;
        if p + 8 + len > data.len() {
            clean = false;
            break;
        }
        let payload = &data[p + 8..p + 8 + len];
        if crc32fast::hash(payload) != crc {
            clean = false;
            break;
        }
        out.push(payload.to_vec());
        p += 8 + len;
    }
    Ok((out, clean))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::StdFs;

    #[test]
    fn roundtrip_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.log");
        let mut w = LogWriter::create(&StdFs, &p).unwrap();
        w.add_record(b"hello").unwrap();
        w.add_record(b"world").unwrap();
        w.flush(true).unwrap();
        let (recs, clean) = read_log(&StdFs, &p).unwrap();
        assert!(clean);
        assert_eq!(recs, vec![b"hello".to_vec(), b"world".to_vec()]);

        let len = std::fs::metadata(&p).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len - 2).unwrap();
        let (recs, clean) = read_log(&StdFs, &p).unwrap();
        assert!(!clean);
        assert_eq!(recs, vec![b"hello".to_vec()]);
    }
}
