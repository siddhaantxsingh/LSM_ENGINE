//! Append-only record log used for both the WAL and the MANIFEST.
//!
//! File: `magic (8 bytes) | record*`
//! Record: `len: u32 | crc32(len): u32 | crc32(payload): u32 | payload | 0xA5`
//!
//! The trailer byte is never zero, so a record whose end was zero-filled by
//! a crash can be told apart from a bit flip in a record that happens to
//! end with zero bytes.
//!
//! The length has its own checksum, so a bit flip in a length field is
//! detected instead of looking like a record that runs past the end of the
//! file (which would be mistaken for a torn tail).
//!
//! The reader classifies how the log ends:
//! * `Clean`: every byte belongs to a valid record.
//! * `TornTail`: the log ends with an incomplete record, with zeros, or
//!   with a full-length last record whose payload checksum fails. A crash
//!   during an append can leave exactly these shapes.
//! * `Corrupt`: a bad record is followed by more data. A crash cannot do
//!   that; it is media corruption or a bug, and data after it would be lost.
//!
//! The caller decides what each outcome means (see `WalRecoveryMode`).

use crate::coding::*;
use crate::fs::{FileSystem, WritableFile};
use std::io;
use std::path::Path;

pub const LOG_MAGIC: &[u8; 8] = b"LSMLOG\x00\x02";
const HEADER: usize = 12;
const TRAILER: u8 = 0xA5;
/// Reason for a complete last record whose payload checksum fails. A crash
/// cannot produce this unless pages were written out of order, so callers
/// that fsync every record (the MANIFEST) treat it as corruption.
pub const LAST_RECORD_CHECKSUM: &str = "checksum mismatch in last record";

pub struct LogWriter {
    file: Box<dyn WritableFile>,
    buf: Vec<u8>,
}

/// Encodes one record (header + payload).
pub fn encode_record(out: &mut Vec<u8>, payload: &[u8]) {
    let len = (payload.len() as u32).to_le_bytes();
    out.extend_from_slice(&len);
    put_u32(out, crc32fast::hash(&len));
    put_u32(out, crc32fast::hash(payload));
    out.extend_from_slice(payload);
    out.push(TRAILER);
}

impl LogWriter {
    pub fn create(fs: &dyn FileSystem, path: &Path) -> io::Result<Self> {
        let mut buf = Vec::with_capacity(64 << 10);
        buf.extend_from_slice(LOG_MAGIC);
        Ok(LogWriter {
            file: fs.create(path)?,
            buf,
        })
    }

    pub fn add_record(&mut self, payload: &[u8]) -> io::Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log record larger than 4 GiB",
            ));
        }
        encode_record(&mut self.buf, payload);
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEnd {
    Clean,
    TornTail { offset: usize, reason: &'static str },
    Corrupt { offset: usize, reason: &'static str },
}

pub struct LogContents {
    pub records: Vec<Vec<u8>>,
    pub end: LogEnd,
}

/// Parses a whole log file. Never fails on content; I/O errors only.
pub fn read_log(fs: &dyn FileSystem, path: &Path) -> io::Result<LogContents> {
    Ok(parse_log(&fs.read(path)?))
}

pub fn parse_log(data: &[u8]) -> LogContents {
    let mut records = Vec::new();
    let torn = |offset, reason| LogEnd::TornTail { offset, reason };
    let corrupt = |offset, reason| LogEnd::Corrupt { offset, reason };
    let all_zero = |b: &[u8]| b.iter().all(|&x| x == 0);

    // Where the trailing run of zero bytes starts. A crash can leave the
    // file longer than the data that reached the disk, padded with zeros.
    let zeros_from = data.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);

    if data.is_empty() {
        return LogContents {
            records,
            end: LogEnd::Clean,
        };
    }
    if data.len() < LOG_MAGIC.len() || data[..LOG_MAGIC.len()] != LOG_MAGIC[..] {
        // A partial magic followed only by zeros (or nothing) is a file
        // whose creation never completed. Anything else is not our log.
        let end = if zeros_from <= LOG_MAGIC.len() && LOG_MAGIC.starts_with(&data[..zeros_from]) {
            torn(0, "truncated file header")
        } else {
            corrupt(0, "bad log magic (wrong file type or old format)")
        };
        return LogContents { records, end };
    }
    let mut p = LOG_MAGIC.len();
    let end = loop {
        if p == data.len() {
            break LogEnd::Clean;
        }
        let rest = &data[p..];
        if all_zero(rest) {
            break torn(p, "zero-filled tail");
        }
        if rest.len() < HEADER {
            break torn(p, "truncated record header");
        }
        let len_bytes = &rest[..4];
        if crc32fast::hash(len_bytes) != get_u32(&rest[4..]) {
            if zeros_from < p + HEADER {
                break torn(p, "partly written record header");
            }
            break corrupt(p, "record length checksum mismatch");
        }
        let len = get_u32(len_bytes) as usize;
        if rest.len() - HEADER < len + 1 {
            break torn(p, "truncated record payload");
        }
        let payload = &rest[HEADER..HEADER + len];
        let trailer = rest[HEADER + len];
        if crc32fast::hash(payload) != get_u32(&rest[8..]) || trailer != TRAILER {
            let is_last = rest.len() == HEADER + len + 1;
            // Zero-filled end of the record (and of the file): an append
            // that was cut short by a crash.
            if trailer == 0 && zeros_from < p + HEADER + len + 1 {
                break torn(p, "zero-filled record tail");
            }
            if is_last {
                break torn(p, LAST_RECORD_CHECKSUM);
            }
            break corrupt(p, "record checksum mismatch");
        }
        records.push(payload.to_vec());
        p += HEADER + len + 1;
    };
    LogContents { records, end }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::StdFs;

    fn log_of(records: &[&[u8]]) -> Vec<u8> {
        let mut d = LOG_MAGIC.to_vec();
        for r in records {
            encode_record(&mut d, r);
        }
        d
    }

    #[test]
    fn roundtrip_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("x.log");
        let mut w = LogWriter::create(&StdFs, &p).unwrap();
        w.add_record(b"hello").unwrap();
        w.add_record(b"world").unwrap();
        w.flush(true).unwrap();
        let c = read_log(&StdFs, &p).unwrap();
        assert_eq!(c.end, LogEnd::Clean);
        assert_eq!(c.records, vec![b"hello".to_vec(), b"world".to_vec()]);

        let len = std::fs::metadata(&p).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
        f.set_len(len - 2).unwrap();
        let c = read_log(&StdFs, &p).unwrap();
        assert!(matches!(c.end, LogEnd::TornTail { .. }));
        assert_eq!(c.records, vec![b"hello".to_vec()]);
    }

    #[test]
    fn every_truncation_is_a_torn_tail_with_a_prefix_of_records() {
        let full = log_of(&[b"a", b"bbbbbbbb", b"", b"cc"]);
        for cut in 0..full.len() {
            let c = parse_log(&full[..cut]);
            assert!(
                matches!(c.end, LogEnd::TornTail { .. } | LogEnd::Clean),
                "cut {cut}: {:?}",
                c.end
            );
            let full_recs = parse_log(&full).records;
            assert_eq!(c.records[..], full_recs[..c.records.len()]);
            // Zero-filling the cut-off part is also a torn tail.
            let mut z = full[..cut].to_vec();
            z.resize(full.len(), 0);
            let c = parse_log(&z);
            assert!(!matches!(c.end, LogEnd::Corrupt { .. }), "zero cut {cut}");
        }
    }

    #[test]
    fn every_bit_flip_before_the_last_record_is_corruption() {
        let full = log_of(&[b"first record", b"second", b"third and last"]);
        let last_start = full.len() - (HEADER + b"third and last".len() + 1);
        for byte in 0..last_start {
            for bit in 0..8 {
                let mut d = full.clone();
                d[byte] ^= 1 << bit;
                let c = parse_log(&d);
                assert!(
                    matches!(c.end, LogEnd::Corrupt { .. }),
                    "flip byte {byte} bit {bit}: {:?}",
                    c.end
                );
            }
        }
    }
}
