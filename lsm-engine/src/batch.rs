//! WriteBatch: a group of updates applied atomically.
//!
//! Encoding (also the WAL record payload):
//! `seq: u64 | count: u32 | (kind: u8, key: lp, [value: lp])*`

use crate::coding::*;
use crate::error::{Result, corruption};
use crate::key::{Kind, SeqNum};

#[derive(Clone, Default)]
pub struct WriteBatch {
    rep: Vec<u8>,
    count: u32,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            rep: vec![0; 12],
            count: 0,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        self.ensure_header();
        self.rep.push(Kind::Put as u8);
        put_length_prefixed(&mut self.rep, key);
        put_length_prefixed(&mut self.rep, value);
        self.count += 1;
        self
    }

    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.ensure_header();
        self.rep.push(Kind::Delete as u8);
        put_length_prefixed(&mut self.rep, key);
        self.count += 1;
        self
    }

    fn ensure_header(&mut self) {
        if self.rep.len() < 12 {
            self.rep.resize(12, 0);
        }
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    pub(crate) fn set_seq(&mut self, seq: SeqNum) {
        self.ensure_header();
        self.rep[..8].copy_from_slice(&seq.to_le_bytes());
        self.rep[8..12].copy_from_slice(&self.count.to_le_bytes());
    }

    pub(crate) fn encoded(&self) -> &[u8] {
        &self.rep
    }

    pub(crate) fn append(&mut self, other: &WriteBatch) {
        self.ensure_header();
        if other.rep.len() > 12 {
            self.rep.extend_from_slice(&other.rep[12..]);
        }
        self.count += other.count;
    }

    /// Decodes an encoded batch and calls f(seq, kind, key, value) per entry.
    pub(crate) fn iterate(
        data: &[u8],
        mut f: impl FnMut(SeqNum, Kind, &[u8], &[u8]),
    ) -> Result<(SeqNum, u32)> {
        if data.len() < 12 {
            return corruption("batch too short");
        }
        let base = get_u64(data);
        let count = get_u32(&data[8..]);
        let mut p = 12;
        for i in 0..count {
            if p >= data.len() {
                return corruption("batch truncated");
            }
            let kind = Kind::from_u8(data[p])
                .ok_or_else(|| crate::Error::Corruption("bad kind".into()))?;
            p += 1;
            let (key, n) = get_length_prefixed(&data[p..])?;
            p += n;
            let value: &[u8] = if kind == Kind::Put {
                let (v, n) = get_length_prefixed(&data[p..])?;
                p += n;
                v
            } else {
                &[]
            };
            f(base + i as u64, kind, key, value);
        }
        Ok((base, count))
    }
}
