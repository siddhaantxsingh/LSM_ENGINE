//! Data/index block format (same idea as LevelDB):
//!
//! ```text
//! entry*  : shared: varint | unshared: varint | value_len: varint | key_delta | value
//! restarts: u32 * num_restarts   (offsets of entries with shared == 0)
//! num_restarts: u32
//! ```
//!
//! Keys are prefix-compressed against the previous key; every
//! `restart_interval` entries the full key is stored so a seek can binary
//! search the restart points and then scan at most `restart_interval`
//! entries.

use crate::coding::*;
use crate::error::{Result, corruption};
use crate::iterator::InternalIterator;
use std::cmp::Ordering;
use std::sync::Arc;

pub type Comparator = fn(&[u8], &[u8]) -> Ordering;

pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        BlockBuilder {
            buf: vec![],
            restarts: vec![0],
            counter: 0,
            restart_interval,
            last_key: vec![],
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared = 0;
        if self.counter < self.restart_interval {
            let max = self.last_key.len().min(key.len());
            while shared < max && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
        }
        put_varint(&mut self.buf, shared as u64);
        put_varint(&mut self.buf, (key.len() - shared) as u64);
        put_varint(&mut self.buf, value.len() as u64);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn estimated_size(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + 4
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for &r in &self.restarts {
            put_u32(&mut out, r);
        }
        put_u32(&mut out, self.restarts.len() as u32);
        self.restarts = vec![0];
        self.counter = 0;
        self.last_key.clear();
        out
    }
}

pub struct Block {
    data: Arc<Vec<u8>>,
    restarts_off: usize,
    num_restarts: usize,
}

impl Block {
    pub fn new(data: Arc<Vec<u8>>) -> Result<Block> {
        if data.len() < 4 {
            return corruption("block too small");
        }
        let n = get_u32(&data[data.len() - 4..]) as usize;
        let need = 4 + n * 4;
        if n == 0 || need > data.len() {
            return corruption("bad restart count");
        }
        Ok(Block {
            restarts_off: data.len() - need,
            num_restarts: n,
            data,
        })
    }

    pub fn iter(&self, cmp: Comparator) -> BlockIterator {
        BlockIterator {
            data: self.data.clone(),
            restarts_off: self.restarts_off,
            num_restarts: self.num_restarts,
            cmp,
            offset: self.restarts_off,
            next_offset: self.restarts_off,
            key: vec![],
            value_range: (0, 0),
            corrupt: false,
        }
    }
}

pub struct BlockIterator {
    data: Arc<Vec<u8>>,
    restarts_off: usize,
    num_restarts: usize,
    cmp: Comparator,
    offset: usize,      // current entry offset (== restarts_off when invalid)
    next_offset: usize, // offset of the following entry
    key: Vec<u8>,
    value_range: (usize, usize),
    /// An entry failed to parse (a block that passed its CRC but is
    /// malformed). Reported through `status`, never treated as the end.
    corrupt: bool,
}

impl BlockIterator {
    fn restart_point(&self, i: usize) -> usize {
        let r = get_u32(&self.data[self.restarts_off + 4 * i..]) as usize;
        r.min(self.restarts_off)
    }

    /// Parses the entry at next_offset; returns false at end of block.
    fn parse_next(&mut self) -> bool {
        self.offset = self.next_offset;
        if self.offset >= self.restarts_off {
            self.offset = self.restarts_off;
            return false;
        }
        let d = &self.data[..self.restarts_off];
        let mut p = self.offset;
        let parse = |p: &mut usize| -> Option<usize> {
            let (v, n) = get_varint(&d[*p..]).ok()?;
            *p += n;
            Some(v as usize)
        };
        let (Some(shared), Some(unshared), Some(vlen)) =
            (parse(&mut p), parse(&mut p), parse(&mut p))
        else {
            self.offset = self.restarts_off;
            self.corrupt = true;
            return false;
        };
        if shared > self.key.len()
            || unshared
                .checked_add(vlen)
                .and_then(|n| n.checked_add(p))
                .is_none_or(|e| e > d.len())
        {
            self.offset = self.restarts_off;
            self.corrupt = true;
            return false;
        }
        self.key.truncate(shared);
        self.key.extend_from_slice(&d[p..p + unshared]);
        p += unshared;
        self.value_range = (p, p + vlen);
        self.next_offset = p + vlen;
        true
    }

    fn seek_to_restart(&mut self, i: usize) {
        self.key.clear();
        self.next_offset = self.restart_point(i);
    }
}

impl InternalIterator for BlockIterator {
    fn valid(&self) -> bool {
        self.offset < self.restarts_off
    }

    fn seek_to_first(&mut self) {
        self.seek_to_restart(0);
        self.parse_next();
    }

    fn seek(&mut self, target: &[u8]) {
        // Binary search for the last restart point whose key < target.
        let (mut lo, mut hi) = (0usize, self.num_restarts - 1);
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            self.seek_to_restart(mid);
            if !self.parse_next() {
                hi = mid - 1;
                continue;
            }
            if (self.cmp)(&self.key, target) == Ordering::Less {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        self.seek_to_restart(lo);
        while self.parse_next() {
            if (self.cmp)(&self.key, target) != Ordering::Less {
                return;
            }
        }
    }

    fn next(&mut self) {
        self.parse_next();
    }

    fn key(&self) -> &[u8] {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &self.data[self.value_range.0..self.value_range.1]
    }

    fn status(&self) -> Result<()> {
        if self.corrupt {
            return corruption("malformed block entry");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytewise(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    #[test]
    fn build_and_seek() {
        let mut b = BlockBuilder::new(4);
        let keys: Vec<String> = (0..100).map(|i| format!("key{i:04}")).collect();
        for k in &keys {
            b.add(k.as_bytes(), format!("v-{k}").as_bytes());
        }
        let block = Block::new(Arc::new(b.finish())).unwrap();
        let mut it = block.iter(bytewise);
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            assert_eq!(it.key(), keys[n].as_bytes());
            assert_eq!(it.value(), format!("v-{}", keys[n]).as_bytes());
            it.next();
            n += 1;
        }
        assert_eq!(n, 100);
        it.seek(b"key0050");
        assert_eq!(it.key(), b"key0050");
        it.seek(b"key00505");
        assert_eq!(it.key(), b"key0051");
        it.seek(b"a");
        assert_eq!(it.key(), b"key0000");
        it.seek(b"z");
        assert!(!it.valid());
    }
}
