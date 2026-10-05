//! Sorted String Table: an immutable, sorted file of internal keys.
//!
//! ```text
//! [data block][crc32] ... [data block][crc32]
//! [filter block][crc32]          bloom filter over user keys
//! [index block][crc32]           key: last internal key of each data block
//!                                value: offset varint | size varint
//! footer (40 bytes): filter_off u64 | filter_size u64 | index_off u64 |
//!                    index_size u64 | magic u64
//! ```
//!
//! Every block carries a CRC so silent disk corruption is detected on read.

use crate::block::{Block, BlockBuilder, BlockIterator};
use crate::bloom::{self, BloomBuilder};
use crate::cache::BlockCache;
use crate::coding::*;
use crate::error::{Result, corruption};
use crate::iterator::InternalIterator;
use crate::key::{cmp_internal, user_key};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

const MAGIC: u64 = 0x4c534d5f53535442; // "LSM_SSTB"
const FOOTER_LEN: usize = 40;

#[derive(Clone, Copy, Debug)]
pub struct TableOptions {
    pub block_size: usize,
    pub restart_interval: usize,
    pub bloom_bits_per_key: usize,
}

impl Default for TableOptions {
    fn default() -> Self {
        TableOptions {
            block_size: 4096,
            restart_interval: 16,
            bloom_bits_per_key: 10,
        }
    }
}

pub struct TableBuilder {
    w: BufWriter<File>,
    opts: TableOptions,
    offset: u64,
    data: BlockBuilder,
    index: BlockBuilder,
    bloom: BloomBuilder,
    last_user_key: Vec<u8>,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub num_entries: u64,
}

impl TableBuilder {
    pub fn create(path: &Path, opts: TableOptions) -> Result<Self> {
        let f = File::create(path)?;
        Ok(TableBuilder {
            w: BufWriter::with_capacity(256 << 10, f),
            opts,
            offset: 0,
            data: BlockBuilder::new(opts.restart_interval),
            index: BlockBuilder::new(1),
            bloom: BloomBuilder::new(opts.bloom_bits_per_key),
            last_user_key: vec![],
            smallest: vec![],
            largest: vec![],
            num_entries: 0,
        })
    }

    /// Keys must be added in strictly increasing internal-key order.
    pub fn add(&mut self, ikey: &[u8], value: &[u8]) -> Result<()> {
        if self.num_entries == 0 {
            self.smallest = ikey.to_vec();
        }
        let uk = user_key(ikey);
        if self.num_entries == 0 || uk != self.last_user_key.as_slice() {
            self.bloom.add(uk);
            self.last_user_key = uk.to_vec();
        }
        self.data.add(ikey, value);
        self.largest.clear();
        self.largest.extend_from_slice(ikey);
        self.num_entries += 1;
        if self.data.estimated_size() >= self.opts.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    fn write_block(&mut self, block: &[u8]) -> Result<(u64, u64)> {
        let off = self.offset;
        self.w.write_all(block)?;
        let mut crc = Vec::with_capacity(4);
        put_u32(&mut crc, crc32fast::hash(block));
        self.w.write_all(&crc)?;
        self.offset += block.len() as u64 + 4;
        Ok((off, block.len() as u64))
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let last = self.data.last_key().to_vec();
        let block = self.data.finish();
        let (off, size) = self.write_block(&block)?;
        let mut handle = Vec::new();
        put_varint(&mut handle, off);
        put_varint(&mut handle, size);
        self.index.add(&last, &handle);
        Ok(())
    }

    pub fn estimated_file_size(&self) -> u64 {
        self.offset + self.data.estimated_size() as u64
    }

    /// Writes filter, index and footer, then fsyncs. Returns the file size.
    pub fn finish(mut self) -> Result<u64> {
        self.flush_data_block()?;
        let filter = self.bloom.finish();
        let (foff, fsize) = self.write_block(&filter)?;
        let index = self.index.finish();
        let (ioff, isize) = self.write_block(&index)?;
        let mut footer = Vec::with_capacity(FOOTER_LEN);
        for v in [foff, fsize, ioff, isize, MAGIC] {
            put_u64(&mut footer, v);
        }
        self.w.write_all(&footer)?;
        self.offset += FOOTER_LEN as u64;
        self.w.flush()?;
        self.w.get_ref().sync_all()?;
        Ok(self.offset)
    }
}

pub struct Table {
    file: File,
    file_number: u64,
    index: Block,
    filter: Vec<u8>,
    cache: Option<Arc<BlockCache>>,
}

fn read_block(file: &File, off: u64, size: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; size as usize + 4];
    file.read_exact_at(&mut buf, off)?;
    let crc = get_u32(&buf[size as usize..]);
    buf.truncate(size as usize);
    if crc32fast::hash(&buf) != crc {
        return corruption(format!("block checksum mismatch at offset {off}"));
    }
    Ok(buf)
}

impl Table {
    pub fn open(path: &Path, file_number: u64, cache: Option<Arc<BlockCache>>) -> Result<Table> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_LEN as u64 {
            return corruption("table too short");
        }
        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut footer, len - FOOTER_LEN as u64)?;
        if get_u64(&footer[32..]) != MAGIC {
            return corruption("bad table magic");
        }
        let (foff, fsize) = (get_u64(&footer[0..]), get_u64(&footer[8..]));
        let (ioff, isize) = (get_u64(&footer[16..]), get_u64(&footer[24..]));
        let filter = read_block(&file, foff, fsize)?;
        let index = Block::new(Arc::new(read_block(&file, ioff, isize)?))?;
        Ok(Table {
            file,
            file_number,
            index,
            filter,
            cache,
        })
    }

    fn data_block(&self, handle: &[u8]) -> Result<Block> {
        let (off, n) = get_varint(handle)?;
        let (size, _) = get_varint(&handle[n..])?;
        let key = (self.file_number, off);
        if let Some(c) = &self.cache
            && let Some(b) = c.get(&key)
        {
            return Block::new(b);
        }
        let data = Arc::new(read_block(&self.file, off, size)?);
        if let Some(c) = &self.cache {
            c.insert(key, data.clone());
        }
        Block::new(data)
    }

    /// Returns the first entry with internal key >= `ikey`, if its user key
    /// matches. The bloom filter short-circuits absent keys.
    pub fn get(&self, ikey: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let uk = user_key(ikey);
        if !bloom::may_contain(&self.filter, uk) {
            return Ok(None);
        }
        let mut idx = self.index.iter(cmp_internal);
        idx.seek(ikey);
        if !idx.valid() {
            return Ok(None);
        }
        let block = self.data_block(idx.value())?;
        let mut it = block.iter(cmp_internal);
        it.seek(ikey);
        if it.valid() && user_key(it.key()) == uk {
            return Ok(Some((it.key().to_vec(), it.value().to_vec())));
        }
        Ok(None)
    }

    pub fn iter(self: &Arc<Self>) -> TableIterator {
        TableIterator {
            table: self.clone(),
            index: self.index.iter(cmp_internal),
            data: None,
            err: None,
        }
    }
}

/// Two-level iterator: walks the index block and lazily loads data blocks.
pub struct TableIterator {
    table: Arc<Table>,
    index: BlockIterator,
    data: Option<BlockIterator>,
    err: Option<String>,
}

impl TableIterator {
    fn load_block(&mut self) {
        self.data = None;
        if self.index.valid() {
            match self.table.data_block(self.index.value()) {
                Ok(b) => self.data = Some(b.iter(cmp_internal)),
                Err(e) => self.err = Some(e.to_string()),
            }
        }
    }

    fn skip_empty_forward(&mut self) {
        while self.data.as_ref().is_some_and(|d| !d.valid()) {
            self.index.next();
            self.load_block();
            if let Some(d) = &mut self.data {
                d.seek_to_first();
            }
        }
    }
}

impl InternalIterator for TableIterator {
    fn valid(&self) -> bool {
        self.data.as_ref().is_some_and(|d| d.valid())
    }
    fn seek_to_first(&mut self) {
        self.index.seek_to_first();
        self.load_block();
        if let Some(d) = &mut self.data {
            d.seek_to_first();
        }
        self.skip_empty_forward();
    }
    fn seek(&mut self, target: &[u8]) {
        self.index.seek(target);
        self.load_block();
        if let Some(d) = &mut self.data {
            d.seek(target);
        }
        self.skip_empty_forward();
    }
    fn next(&mut self) {
        if let Some(d) = &mut self.data {
            d.next();
        }
        self.skip_empty_forward();
    }
    fn key(&self) -> &[u8] {
        self.data.as_ref().unwrap().key()
    }
    fn value(&self) -> &[u8] {
        self.data.as_ref().unwrap().value()
    }
    fn status(&self) -> Result<()> {
        match &self.err {
            Some(e) => corruption(e.clone()),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{Kind, make_internal_key};

    #[test]
    fn build_read_iterate() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("1.sst");
        let mut b = TableBuilder::create(
            &p,
            TableOptions {
                block_size: 256,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..5000u32 {
            let k = make_internal_key(format!("k{i:06}").as_bytes(), 1, Kind::Put);
            b.add(&k, format!("value{i}").as_bytes()).unwrap();
        }
        b.finish().unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let t = Arc::new(Table::open(&p, 1, Some(cache.clone())).unwrap());

        let (k, v) = t
            .get(&make_internal_key(b"k002500", 9, Kind::Put))
            .unwrap()
            .unwrap();
        assert_eq!(user_key(&k), b"k002500");
        assert_eq!(v, b"value2500");
        assert!(
            t.get(&make_internal_key(b"nope", 9, Kind::Put))
                .unwrap()
                .is_none()
        );

        let mut it = t.iter();
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        assert_eq!(n, 5000);
        it.seek(&make_internal_key(b"k004999", 9, Kind::Put));
        assert_eq!(user_key(it.key()), b"k004999");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("2.sst");
        let mut b = TableBuilder::create(&p, TableOptions::default()).unwrap();
        for i in 0..100u32 {
            b.add(
                &make_internal_key(format!("k{i:03}").as_bytes(), 1, Kind::Put),
                b"v",
            )
            .unwrap();
        }
        b.finish().unwrap();
        let mut data = std::fs::read(&p).unwrap();
        data[10] ^= 0xff;
        std::fs::write(&p, &data).unwrap();
        let t = Table::open(&p, 2, None).unwrap();
        assert!(matches!(
            t.get(&make_internal_key(b"k000", 9, Kind::Put)),
            Err(crate::Error::Corruption(_))
        ));
    }
}
